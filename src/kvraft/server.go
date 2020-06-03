package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string
	// Order      int
	Identifier string
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// the actual map structure
	keyValueMap map[string]string

	// the applyCond map that the dedicated applyCh check will signal.
	// the key represents the identifier that each command is received by the server
	applyIdentifierMap map[string]*sync.Cond

	// Timeout value that has identifier -> timeOutValue
	timeoutMap map[string]time.Time

	// seen identifiers map
	identifiersMap map[string]bool
}

// check if the corresponding raft instance is leader
func (kv *KVServer) checkLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// RPC handler for Get
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// form the command
	kv.mu.Lock()
	op := Op{args.Key, "", "Get", args.Identifier}

	_, prevTerm, _ := kv.rf.Start(op)

	// create the sync.cond variable
	m := sync.Mutex{}
	condVar := sync.NewCond(&m)

	kv.applyIdentifierMap[args.Identifier] = condVar
	kv.timeoutMap[args.Identifier] = time.Now()

	kv.mu.Unlock()

	// wait for condVar
	condVar.L.Lock()
	condVar.Wait()

	curTerm, isLeader := kv.rf.GetState()

	kv.mu.Lock()

	// apply get

	if !isLeader || curTerm != prevTerm || time.Now().Sub(kv.timeoutMap[args.Identifier]) > time.Millisecond*1000 {
		reply.Err = ErrWrongLeader
		delete(kv.applyIdentifierMap, args.Identifier)
		delete(kv.timeoutMap, args.Identifier)
	} else if val, ok := kv.keyValueMap[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Value = val
		reply.Err = OK
	}

	// fmt.Println("Get Server", kv.me)

	condVar.L.Unlock()
	kv.mu.Unlock()
}

// RPC handler for PutAppend
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if !kv.checkLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// form the command
	kv.mu.Lock()

	// check if the put/append request is already commited in leader's log

	// args.Op is either "Put" or "Append"
	op := Op{args.Key, args.Value, args.Op, args.Identifier}

	_, prevTerm, _ := kv.rf.Start(op)

	// create the sync.cond variable
	m := sync.Mutex{}
	condVar := sync.NewCond(&m)

	kv.applyIdentifierMap[args.Identifier] = condVar
	kv.timeoutMap[args.Identifier] = time.Now()

	kv.mu.Unlock()

	// wait for condVar
	condVar.L.Lock()
	condVar.Wait()

	curTerm, isLeader := kv.rf.GetState()

	kv.mu.Lock()

	// only deals with sending RPC
	// fmt.Println(";;;;", kv.me, isLeader, curTerm, prevTerm, time.Now().Sub(kv.timeoutMap[args.Identifier]))
	if !isLeader || curTerm != prevTerm || time.Now().Sub(kv.timeoutMap[args.Identifier]) > time.Millisecond*1500 {
		reply.Err = ErrWrongLeader
		delete(kv.applyIdentifierMap, args.Identifier)
		delete(kv.timeoutMap, args.Identifier)
	} else {
		reply.Err = OK
	}

	condVar.L.Unlock()
	kv.mu.Unlock()
}

// checkApply should be run by a dedicated goroutine to
// periodically check if any ApplyMsg is received from ApplyCh
func (kv *KVServer) checkApply() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			// type assertion here
			kv.mu.Lock()
			// if it's a snapshot message:
			if !applyMsg.CommandValid && applyMsg.CommandType == "snapshot" {
				kv.keyValueMap = applyMsg.Command.(raft.SnapshotComplex).KeyValueMap
				kv.identifiersMap = applyMsg.Command.(raft.SnapshotComplex).IdentifiersMap
				kv.mu.Unlock()
				continue
			}

			// if the index is found
			theOpStruct := applyMsg.Command.(Op)
			if _, ok := kv.identifiersMap[theOpStruct.Identifier]; !ok {
				if theOpStruct.Type == "Put" {
					kv.keyValueMap[theOpStruct.Key] = theOpStruct.Value
				} else if applyMsg.Command.(Op).Type == "Append" {
					// exist, append
					if _, ok := kv.keyValueMap[theOpStruct.Key]; ok {
						kv.keyValueMap[theOpStruct.Key] = kv.keyValueMap[theOpStruct.Key] + theOpStruct.Value // append
					} else {
						// doesn't exist, act like put
						kv.keyValueMap[theOpStruct.Key] = theOpStruct.Value
					}
				}
			}

			kv.identifiersMap[theOpStruct.Identifier] = true
			if cond, ok := kv.applyIdentifierMap[theOpStruct.Identifier]; ok {
				cond.Signal()
			}

			kv.mu.Unlock()
			kv.trySnapShot()
		default:

			// checktime outs below:
			kv.mu.Lock()
			for id, startTime := range kv.timeoutMap {
				if time.Now().Sub(startTime) > time.Millisecond*1500 {
					cond := kv.applyIdentifierMap[id]
					cond.Signal()
				}
			}

			kv.mu.Unlock()
			// kv.trySnapShot()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// try snapshot, if can't just return, else snapshot
// this should be run by the checkApply goroutine
func (kv *KVServer) trySnapShot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// when maxraftstate is -1, snapshot is not required
	if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
		// communicate with raft to get the metadata
		fmt.Println("snapshot")
		lastIncludedIndex, lastIncludedTerm := kv.rf.GetLastAppliedMeta()

		snapshotStruct := raft.SnapshotComplex{kv.identifiersMap, kv.keyValueMap}

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(lastIncludedIndex)
		e.Encode(lastIncludedTerm)
		e.Encode(snapshotStruct)
		snapshotData := w.Bytes()
		kv.rf.PersistSnapshot(snapshotData, lastIncludedIndex)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.keyValueMap = make(map[string]string)
	kv.timeoutMap = make(map[string]time.Time)

	kv.applyIdentifierMap = make(map[string]*sync.Cond)

	kv.identifiersMap = make(map[string]bool)

	// a dedicated goroutine runs checkApply
	go func() { kv.checkApply() }()

	return kv
}
