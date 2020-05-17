package kvraft

import (
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
	Key        string
	Value      string
	Type       string
	Order      int
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
	// the key represents the order that each command is received by the server
	applyCondOrderMap map[int]*sync.Cond

	// atomically increasing counter stores the order when each command is received
	counter int
	// commitedCounter int
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
	op := Op{args.Key, "", "Get", kv.counter, ""}

	_, prevTerm, _ := kv.rf.Start(op)

	// create the sync.cond variable
	m := sync.Mutex{}
	condVar := sync.NewCond(&m)

	kv.applyCondOrderMap[kv.counter] = condVar

	kv.counter++
	kv.mu.Unlock()

	// wait for condVar
	condVar.L.Lock()
	condVar.Wait()

	// if !kv.checkLeader() {
	// 	reply.Err = ErrWrongLeader
	// 	condVar.L.Unlock()
	// 	return
	// }
	curTerm, isLeader := kv.rf.GetState()

	kv.mu.Lock()

	// apply get

	if !isLeader || curTerm != prevTerm {
		reply.Err = ErrWrongLeader
	} else if val, ok := kv.keyValueMap[args.Key]; !ok {
		reply.Err = ErrNoKey
	} else {
		reply.Value = val
		reply.Err = OK
	}

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

	for _, curCommitedLog := range kv.rf.GetCommitedLogs() {
		// already commited in the log, then we don't have to perform again
		if curCommitedLog.Command.(Op).Identifier == args.Identifier {
			kv.mu.Unlock()
			reply.Err = OK
			return
		}
	}

	// check if the put/append request is already commited in leader's log

	// args.Op is either "Put" or "Append"
	op := Op{args.Key, args.Value, args.Op, kv.counter, args.Identifier}

	_, prevTerm, _ := kv.rf.Start(op)

	// create the sync.cond variable
	m := sync.Mutex{}
	condVar := sync.NewCond(&m)

	kv.applyCondOrderMap[kv.counter] = condVar

	kv.counter++
	kv.mu.Unlock()

	// wait for condVar
	condVar.L.Lock()
	condVar.Wait()

	// if !kv.checkLeader() {
	// 	reply.Err = ErrWrongLeader
	// 	condVar.L.Unlock()
	// 	return
	// }

	// apply put/Append
	curTerm, isLeader := kv.rf.GetState()

	kv.mu.Lock()

	if !isLeader || curTerm != prevTerm {
		reply.Err = ErrWrongLeader
	} else if args.Op == "Put" {
		kv.keyValueMap[args.Key] = args.Value
		reply.Err = OK
	} else {
		// exist, append
		if _, ok := kv.keyValueMap[args.Key]; ok {
			kv.keyValueMap[args.Key] = kv.keyValueMap[args.Key] + args.Value // append
		} else {
			// doesn't exist, act like put
			kv.keyValueMap[args.Key] = args.Value
		}
		reply.Err = OK
	}

	fmt.Println("Server", kv.me, kv.keyValueMap)

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
			applyMsgOrder := applyMsg.Command.(Op).Order
			kv.mu.Lock()
			// if the index is found
			if cond, ok := kv.applyCondOrderMap[applyMsgOrder]; ok {
				cond.Signal()
				delete(kv.applyCondOrderMap, applyMsgOrder)
			}
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		default:

			// checkLeadership, if not leader anymore, any unsent RPC to client will
			// be issued with leadershipFailure

			// signal all
			if !kv.checkLeader() {
				kv.mu.Lock()
				for order, cond := range kv.applyCondOrderMap {
					cond.Signal()
					delete(kv.applyCondOrderMap, order)
				}
				kv.mu.Unlock()
			}

			time.Sleep(time.Millisecond * 10)
		}
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

	//
	kv.counter = 0
	// kv.commitedCounter = -1

	//
	kv.applyCondOrderMap = make(map[int]*sync.Cond)

	// a dedicated goroutine runs checkApply
	go func() { kv.checkApply() }()

	return kv
}
