package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// last kvserver that Clerk remembers to be the leader
	lastKvServerLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastKvServerLeader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{key, GenerateRandomString(32)}
	reply := GetReply{ErrNoKey, ""}
	// try forever
	for {
		for i := 0; i < len(ck.servers); i++ {
			kvServerIdx := (i + ck.lastKvServerLeader) % len(ck.servers)
			kvserver := ck.servers[kvServerIdx]
			ok := kvserver.Call("KVServer.Get", &args, &reply)
			if !ok {
				// fmt.Println("RPC 'GET' failed")
			} else {
				// if successful
				if reply.Err == OK {
					ck.lastKvServerLeader = kvServerIdx
					return reply.Value
				} else if reply.Err == ErrNoKey {
					return ""
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, GenerateRandomString(32)}
	reply := PutAppendReply{ErrNoKey}
	// try forever
	for {
		for i := 0; i < len(ck.servers); i++ {
			kvServerIdx := (i + ck.lastKvServerLeader) % len(ck.servers)
			kvserver := ck.servers[kvServerIdx]
			ok := kvserver.Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				// fmt.Println("RPC '", op, "' failed")
			} else {
				// if successful
				if reply.Err == OK {
					ck.lastKvServerLeader = kvServerIdx
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
