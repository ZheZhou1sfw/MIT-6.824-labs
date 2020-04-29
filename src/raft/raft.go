package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// a log structre that contains:
//    1. command for state macchine
//    2. term that this log is received by the leader
//
type LogStruct struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent states on all servers
	currentTerm int
	votedFor    int // -1 for nil
	log         []*LogStruct

	// Volatile states on all servers
	commitIndex         int
	lastApplied         int
	lastHeardFromLeader time.Time
	timeLimit           time.Duration // TimeLimit for leader election

	// Volatile states on leaders only!
	isLeader   bool
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// 2B

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	// if rf.currentTerm == args.Term {
	// 	fmt.Printf("They have the same term %v ??\n", rf.currentTerm)
	// }
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else {
		// update term
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			if rf.isLeader {
				rf.isLeader = false
				go rf.checkTimeouts()
			}

		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			lastLogTerm := -1
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
			lastLogIndex := len(rf.log)
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				// fmt.Println("granted??", args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				// reset timer
				rf.lastHeardFromLeader = time.Now()
				rf.timeLimit = getRandTimeoutDuration(300)
			}
		}
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here (2A).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntires   []*LogStruct
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A deal with heartbeats'
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		// make sure it's not a leader
		if rf.isLeader {
			rf.isLeader = false
			go func() { rf.checkTimeouts() }()
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// this will abort any undergoing election because
		// the new term is promised to be higher than the term during election
	}
	// empty log entry means heartbeat, gonna reset time out value
	rf.lastHeardFromLeader = time.Now()
	rf.timeLimit = getRandTimeoutDuration(300)

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	// 2B deal with log entries
	if len(args.LogEntires) > 0 {

	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// RPC call to AppendEntries
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// 'checkTimeouts' regularly checks if the election timeout is due
//	and starts an new election.
//
//  This function is always minitored by a dedicated goroutine.
//  A leader server will suppress(end) this functionality / replaced with heartbeat
//
func (rf *Raft) checkTimeouts() {
	for {
		rf.mu.Lock()
		// fmt.Println(rf.me, " is checking timeout")
		// durationPassed := time.Now().Sub(rf.lastHeardFromLeader).Nanoseconds() / 1e6
		durationPassed := time.Now().Sub(rf.lastHeardFromLeader)
		// leader doesn't need checkTimeouts
		if rf.isLeader {
			rf.mu.Unlock()
			return
		} else if durationPassed > rf.timeLimit { // overdue, should start an election
			// have a separate goroutine deal with election because checkTimeouts need to keep monitoring
			go func() { rf.startElection() }()

			// test
			// rf.mu.Unlock()
			// time.Sleep(getRandTimeoutDuration(1000))
			// rf.mu.Lock()

			// reset election timer
			rf.lastHeardFromLeader = time.Now()
			rf.timeLimit = getRandTimeoutDuration(300)
			rf.mu.Unlock()
		} else {
			// sleep for a while
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

//
// 'heartBeats' regularly sends heart beats to followers.
//
// This should be run on a dedicated goroutine and only a leader will run this.
//
func (rf *Raft) heartBeats() {
	// only the leader is required to do this
	rf.mu.Lock()
	stillLeader := rf.isLeader
	rf.mu.Unlock()
	for stillLeader {
		// send heartbeat to followers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			targetID := i // save index
			go func() {
				// create args structure
				var prevLogTerm int
				rf.mu.Lock()
				if len(rf.log) > 0 {
					prevLogTerm = rf.log[len(rf.log)-1].Term
				} else {
					prevLogTerm = -1
				}
				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					len(rf.log), // prevLogIndex
					prevLogTerm,
					nil,
					rf.commitIndex,
				}

				rf.mu.Unlock()

				reply := AppendEntriesReply{
					-1,
					false,
				}
				// send RPC call
				rpcSuccess := rf.sendAppendEntries(targetID, &args, &reply)

				rf.mu.Lock()
				if !rpcSuccess {
					//fmt.Printf("sendAppendEntries RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
					rf.mu.Unlock()
				} else {

					// if the follower has a higher term than the leader, the leader should convert to follower
					if reply.Term > rf.currentTerm {
						rf.isLeader = false
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						go func() { rf.checkTimeouts() }()
					} else {
						rf.mu.Unlock()
					}
				}
			}()
		}
		// sleep for a while. Limit 10 heartbeats per second
		time.Sleep(time.Millisecond * 120)
		rf.mu.Lock()
		stillLeader = rf.isLeader
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	// always vote for itself
	majorityCount := 1
	wg := sync.WaitGroup{}
	electionLock := sync.Mutex{}

	threshold := len(rf.peers)/2 + 1

	rf.mu.Lock()
	// Increment current Term
	rf.currentTerm++
	fmt.Println(rf.me, " is starting the election on term ", rf.currentTerm)
	// vote for yourself
	rf.votedFor = rf.me
	// // clear voted for
	// rf.votedFor = -1

	// record current term that the election starts
	// If later this election time out then this term would be less than rf.currentTerm,
	// meaning this election result should be discarded

	recordTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		// sendRequestVote rpc to peers in goroutine, and deal with reply
		targetID := i // save index
		go func(wg *sync.WaitGroup) {
			defer wg.Done()

			// create struct
			lastLogTerm := -1
			rf.mu.Lock()
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
			voteArgs := RequestVoteArgs{
				recordTerm,
				rf.me,
				len(rf.log),
				lastLogTerm,
			}
			voteReply := RequestVoteReply{
				-1,
				false,
			}
			// send rpc call
			rf.mu.Unlock()
			rpcSuccess := rf.sendRequestVote(targetID, &voteArgs, &voteReply)
			rf.mu.Lock()
			if !rpcSuccess {
				fmt.Printf("sendRequestVote RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
			} else {
				// successful rpc, process result here

				// if the terms are not equal, return
				if rf.currentTerm != recordTerm {
					rf.mu.Unlock()
					return
				}

				// if the server vote for you
				if voteReply.VoteGranted {
					electionLock.Lock()
					majorityCount++
					// election succeed, should process here because we don't want blocking(unreachable) rpc to halt the whole election
					if majorityCount >= threshold && rf.currentTerm == recordTerm && !rf.isLeader {
						rf.isLeader = true
						fmt.Printf("new leader is ID %v with term %v \n", rf.me, rf.currentTerm)
						go func() { rf.heartBeats() }()
					}
					electionLock.Unlock()

				} else {
					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower!
					if voteReply.Term > rf.currentTerm {
						rf.currentTerm = voteReply.Term
						rf.votedFor = -1
					}
				}
			}
			rf.mu.Unlock()
		}(&wg)
	}
	wg.Wait()
	// election succeed
	// rf.mu.Lock()
	// // when it has a majority of votes
	// // and the election term hasn't changed,
	// // i.e. the server hasn't started a new election && no other leader has won
	// fmt.Printf("ID '%v' on term '%v-%v' has majority count '%v' \n", rf.me, rf.currentTerm, recordTerm, majorityCount)
	// //fmt.Println(rf.currentTerm, recordTerm, rf.me)
	// if majorityCount > len(rf.peers)/2 && rf.currentTerm == recordTerm {
	// 	rf.isLeader = true
	// 	fmt.Printf("new leader is ID %v with term %v \n", rf.me, rf.currentTerm)
	// 	// !!!!!!!!!!!!!!!!!!
	// 	// !!!!!!!!!!!!!!!!!!
	// 	// !!!!!!!!!!!!!!!!!!
	// 	// Code for leader initialization
	// 	go func() { rf.heartBeats() }()
	// 	// !!!!!!!!!!!!!!!!!!
	// 	// !!!!!!!!!!!!!!!!!!
	// 	// !!!!!!!!!!!!!!!!!!
	// }
	// rf.mu.Unlock()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// get random timeout, in Milliseconds, increase
func getRandTimeoutDuration(base int) time.Duration {
	//return time.Millisecond * time.Duration(base/3+3*rand.Intn(base)/3)
	return time.Millisecond * time.Duration(600+rand.Intn(base))
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).

	// persistent states
	rf.currentTerm = 0
	rf.votedFor = -1 // represent nil
	rf.log = []*LogStruct{}

	// volatile states
	rf.commitIndex = 0
	rf.lastApplied = 0

	// leader for states

	rf.isLeader = false
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	// timeout & election parameters
	rf.lastHeardFromLeader = time.Now()
	rf.timeLimit = getRandTimeoutDuration(300)
	// fmt.Println(rf.timeLimit)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start checktimeouts
	go func() { rf.checkTimeouts() }()
	return rf
}
