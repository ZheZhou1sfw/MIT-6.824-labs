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
type logStruct struct {
	term    int
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
	log         []*logStruct

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
	term = rf.currentTerm
	isleader = rf.isLeader

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
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		lastLogTerm := rf.log[len(rf.log)-1].term
		lastLogIndx := len(rf.log)
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndx) {
			reply.VoteGranted = true
		}
	}
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
	LogEntires   []*logStruct
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
		// durationPassed := time.Now().Sub(rf.lastHeardFromLeader).Nanoseconds() / 1e6
		durationPassed := time.Now().Sub(rf.lastHeardFromLeader)
		// leader doesn't need checkTimeouts
		if rf.isLeader {
			return
		} else if durationPassed > rf.timeLimit { // overdue, should start an election
			// have a separate goroutine deal with election because checkTimeouts need to keep monitoring
			go rf.startElection()
			// reset election timer
			rf.lastHeardFromLeader = time.Now()
		} else {
			// sleep for a while
			time.Sleep(getRandTimeoutDuration(40))
		}
	}
}

//
// 'heartBeats' regularly sends heart beats to followers.
//
// This should be run on a dedicated goroutine and only a leader will run this.
//
func (rf *Raft) heartBeats() {

}

func (rf *Raft) startElection() {
	// always vote for itself
	majorityCount := 1
	wg := sync.WaitGroup{}
	electionLock := sync.Mutex{}

	// Increment current Term
	rf.currentTerm++

	// record current term that the election starts
	// If later this election time out then this term would be less than rf.currentTerm,
	// meaning this election result should be discarded
	rf.mu.Lock()
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
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].term
			}
			voteArgs := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				len(rf.log),
				lastLogTerm,
			}
			voteReply := RequestVoteReply{
				-1,
				false,
			}
			// send rpc call
			rpcSuccess := rf.sendRequestVote(targetID, &voteArgs, &voteReply)
			if !rpcSuccess {
				fmt.Printf("snedRequestVote RPC is not successful from senderID %v to receiverID %v", rf.me, targetID)
			} else {
				// successful rpc, process result here

				// if the server vote for you
				if voteReply.VoteGranted {
					electionLock.Lock()
					majorityCount++
					electionLock.Unlock()
				} else {
					rf.mu.Lock()
					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower!
					if voteReply.Term > rf.currentTerm {
						rf.currentTerm = voteReply.Term
					}
					rf.mu.Unlock()
				}
			}
		}(&wg)

	}
	wg.Wait()
	// election succeed
	rf.mu.Lock()
	// when it has a majority of votes
	// and the election term hasn't changed,
	// i.e. the server hasn't started a new election && no other leader has won
	if majorityCount > len(rf.peers)/2 && rf.currentTerm == recordTerm {
		rf.isLeader = true
		// !!!!!!!!!!!!!!!!!!
		// !!!!!!!!!!!!!!!!!!
		// !!!!!!!!!!!!!!!!!!
		// Code for leader initialization

		// !!!!!!!!!!!!!!!!!!
		// !!!!!!!!!!!!!!!!!!
		// !!!!!!!!!!!!!!!!!!
	}
	rf.mu.Unlock()
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

// get random timeout, in Milliseconds
func getRandTimeoutDuration(base int) time.Duration {
	return time.Millisecond * time.Duration(base/4*3+rand.Intn(base)/3)
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
	rf.log = []*logStruct{}

	// volatile states
	rf.commitIndex = 0
	rf.lastApplied = 0

	// leader for states

	rf.isLeader = false
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	// timeout & election parameters
	rf.lastHeardFromLeader = time.Now()
	rf.timeLimit = getRandTimeoutDuration(800)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
