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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// helper min function for comparing integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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

	// condition variable sync.Cond for triggering applyCommited
	applyCond *sync.Cond
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

// Helper function that prints out the log of the server
func (rf *Raft) printLog() {
	// var f string
	// if rf.isLeader {
	// 	f = "true"
	// } else {
	// 	f = "false"
	// }
	// fmt.Printf("The log entries of the %v-th server which is a leader? %v, its commiteIndex is %v and lastApplied is %v \n", rf.me, f, rf.commitIndex, rf.lastApplied)
	// for i, l := range rf.log {
	// 	fmt.Print(" ", i, ": ")
	// 	fmt.Print(l)
	// }
	// fmt.Println("")
}

// Helper function that insert a LogStruct to the given index of the lock
// Will only insert if the index exists or could be added.
// E.g. [1,2] insert at index 3 is okay, [1,2] insert at index 4 is bad!
// Index is the raft index, which is 1-indexed.
func (rf *Raft) insertLog(index int, command interface{}) {
	if len(rf.log) < index-1 {
		fmt.Println("Something very wrong in insertLog!!")
	} else if len(rf.log) == index-1 {
		rf.log = append(rf.log, &LogStruct{rf.currentTerm, command})
	} else {
		rf.log[index] = &LogStruct{rf.currentTerm, command}
	}
}

// Update state to either follower or leader. This doesn't necessarily involve
// state changes. It could be a leader -> leader || follower -> follower.
// Both need to have higher terms to update.
// The reason we don't put "candidate" state here is because
// candidate is actually follower in election. Nothing special
// needs to be done for a follower to be a candidate.
func (rf *Raft) updateState(state string, targetTerm int) {
	possibleStates := map[string]bool{"follower": true, "leader": true}
	key := strings.ToLower(state)
	if _, ok := possibleStates[key]; !ok {
		fmt.Printf("The state '%v' you want to change to is not valid! \n", state)
	}
	if key == "follower" {
		rf.currentTerm = targetTerm
		rf.votedFor = -1
		if rf.isLeader {
			rf.isLeader = false
			// go rf.checkTimeouts()
			go func() { rf.checkTimeouts() }()
		}
	} else if key == "leader" {
		rf.isLeader = true
		fmt.Printf("new leader is ID %v with term %v \n", rf.me, rf.currentTerm)
		// initialize nextIndex[] and matchIndex[]
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
			rf.matchIndex[i] = 0
		}
		// start enhancedHeartBeats()
		go func() { rf.heartBeats() }()
	}
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
// A goroutine that periodically apply newly commited command to the channel (application)
// Using Conditional variable, this goroutine will only be waken when signaled.
func (rf *Raft) applyCommited(applyCh chan ApplyMsg) {
	for {
		rf.applyCond.L.Lock()
		rf.applyCond.Wait()
		// ready to apply command
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			// increment lastApplied
			rf.lastApplied++
			// apply log[lastApplied] to state machine
			newMsg := ApplyMsg{true, rf.log[rf.lastApplied-1].Command, rf.commitIndex}
			applyCh <- newMsg
			// fmt.Println("&&&&&& ", rf.me, " is commiting", rf.log[rf.lastApplied-1].Command)
		}
		rf.mu.Unlock()

		rf.applyCond.L.Unlock()
	}
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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else {
		// update term
		if args.Term > rf.currentTerm {
			rf.updateState("follower", args.Term)
		}
		// if haven't voted for anyone, then this server is allowed to vote
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			lastLogTerm := -1
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
			lastLogIndex := len(rf.log)
			fmt.Println(rf.me, "is receiving")
			// if candidate’s log is at least as up-to-date as receiver’s log, grant vote
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				fmt.Println(rf.me, "is voting")
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				// reset timer
				rf.resetTimer()
			}
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
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// fmt.Println("@@@@@@@@@@@@@@", args.Term, rf.currentTerm)
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.updateState("follower", args.Term)
		// this will abort any undergoing election because
		// the new term is promised to be higher than the term during election
	}
	if args.Term >= rf.currentTerm {
		// empty log entry means heartbeat, gonna reset time out value
		// Normal AppendEntries RPC from leader will reset timer as well
		// must prevent invalid leader (older terms) to reset the timer`
		rf.resetTimer()
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	// fmt.Println(args.LogEntires)
	// 2B deal with real log entries
	if args.LogEntires != nil && len(args.LogEntires) > 0 {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		fmt.Println("$$$$$$$$$$$")
		fmt.Println(rf.me, args.PrevLogIndex, rf.log)
		if len(rf.log) < args.PrevLogIndex || (len(rf.log) > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			reply.Success = false
		} else {
			// If an existing entry conflicts with a new one (same index but
			// different terms), delete the existing entry and all that follow it
			i := 1

			for ; i <= len(args.LogEntires); i++ {
				toInsertIndex := args.PrevLogIndex + i
				if len(rf.log) < toInsertIndex {
					break
				}
				if rf.log[toInsertIndex-1].Term != args.LogEntires[i-1].Term {
					for j := i; j <= len(args.LogEntires); j++ {
						if len(rf.log) < args.PrevLogIndex+j {
							break
						}
						rf.log[args.PrevLogIndex+j-1] = nil // delete
					}
					break
				}
			}
			// Append any new entries not already in the log {
			for ; i <= len(args.LogEntires); i++ {
				toInsertIndex := args.PrevLogIndex + i
				// fmt.Println("!!!!!!!! ", toInsertIndex)
				// rf.log[toInsertIndex-1] = args.LogEntires[i-1]
				rf.insertLog(toInsertIndex, args.LogEntires[i-1].Command)
			}
		}

	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		// follower signal commit
		for rf.commitIndex > rf.lastApplied && oldCommitIndex < rf.commitIndex {
			rf.applyCond.Signal()
			oldCommitIndex++
		}
	}

	// fmt.Println("leader commit is ", args.LeaderCommit, " commitIndex is ", rf.commitIndex)
	rf.printLog()

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
	// fmt.Println(command)
	rf.mu.Lock()
	// if not leader, return false immediately
	if !rf.isLeader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	// don't need to have a goroutine to work on commit this command
	// because of the enhancedHeartBeats() will check this periodically

	// apply to leader's local log, should be the next index of the current commitIndex.
	// If that location is occupied, definitely sends a warning
	targetCommitIndex := rf.commitIndex + 1
	// if rf.log[targetCommitIndex] != nil {
	// 	fmt.Println("Leader's local log already occupied. Some elements get overwritten!!")
	// }

	// rf.log[targetCommitIndex] = &LogStruct{rf.currentTerm, command}

	rf.insertLog(targetCommitIndex, command)

	// update matchIndex and nextIndex for leader
	rf.matchIndex[rf.me]++
	rf.nextIndex[rf.me]++

	// if commited, then the index should be the next index of the current commitIndex
	index = targetCommitIndex
	term = rf.currentTerm

	// return when this is applied to local machine! Wrong! Should return immediately
	// currentApplied := rf.lastApplied
	rf.mu.Unlock()
	rf.broadcastEntries(false)
	// for currentApplied < targetCommitIndex {
	// 	time.Sleep(time.Millisecond * 20)
	// 	rf.mu.Lock()	// 	rf.mu.Lock()	// 	rf.mu.Lock()	// 	rf.mu.Lock()

	// 	currentApplied = rf.lastApplied
	// 	rf.mu.Unlock()
	// }
	// fmt.Println("Yes yes yes yes")
	// fmt.Println(index, term, isLeader)
	// rf.printLog()
	// rf.mu.Lock()
	// fmt.Println(">>>>>>>>>>>>>>")
	// fmt.Println(rf.me, rf.lastApplied, rf.nextIndex, rf.matchIndex)
	// rf.mu.Unlock() // debug only

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
		// fmt.Println(rf.me, " is checking timeout", "I am a leader?", rf.isLeader)
		// durationPassed := time.Now().Sub(rf.lastHeardFromLeader).Nanoseconds() / 1e6
		durationPassed := time.Now().Sub(rf.lastHeardFromLeader)
		// leader doesn't need checkTimeouts
		// if rf.isLeader {
		// 	rf.mu.Unlock()
		// 	return
		// } else if durationPassed > rf.timeLimit { // overdue, should start an election
		if !rf.isLeader && durationPassed > rf.timeLimit { // overdue, should start an election
			// have a separate goroutine deal with election because checkTimeouts need to keep monitoring

			// debug
			if rf.isLeader {
				// fmt.Println("********************")
			}

			go func() { rf.startElection() }()
			// reset election timer
			rf.resetTimer()
			rf.mu.Unlock()
		} else {
			// sleep for a while, 10ms here
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

//
// 'enhancedHeartBeats' regularly sends heart beats to followers.
//	This should be run on a dedicated goroutine and only a leader will run this.
//
// This function will send heart beats when no updates are needed.
// When last log index ≥ nextIndex for a follower, then there needs an update for this follower.
//
func (rf *Raft) heartBeats() {
	// only the leader is required to do this
	rf.mu.Lock()
	stillLeader := rf.isLeader
	rf.mu.Unlock()

	for stillLeader {
		rf.mu.Lock()
		// fmt.Println("current leader is", rf.me, rf.isLeader, rf.currentTerm)
		rf.mu.Unlock()
		rf.printLog()
		go func() { rf.broadcastEntries(true) }()
		// sleep for a while. Limit 10 heartbeats per second
		time.Sleep(time.Millisecond * 120)
		rf.mu.Lock()
		stillLeader = rf.isLeader
		rf.mu.Unlock()
	}
}

// This function sends append entries to clients and wait for it to be done.
// This requires to be done by a goroutine.
// heartbeats() and updateFollowerLogs() would all dispatch a goroutine to call this function
func (rf *Raft) broadcastEntries(isHeartBeat bool) {
	// debug message
	// fmt.Println("Message from leader ", rf.me, " match index: ", rf.matchIndex, " nextIndex: ", rf.nextIndex)
	wg := sync.WaitGroup{}
	// send heartbeat to followers
	for i := range rf.peers {
		// skip leader itself
		if i == rf.me {
			continue
		}

		wg.Add(1)

		targetID := i // save index
		// this goroutine send RPC, process RPC and establish leadership having majority votes
		go func(wgg *sync.WaitGroup, rf *Raft) {

			rf.mu.Lock()
			isSuccess := false
			rf.mu.Unlock()
			// if the response is not successful, then there are two possible causes:
			// 1. This leader is outdated and it should step down immediately (break out of the loop)
			// 2. AppendEntries fails because of log inconsistency, we decrement nextIndex and retry
			for !isSuccess {
				// create args structure
				rf.mu.Lock()
				// last log index
				nextIndex := rf.nextIndex[targetID]

				prevLogIndex := nextIndex - 1

				// last log term
				prevLogTerm := -1
				if prevLogIndex > 0 {
					prevLogTerm = rf.log[prevLogIndex-1].Term
				}

				// log entries to apply
				// rf.printLog()
				logs := []*LogStruct{}
				// when heartbeats, send empty logs, else send full logs
				if !isHeartBeat && len(rf.log) > 0 {
					logs = rf.log[nextIndex-1:]
				}
				// fmt.Println(logs)
				// fmt.Println(rf.isLeader, rf.me, logs, isHeartBeat)
				// save term
				recordTerm := rf.currentTerm

				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex, // prevLogIndex
					prevLogTerm,
					logs, // the input argument
					rf.commitIndex,
				}

				rf.mu.Unlock()

				reply := AppendEntriesReply{
					-1,
					false,
				}
				// send RPC call
				rpcSuccess := rf.sendAppendEntries(targetID, &args, &reply)
				// heartbeat simply returns
				if isHeartBeat {
					break
				}

				// Below is broadcasting real log entries:
				//
				isSuccess = reply.Success
				rf.mu.Lock()
				// leader out dated
				if recordTerm != rf.currentTerm {
					fmt.Println("Leader ", rf.me, rf.isLeader, " is outdated during sending enhancedHeart beats to server ", targetID, rf.currentTerm)
					rf.mu.Unlock()
					break
				}

				if !rpcSuccess {
					fmt.Printf("sendAppendEntries RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
					rf.mu.Unlock()
				} else {
					if isSuccess { // If successful: update nextIndex and matchIndex for follower
						// since success from follower means all logs have been up to date,
						// we put matchIndex to be the length of current log and nextIndex to be matchIndex + 1
						rf.matchIndex[targetID] = len(rf.log)
						rf.nextIndex[targetID] = rf.matchIndex[targetID] + 1

						rf.mu.Unlock()
					} else if reply.Term > rf.currentTerm { // if the follower has a higher term than the leader, the leader should convert to follower
						// rf.currentTerm = reply.Term
						fmt.Println("pppppppppppppp", rf.me, targetID, rf.currentTerm, reply.Term, rf.isLeader)
						rf.updateState("follower", reply.Term)
						rf.mu.Unlock()
						// go func() { rf.checkTimeouts() }()
						// escape the check loop
						break
					} else { // because of log inconsistency, decrement nextIndex and retry
						// fmt.Println("%%%%%%%%%% ", reply.Term, rf.currentTerm)
						rf.nextIndex[targetID]--
						rf.mu.Unlock()
					}
				}
			}
			wgg.Done()
		}(&wg, rf)
	}
	wg.Wait()

	// Apply the last rule for leader here, i.e. a log replicated on a majority of servers but not commited,
	// then this log is actually commited and we need to forward our commitIndex to that point
	rf.mu.Lock()
	// use a map to store all matchIndex. Sort it. If there is a matchIndex = N that satisfy the requirements, set commitIndex = N
	m := make(map[int]int)
	for _, matchIndex := range rf.matchIndex {
		m[matchIndex]++ // default is 0
	}
	keys := []int{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keys)))
	sum := 0
	for _, N := range keys {
		if N <= rf.commitIndex {
			break
		}
		sum += m[N]
		// if exist a majority of matchIndex[i] ≥ N,
		// and log[N].term == currentTerm, set commitIndex = N
		if sum > len(rf.peers)/2 && rf.log[N-1].Term == rf.currentTerm {
			rf.commitIndex = N
			rf.applyCond.Signal()
			break // break loop
		}
	}
	rf.mu.Unlock()
}

// func (rf *Raft) broadcastEntries() {
// 	// debug message
// 	// fmt.Println("Message from leader ", rf.me, " match index: ", rf.matchIndex, " nextIndex: ", rf.nextIndex)
// 	wg := sync.WaitGroup{}
// 	// send heartbeat to followers
// 	for i := range rf.peers {
// 		// skip leader itself
// 		if i == rf.me {
// 			continue
// 		}

// 		wg.Add(1)

// 		targetID := i // save index
// 		// this goroutine send RPC, process RPC and establish leadership having majority votes
// 		go func(wgg *sync.WaitGroup, rf *Raft) {

// 			rf.mu.Lock()
// 			isSuccess := false
// 			rf.mu.Unlock()
// 			// if the response is not successful, then there are two possible causes:
// 			// 1. This leader is outdated and it should step down immediately (break out of the loop)
// 			// 2. AppendEntries fails because of log inconsistency, we decrement nextIndex and retry
// 			for !isSuccess {
// 				// create args structure
// 				rf.mu.Lock()
// 				// last log index
// 				lastRfLogIndex := len(rf.log)

// 				// last log term
// 				prevLogTerm := -1
// 				if len(rf.log) > 0 {
// 					prevLogTerm = rf.log[len(rf.log)-1].Term
// 				}

// 				// log entries to apply
// 				// rf.printLog()
// 				logs := []*LogStruct{}
// 				nextIndex := rf.nextIndex[targetID]
// 				// fmt.Println(rf.log, nextIndex)
// 				if lastRfLogIndex >= nextIndex {
// 					logs = rf.log[nextIndex-1:]
// 				}
// 				// fmt.Println(logs)

// 				// save term
// 				recordTerm := rf.currentTerm

// 				args := AppendEntriesArgs{
// 					rf.currentTerm,
// 					rf.me,
// 					lastRfLogIndex - 1, // prevLogIndex
// 					prevLogTerm,
// 					logs,
// 					rf.commitIndex,
// 				}

// 				rf.mu.Unlock()

// 				reply := AppendEntriesReply{
// 					-1,
// 					false,
// 				}
// 				// send RPC call
// 				rpcSuccess := rf.sendAppendEntries(targetID, &args, &reply)
// 				isSuccess = reply.Success
// 				rf.mu.Lock()
// 				// leader out dated
// 				if recordTerm != rf.currentTerm {
// 					fmt.Println("Leader ", rf.me, rf.isLeader, " is outdated during sending enhancedHeart beats to server ", targetID, rf.currentTerm)
// 					rf.mu.Unlock()
// 					break
// 				}

// 				if !rpcSuccess {
// 					// fmt.Printf("sendAppendEntries RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
// 					rf.mu.Unlock()
// 				} else {
// 					if isSuccess { // If successful: update nextIndex and matchIndex for follower
// 						// since success from follower means all logs have been up to date,
// 						// we put matchIndex to be the length of current log and nextIndex to be matchIndex + 1
// 						rf.matchIndex[targetID] = len(rf.log)
// 						rf.nextIndex[targetID] = rf.matchIndex[targetID] + 1

// 						rf.mu.Unlock()
// 					} else if reply.Term > rf.currentTerm { // if the follower has a higher term than the leader, the leader should convert to follower
// 						// rf.currentTerm = reply.Term
// 						fmt.Println("pppppppppppppp", rf.me, targetID, rf.currentTerm, reply.Term, rf.isLeader)
// 						rf.updateState("follower", reply.Term)
// 						rf.mu.Unlock()
// 						// go func() { rf.checkTimeouts() }()
// 						// escape the check loop
// 						break
// 					} else { // because of log inconsistency, decrement nextIndex and retry
// 						// fmt.Println("%%%%%%%%%% ", reply.Term, rf.currentTerm)
// 						rf.nextIndex[targetID]--
// 						rf.mu.Unlock()
// 					}
// 				}
// 			}
// 			wgg.Done()
// 		}(&wg, rf)
// 	}
// 	wg.Wait()

// 	// Apply the last rule for leader here, i.e. a log replicated on a majority of servers but not commited,
// 	// then this log is actually commited and we need to forward our commitIndex to that point
// 	rf.mu.Lock()
// 	// use a map to store all matchIndex. Sort it. If there is a matchIndex = N that satisfy the requirements, set commitIndex = N
// 	m := make(map[int]int)
// 	for _, matchIndex := range rf.matchIndex {
// 		m[matchIndex]++ // default is 0
// 	}
// 	keys := []int{}
// 	for k := range m {
// 		keys = append(keys, k)
// 	}
// 	sort.Sort(sort.Reverse(sort.IntSlice(keys)))
// 	sum := 0
// 	for _, N := range keys {
// 		if N <= rf.commitIndex {
// 			break
// 		}
// 		sum += m[N]
// 		// if exist a majority of matchIndex[i] ≥ N,
// 		// and log[N].term == currentTerm, set commitIndex = N
// 		if sum > len(rf.peers)/2 && rf.log[N-1].Term == rf.currentTerm {
// 			rf.commitIndex = N
// 			rf.applyCond.Signal()
// 			break // break loop
// 		}
// 	}
// 	rf.mu.Unlock()
// }

func (rf *Raft) startElection() {
	// always vote for itself
	majorityCount := 1
	wg := sync.WaitGroup{}
	electionLock := sync.Mutex{}

	threshold := len(rf.peers)/2 + 1

	rf.mu.Lock()
	// Increment current Term
	rf.currentTerm++
	fmt.Println(rf.me, rf.isLeader, " is starting the election on term ", rf.currentTerm, rf.log, rf.matchIndex, rf.nextIndex)
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
			// send rpc call, release lock to prevent deadlock in rpc handler
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
						// change state to leader
						rf.updateState("leader", -99999) // term is not used
					}
					electionLock.Unlock()
				} else {
					// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower!
					if voteReply.Term > rf.currentTerm {
						rf.updateState("follower", voteReply.Term)
					}
				}
			}
			rf.mu.Unlock()
		}(&wg)
	}
	wg.Wait()
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

// reset raft election timeout value
func (rf *Raft) resetTimer() {
	rf.lastHeardFromLeader = time.Now()
	rf.timeLimit = getRandTimeoutDuration(300)
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
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize Cond variable
	m := sync.Mutex{}
	rf.applyCond = sync.NewCond(&m)

	// timer parameters

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initialize timer
	rf.resetTimer()

	// start checktimeouts
	go func() { rf.checkTimeouts() }()

	//
	// go func() { rf.heartBeats() }()

	// start applying commited messages
	go func() { rf.applyCommited(applyCh) }()

	return rf
}
