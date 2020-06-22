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
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// a helper struct that helps the data comprehension during snapshot
type SnapshotComplex struct {
	IdentifiersMap map[string]string
	KeyValueMap    map[string]string
}

// helper min function for comparing integers, return the smaller one
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// helper max function for comparing integers, return the larger one
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

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
	CommandType  string // currently only "snapshot" is used
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
	Persister *Persister          // Object to hold this peer's persisted state
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

	// snapshot variable for log compaction
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotState     SnapshotComplex
	snapshotSent      bool
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.isLeader
	rf.mu.Unlock()

	return term, isleader
}

// return last applied index and term
func (rf *Raft) GetLastAppliedMeta() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < rf.lastIncludedIndex {
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
		rf.mu.Lock()
	}
	lastIncludedIndex := rf.lastApplied
	fmt.Println("....", rf.me, rf.isLeader, rf.log, rf.lastApplied, rf.lastIncludedIndex, rf.snapshotSent)
	lastIncludedTerm := 0
	if rf.lastApplied == rf.lastIncludedIndex {
		lastIncludedTerm = rf.lastIncludedTerm
	} else {
		lastIncludedTerm = rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Term
	}

	// rf.lastIncludedIndex = lastIncludedIndex
	// rf.lastIncludedTerm = lastIncludedTerm

	return lastIncludedIndex, lastIncludedTerm
}

// Helper function that prints out the log of the server,
// and some messages for diagnosing.
func (rf *Raft) printLog() {
	f := "false"
	if rf.isLeader {
		f = "true"
	}
	fmt.Printf("The log entries of the %v-th server which is a leader? %v, its commiteIndex is %v and lastApplied is %v, matchIndex is %v \n", rf.me, f, rf.commitIndex, rf.lastApplied, rf.matchIndex)
	for i, l := range rf.log {
		fmt.Print(" ", i, ": ", l)
	}
	fmt.Println("")
}

// Helper function that insert a LogStruct to the given index of the lock
// Will only insert if the index exists or could be added.
// E.g. [1,2] insert at index 3 is okay, [1,2] insert at index 4 is bad!
// Index is the raft index, which is 1-indexed.
func (rf *Raft) insertLog(index int, command interface{}) {
	// if this is the case, then we are inserting beyond the possible location
	if len(rf.log) < index-1 {
		fmt.Println("The index you want to insert the log is beyond the maximum possible location")
		return
	} else if len(rf.log) == index-1 { // append
		rf.log = append(rf.log, &LogStruct{rf.currentTerm, command})
	} else { // replace
		rf.log[index-1] = &LogStruct{rf.currentTerm, command}
	}
	rf.persist()
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
		rf.persist()
		if rf.isLeader {
			rf.isLeader = false
		}
	} else if key == "leader" {
		rf.isLeader = true
		fmt.Printf("new leader is ID %v with term %v \n", rf.me, rf.currentTerm)
		// initialize nextIndex[] and matchIndex[]
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex + 1
			rf.matchIndex[i] = 0
		}
		// the match index of leader itself is known
		rf.matchIndex[rf.me] = len(rf.log) + rf.lastIncludedIndex
		// start heartBeats()
		go func() { rf.heartBeats() }()
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.Persister.SaveRaftState(rf.getRaftStateData())
}

func (rf *Raft) getRaftStateData() []byte {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []*LogStruct
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		// log.Fatal("decode error")
		// fmt.Println("read snapshot doesn't exist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor // represent nil
		rf.log = logs
	}
}

// called by kvserver (outside), that's why lock is required
// should discard log here as well !!!!!
func (rf *Raft) PersistSnapshot(snapshotData []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// discard old log entries
	// fmt.Println("PPPPPPPPP", rf.me, rf.log, rf.lastApplied, rf.lastIncludedIndex)
	rf.log = rf.log[lastIncludedIndex-rf.lastIncludedIndex:]

	data := rf.getRaftStateData()
	rf.Persister.SaveStateAndSnapshot(data, snapshotData)
	rf.readSnapshot()
	fmt.Println(rf.snapshotState)
	rf.snapshotSent = true // because kvserver started snapshot, which doesn't require sending back
	// fmt.Println("persisted snapshot", rf.me, rf.isLeader, rf.log, rf.lastIncludedIndex)
}

func (rf *Raft) readSnapshot() {
	snapshotData := rf.Persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	// var includedKeyValueMap map[string]string
	// var identifiersMap map[string]bool
	var snapshotComplex SnapshotComplex

	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		// d.Decode(&includedKeyValueMap) != nil ||
		// d.Decode(&identifiersMap) != nil {
		d.Decode(&snapshotComplex) != nil {
		// log.Fatal("decode error")
		rf.snapshotSent = true // doesn't need to send
	} else {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshotState = snapshotComplex
	}
}

//
// A goroutine that periodically applies newly commited commands to the channel (application).
// Using Conditional variable, this goroutine will only be waken when signaled.
func (rf *Raft) applyCommited(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.applyCond.L.Lock()
		rf.applyCond.Wait()
		// ready to apply command
		rf.mu.Lock()
		// term outdated, should send snapshot to server
		fmt.Println("?????", rf.me, rf.isLeader, rf.snapshotSent, len(rf.log))
		if !rf.snapshotSent {
			// fmt.Println("raft apply snapshot to server via channel")

			newMsg := ApplyMsg{false, rf.snapshotState, 0, "snapshot"}
			rf.snapshotSent = true
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			applyCh <- newMsg
			rf.mu.Lock()
		}
		if rf.snapshotSent && rf.commitIndex > rf.lastApplied {
			// fmt.Println("\\\\\\\\", rf.me, rf.isLeader, rf.commitIndex, rf.lastApplied)
			// for rf.commitIndex > rf.lastApplied && rf.lastApplied < len(rf.log) {
			for rf.commitIndex > rf.lastApplied && rf.snapshotSent {
				// increment lastApplied
				rf.lastApplied++
				// apply log[lastApplied] to state machine
				fmt.Println("\\\\\\\\", rf.me, rf.isLeader, rf.snapshotSent, rf.commitIndex, rf.lastApplied, len(rf.log), rf.lastIncludedIndex, time.Now().UnixNano())
				newMsg := ApplyMsg{true, rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Command, rf.lastApplied, "normal"}
				// fmt.Println("newMSG here", rf.me, newMsg, rf.log, rf.lastIncludedIndex)
				rf.mu.Unlock()
				applyCh <- newMsg
				rf.mu.Lock()
			}
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
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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
			// fmt.Println(rf.me, "is receiving a vote")
			// if candidate’s log is at least as up-to-date as receiver’s log, grant vote
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				// fmt.Println(rf.me, "is voting for some other node")
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
				rf.persist()
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
	LogEntries   []*LogStruct
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term            int
	Success         bool
	ConflictingTerm int
	IndexOfConflict int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A deal with heartbeats'
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("head of append entries", rf.me, rf.isLeader, rf.currentTerm, args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.IndexOfConflict = -200
		return
	} else if args.Term > rf.currentTerm {
		rf.updateState("follower", args.Term)
		reply.Term = rf.currentTerm
		// this will abort any undergoing election because
		// the new term is promised to be higher than the term during election
	}
	if args.Term >= rf.currentTerm {
		// empty log entry means heartbeat, gonna reset time out value
		// Normal AppendEntries RPC from leader will reset timer as well
		// must prevent invalid leader (older terms) to reset the timer`
		rf.resetTimer()
	}

	// Update on 06/07/2020, this happens when
	// an outdated broadcastentries comes, we shouldn't proceed.
	// set reply.Success = false is more secure IMO.
	if args.LeaderCommit < rf.commitIndex && args.PrevLogIndex < len(rf.log)+rf.lastIncludedIndex-1 {
		reply.Success = false
		reply.ConflictingTerm = -500
		return
	}

	// if args.LeaderCommit < rf.commitIndex {
	// 	reply.Success = false
	// 	reply.ConflictingTerm = -1
	// 	reply.IndexOfConflict = len(rf.log) + 1
	// 	return
	// }

	// 2B deal with real log entries
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	fmt.Println("[[[[", rf.me, rf.isLeader, len(rf.log), rf.commitIndex, args.PrevLogIndex, rf.lastIncludedIndex)
	if len(rf.log)+rf.lastIncludedIndex < args.PrevLogIndex ||
		(len(rf.log)+rf.lastIncludedIndex > 0 && args.PrevLogIndex > 0 &&
			((args.PrevLogIndex == rf.lastIncludedIndex && rf.lastIncludedTerm != args.PrevLogTerm) ||
				(args.PrevLogIndex > rf.lastIncludedIndex && rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term != args.PrevLogTerm))) {
		reply.Success = false
		// Advanced technique to skip more than one nextIndex at a time.
		// This part strictly follows the guide.
		if len(rf.log)+rf.lastIncludedIndex < args.PrevLogIndex {
			reply.ConflictingTerm = -1
			reply.IndexOfConflict = len(rf.log) + 1 + rf.lastIncludedIndex
		} else { // not found that log position
			reply.ConflictingTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
			theIndex := args.PrevLogIndex - rf.lastIncludedIndex
			for theIndex > 0 {
				if rf.log[theIndex-1].Term == reply.ConflictingTerm {
					theIndex--
				} else {
					break
				}
			}
			reply.IndexOfConflict = theIndex + 1 + rf.lastIncludedIndex
		}
	} else {
		reply.Success = true
		// If an existing entry conflicts with a new one (same index but
		// different terms), delete the existing entry and all that follow it

		// i is the index of args.LogEntries that mismatch occurs
		i := 1

		for ; i <= len(args.LogEntries); i++ {
			toInsertIndex := args.PrevLogIndex + i - rf.lastIncludedIndex
			if len(rf.log) < toInsertIndex {
				break
			}
			fmt.Println("to to to insert", rf.me, rf.isLeader, rf.lastIncludedIndex, args.PrevLogIndex, toInsertIndex)
			if toInsertIndex < 0 {
				continue
			}
			if toInsertIndex == 0 && rf.lastIncludedTerm != args.LogEntries[i-1].Term {
				rf.log = []*LogStruct{}
				rf.persist()
			} else if toInsertIndex > 0 && rf.log[toInsertIndex-1].Term != args.LogEntries[i-1].Term {
				fmt.Println("dddddd", rf.me, rf.isLeader, len(rf.log), toInsertIndex, i, rf.log[toInsertIndex-1], args.LogEntries[i-1])
				rf.log = rf.log[:toInsertIndex-1]
				rf.persist()
				break
			}
		}
		// Append any new entries not already in the log
		fmt.Println("apppppp", rf.me, rf.isLeader, args.PrevLogIndex, i, len(rf.log), len(args.LogEntries), args.LeaderCommit, rf.commitIndex, rf.lastIncludedIndex)

		// only append new entries not already in the log!
		// if len(rf.log) < args.PrevLogIndex+len(args.LogEntries) {
		if len(args.LogEntries) > 0 {
			rf.log = append(rf.log[:args.PrevLogIndex+i-1-rf.lastIncludedIndex], args.LogEntries[i-1:]...)
			rf.persist()
		}

		// }

		if args.LeaderCommit > rf.commitIndex {
			fmt.Println("<<<<<<", rf.me, rf.isLeader, rf.commitIndex, rf.lastIncludedIndex, args.LeaderCommit, args.PrevLogIndex, len(args.LogEntries), len(rf.log), time.Now().UnixNano())
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.LogEntries))
			// follower signal commit
			// fmt.Println("-----", rf.me, rf.isLeader, rf.commitIndex, rf.lastApplied)
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
		}

	}
	// fmt.Println("End append entries", rf.me, rf.isLeader, rf.log, rf.lastIncludedIndex, rf.commitIndex, args.LeaderCommit, reply)

	// rf.printLog()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              SnapshotComplex
	// Done           bool
	// Offset         int
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//
// InstallSnapshot RPC Handler
//
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// fmt.Println("XXXXXXXXXXX", rf.me, rf.isLeader, args, reply, rf.currentTerm)
	if args.Term < rf.currentTerm {
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

	// check if the snapshot is newer than current snapshot
	if args.LastIncludedIndex < rf.lastIncludedIndex ||
		(args.LastIncludedIndex == rf.lastIncludedIndex &&
			args.LastIncludedTerm < rf.lastIncludedTerm) {
		return
	}

	// update snapshot status, set snapshotSent to false
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshotState = args.Data
	rf.snapshotSent = false

	// // delete log to that point

	// if len(rf.log) >= args.LastIncludedIndex {
	// 	// delete partial log
	// 	rf.log = rf.log[args.LastIncludedIndex:]
	// } else {
	// 	// discard entire log
	// 	rf.log = []*LogStruct{}
	// }

	// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	// fmt.Println("RRRRRRRRRRR", rf.me, rf.isLeader, args.LastIncludedIndex, rf.lastIncludedIndex, rf.log)
	if len(rf.log)+rf.lastIncludedIndex >= args.LastIncludedIndex {
		if args.LastIncludedIndex == rf.lastIncludedIndex && rf.lastIncludedTerm == args.LastIncludedTerm {
			return
		} else if rf.log[args.LastIncludedIndex-rf.lastIncludedIndex-1].Term == args.LastIncludedTerm {
			rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
		}
	} else {
		rf.log = []*LogStruct{}
	}

	// apply snapshot to server
	rf.applyCond.Signal()
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
// RPC call to InstallSnapshot
//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	// fmt.Println("start start", rf.me, rf.isLeader, command, rf.log)
	// Your code here (2B).
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

	targetCommitIndex := len(rf.log) + 1

	rf.insertLog(targetCommitIndex, command)

	// update matchIndex and nextIndex for leader
	rf.matchIndex[rf.me]++
	rf.nextIndex[rf.me]++

	// if commited, then the index should be the next index of the current commitIndex
	index = targetCommitIndex + rf.lastIncludedIndex
	term = rf.currentTerm

	go func() { rf.broadcastEntries() }()

	// Should return immediately
	rf.mu.Unlock()
	// fmt.Println("end start", rf.me, rf.isLeader, index, term, isLeader, rf.log)
	// rf.printLog()
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
	// prevent goroutine leak
	for !rf.killed() {
		rf.mu.Lock()
		durationPassed := time.Now().Sub(rf.lastHeardFromLeader)
		// leader doesn't need checkTimeouts
		if !rf.isLeader && durationPassed > rf.timeLimit { // overdue, should start an election
			// have a separate goroutine deal with election because checkTimeouts need to keep monitoring
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

	// If the raft instance is killed, should kill this sub-goroutine to prevent goroutine leak.
	for stillLeader && !rf.killed() {
		// debug info:
		// rf.mu.Lock()
		// fmt.Println("current leader is", rf.me, rf.isLeader, rf.currentTerm)
		// rf.mu.Unlock()

		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		rf.mu.Lock()
		// lastLogIndex := len(rf.log)
		// f := false
		// for i := 0; i < len(rf.peers); i++ {
		// 	if i == rf.me {
		// 		continue
		// 	}
		// 	if rf.nextIndex[i] <= lastLogIndex {
		// 		f = true
		// 		go func() { rf.broadcastEntries(false) }()
		// 		break
		// 	}
		// }
		// if not sending appendentries, then do normal heartbeats
		// if !f {
		// 	go func() { rf.broadcastEntries(true) }()
		// }
		go func() { rf.broadcastEntries() }()
		// rf.printLog()
		rf.mu.Unlock()
		// sleep for a while. Limit 10 heartbeats per second
		time.Sleep(time.Millisecond * 110)
		rf.mu.Lock()
		stillLeader = rf.isLeader
		rf.mu.Unlock()
	}
}

// This function sends append entries to clients and wait for it to be done.
// This requires to be done by a goroutine.
// heartbeats() and updateFollowerLogs() would all dispatch a goroutine to call this function
func (rf *Raft) broadcastEntries() {
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
			// a bool represents weather appendEntries is successful
			isSuccess := false
			// if the response is not successful, then there are two possible causes:
			// 1. This leader is outdated and it should step down immediately (break out of the loop)
			// 2. AppendEntries fails because of log inconsistency, we decrement nextIndex and retry

			// for !isSuccess && !rf.killed() && rf.isLeader {
			for !isSuccess && !rf.killed() {
				// create args structure
				rf.mu.Lock()
				// last log index
				nextIndex := rf.nextIndex[targetID]

				// part3B new
				// If nextIndex <= rf.lastIncludedIndex, then we need to send RPC call
				if nextIndex <= rf.lastIncludedIndex {

					snapArgs := InstallSnapshotArgs{
						rf.currentTerm,
						rf.me,
						rf.lastIncludedIndex,
						rf.lastIncludedTerm,
						rf.snapshotState,
					}

					reply := InstallSnapshotReply{
						-1,
					}
					rf.mu.Unlock()
					// fmt.Println("QQQQQQQQQ", args)
					rpcSuccess := rf.sendInstallSnapshot(targetID, &snapArgs, &reply)
					rf.mu.Lock()
					if !rpcSuccess {
						// fmt.Printf("sendInstallSnapshot RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
						// rf.mu.Unlock()
					} else if reply.Term > rf.currentTerm {
						rf.updateState("follower", reply.Term)
						// rf.mu.Unlock()
					} else {
						// successful installsnapshot, update nextIndex and matchIndex
						// fmt.Printf("successful InstallSnapshot RPC from senderID %v to receiverID %v \n", rf.me, targetID)
						rf.matchIndex[targetID] = rf.lastIncludedIndex
						// fmt.Println("oneoneoneoneoneone")
						rf.nextIndex[targetID] = rf.matchIndex[targetID] + 1
					}
					rf.mu.Unlock()
					wgg.Done()
					return
				}

				nextIndex = rf.nextIndex[targetID]
				prevLogIndex := nextIndex - 1
				// fmt.Println("ppppLog", rf.me, rf.isLeader, rf.nextIndex, prevLogIndex)
				// last log term
				prevLogTerm := -1
				fmt.Println("\\\\\\\\\\\\", rf.me, targetID, rf.isLeader, rf.log, rf.lastApplied, rf.lastIncludedIndex, prevLogIndex)
				if prevLogIndex-rf.lastIncludedIndex == 0 && rf.lastIncludedIndex > 0 {
					// should use last includedIndex and last included term
					prevLogTerm = rf.lastIncludedTerm
				} else if prevLogIndex-rf.lastIncludedIndex > 0 {
					prevLogTerm = rf.log[prevLogIndex-1-rf.lastIncludedIndex].Term
				}

				// log entries to apply
				// rf.printLog()
				logs := []*LogStruct{}

				// when heartbeats, send empty log, else send non-empty logs
				// if !isHeartBeat && len(rf.log) > 0 {
				if len(rf.log) > 0 {
					// fmt.Println("[[[[", rf.me, rf.isLeader, rf.log, targetID, nextIndex, rf.lastIncludedIndex)
					logs = rf.log[nextIndex-1-rf.lastIncludedIndex:]
				}
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
					-10,
					-10,
				}
				// send RPC call
				rpcSuccess := rf.sendAppendEntries(targetID, &args, &reply)

				// Below is broadcasting real log entries:
				//
				isSuccess = reply.Success
				rf.mu.Lock()
				// leader out dated
				if recordTerm != rf.currentTerm {
					rf.mu.Unlock()
					break
				}

				if !rpcSuccess {
					// fmt.Printf("sendAppendEntries RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
					rf.mu.Unlock()
				} else {
					if isSuccess {
						// If successful: update nextIndex and matchIndex for follower
						// since success from follower means all logs have been up to date,
						// we put matchIndex to be the length of current log and nextIndex to be matchIndex + 1
						rf.matchIndex[targetID] = prevLogIndex + len(logs)
						// fmt.Println("twotwotwotwotwotwotwo", rf.me, rf.isLeader, targetID, rf.matchIndex, rf.commitIndex, prevLogIndex, rf.log)
						rf.nextIndex[targetID] = rf.matchIndex[targetID] + 1
						// fmt.Println("iiiiidex", rf.me, rf.isLeader, rf.nextIndex, prevLogIndex, len(logs))
						rf.mu.Unlock()
					} else if reply.Term > rf.currentTerm { // if the follower has a higher term than the leader, the leader should convert to follower
						rf.updateState("follower", reply.Term)
						rf.mu.Unlock()
						// escape the check loop
						break
					} else {
						// special cases for outdated broadcastentries
						if reply.ConflictingTerm == -500 {
							rf.mu.Unlock()
							wgg.Done()
							return
						} else if reply.ConflictingTerm == -1 {
							// if reply.ConflictingTerm == -1 {
							// because of log inconsistency, decrement nextIndex and retry
							// fmt.Println("threethreethreethreethree")
							rf.nextIndex[targetID] = reply.IndexOfConflict
							// fmt.Println("ccccconflict111", rf.me, rf.isLeader, rf.nextIndex)

						} else {
							// advanced skip nextIndex
							// search for last entry of the log that has conflicting term
							target := -1
							for j := len(rf.log); j > 0; j-- {
								if rf.log[j-1].Term == reply.ConflictingTerm {
									target = j + 1
									break
								}
							}
							if target == -1 {
								target = reply.IndexOfConflict
							}
							// fmt.Println("fourfourfourfourfour")
							rf.nextIndex[targetID] = target + rf.lastIncludedIndex
							// fmt.Println("ccccconflict222", rf.me, rf.isLeader, rf.nextIndex)
						}
						rf.mu.Unlock()
					}
				}
				// Apply the last rule for leader here, i.e. a log replicated on a majority of servers but not commited,
				// then this log is actually commited and we need to forward our commitIndex to that point
				rf.mu.Lock()
				// use a map to store all matchIndex. Sort it. If there is a matchIndex = N that satisfy the requirements, set commitIndex = N
				m := make(map[int]int)
				maxMatchIndex := 0
				for _, matchIndex := range rf.matchIndex {
					m[matchIndex]++ // default is 0
					maxMatchIndex = max(maxMatchIndex, matchIndex)
				}

				sum := 0

				prevCommitIndex := rf.commitIndex
				// here mIdx is the N we are looking for
				for mIdx := len(rf.log) + rf.lastIncludedIndex; mIdx >= rf.commitIndex+1 && mIdx >= rf.lastIncludedIndex; mIdx-- {
					if _, ok := m[mIdx]; ok {
						sum += m[mIdx]
					}
					// fmt.Println("-----", rf.me, rf.isLeader, rf.currentTerm, rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, mIdx, rf.matchIndex, len(rf.log), rf.log, rf.log[mIdx-1-rf.lastIncludedIndex].Term)
					if sum > len(rf.peers)/2 && ((mIdx == rf.lastIncludedIndex && rf.lastIncludedTerm == rf.currentTerm) || (mIdx > rf.lastIncludedIndex && rf.log[mIdx-1-rf.lastIncludedIndex].Term == rf.currentTerm)) {
						// fmt.Println("-----", rf.me, rf.isLeader, rf.lastIncludedIndex, mIdx, rf.matchIndex, len(rf.log))
						rf.commitIndex = mIdx
						break
					}
				}
				// fmt.Println("****", rf.me, rf.isLeader, rf.commitIndex, prevCommitIndex)
				if rf.commitIndex > prevCommitIndex {
					rf.applyCond.Signal()
				}
				rf.mu.Unlock()
			}
			wgg.Done()
		}(&wg, rf)
	}
	wg.Wait()
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
	// vote for yourself
	rf.votedFor = rf.me
	rf.persist()

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
				// fmt.Printf("sendRequestVote RPC is not successful from senderID %v to receiverID %v \n", rf.me, targetID)
			} else {
				// successful rpc, process result here.
				// if the terms are not equal, return.
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
						rf.updateState("leader", -99999) // the term parameter is not used for update leader
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
	rf.Persister = persister
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

	// initialize snapshot variable
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	// rf.snapshotState = nil
	rf.snapshotSent = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// read snapshot if exist
	rf.readSnapshot()

	// initialize timer
	rf.resetTimer()

	// start checktimeouts
	go func() { rf.checkTimeouts() }()

	// start applying commited messages
	go func() { rf.applyCommited(applyCh) }()

	return rf
}
