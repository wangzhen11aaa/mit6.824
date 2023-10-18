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
	//	"bytes"

	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// TODO: define and implement the interface to store the state into files.

// State machine used to apply the committed log

//
type StateMachine struct {
	//Figure 13 Install SnapShot RPC, invoked by leader
	// the snapshot replaces all entries up through and including this index.
	lastIncludedIndex int
	// term of lastIncludedIndex
	lastIncludedTerm int
	// Store the latest key/values.
	kv map[interface{}]interface{}
}

/// Log Entry
type LogEntry struct {
	// The term numbers in log entries are used to detect inconsistencies between logs and ensure some of properties in Figure 3. $5.3
	Term int
	//Each log entry has an integer index identifying its position in the log.
	Idx int
	// Each log entry stores a state machine command along with the term
	// number when the entry was received by the leader.$5.3
	Cmd interface{}
}

const (
	Nobody = -1
)

// wait response type
const (
	VoteRequest   = 0
	AppendRequest = 1
)
const (
	NoLeader = -1
)

const (
	InvalidPosition = -1
)

const (
	LostConnection = -1
)

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
)

const (
	NetWorkProblem  int = -2
	NewTerm         int = -1
	MinoritySupport int = 0
	MajoritySupport int = 1
)

type AppendEntriesReplyWithPeerID struct {
	reply  AppendEntriesReply
	server int
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

	// 0: follower, 1: candidate, 2. leader
	role int
	// True if no leader or candidate sends the RPC or heartbeat to Follower.
	reElectionForFollower bool

	// True if the candidate does not win the election or lose election.
	//reElectionForCandidateChan chan bool
	logs []LogEntry

	// rejectServer to store the failed append request.
	rejectServer map[int]interface{}

	latestSuccessfullyAppendTimeStamp int64

	// Minimum election time for raft election is 100milliseconds
	minimumElectionTimeDelta int64
	// current term
	currentTerm int
	// current leader
	//currentLeaderId int

	// candidateId that received vote in current term (or null if none)
	voteFor int

	// channel for successful election vote
	voteSuccessChannel chan int

	// channel for applyCh
	applyCh chan ApplyMsg

	// Index of highest log entry known to be committed.(Initialized to 0, increases monotonically).
	// The leader keeps track of the highest index it knows to be committed, and it includes that index in future AppendEntries RPCs(including heartbeats) so that the other servers eventually find out.
	commitIndex int

	// Index of highest log entry applied to state machine. Initialized to 0, increased monotonically
	lastApplied int

	// The following two need to be Reinitialized after election.

	// For each server, index of the next log entry to send to that server. (Initialized to leader last log index + 1)
	nextIndex []int

	// For each server, index highest log entry known to be replicated on server. (Initialized to 0, increases monotonically)
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// If this node believes he is the leader, then he should prove it.
	//DPrintf("Try get %v 's state \n", rf.me)
	//DPrintf("Try get %v 's state done \n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Once the leader found another term is larger than his currentTerm,
	// leader changed to follower, and the currentTerm get changed to the largest term.
	isleader = (rf.role == Leader)
	term = rf.currentTerm

	DPrintf("%v rf's role: %v , currentTerm: %v, voteFor: %v, logs's length %v\n", rf.me, rf.role, rf.currentTerm, rf.voteFor, len(rf.logs))

	// Your code here (2A).
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// candidate's term
	Term int
	// candidate requesting vote.
	CandidateId int

	// index of candidate's last log entry
	LastLogIndex int
	// term of the candidate's last log entry
	LastLogTerm int
	// Your data here (2A, 2B).
}

type RequestVoteReply struct {
	// currentTerm, for candidate to update itself.
	Term int
	// true means candidate received vote.
	VoteGranted bool
	// Your data here (2A).
}

type RequestAppendEntriesArgs struct {
	// leader's term
	Term int
	// so follower can redirect client
	LeaderId int
	// index of log entry immediately preceding new ones.
	PrevLogIndex int
	// term of prevLogIndex Entry
	PrevLogTerm int
	// log entries to store(empty for heartbeat; may send more than one for efficiency)
	Entries []LogEntry
	// Leader's commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// CurrentTerm, for leader to update itself.
	Term int
	// True if the follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *AppendEntriesReply, result chan AppendEntriesReplyWithPeerID) bool {
	// DPrintf("Goroutine %v Leader %v sendAppendEntries logs :%v to server %v at term %v, time: %v", GetGOId(), rf.me, args.Entries, server, rf.currentTerm, time.Now().UnixMilli())
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("Goroutine %v ,[%v] executed the AppendEntries from leader %v, status: %v \n", GetGOId(), server, args.LeaderId, reply.Success)
		result <- AppendEntriesReplyWithPeerID{reply: *reply, server: server}
		DPrintf("Goroutine %v ,[%v] executed the AppendEntries from leader Ended %v, status: %v \n", GetGOId(), server, args.LeaderId, reply.Success)
	} else {
		DPrintf("Goroutine: %v, [%v] lost from leader %v \n", GetGOId(), server, args.LeaderId)
		reply.Term = LostConnection
		result <- AppendEntriesReplyWithPeerID{reply: *reply, server: server}
		DPrintf("Goroutine: %v, [%v] lost from leader Ended %v \n", GetGOId(), server, args.LeaderId)
	}
	return ok
}

func (rf *Raft) becomeFollower(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v 's [current role: %v to Follower]: currentTerm changed from %v to %v, in AppendEntries for the coming request %v", rf.me, rf.role, rf.currentTerm, args.Term, args.LeaderId)

	rf.currentTerm = args.Term
	rf.role = Follower
	rf.voteFor = args.LeaderId

	rf.reElectionForFollower = false
	rf.latestSuccessfullyAppendTimeStamp = time.Now().UnixMilli()

	reply.Success = rf.consistLogCheck(args)
	if !reply.Success {
		reply.Term = rf.logs[rf.commitIndex-1].Term
	} else {
		reply.Term = rf.currentTerm
	}
}

// If the consistLogCheck failed, return false.(This makes the leader reduce the nextIndex for this server)
func (rf *Raft) consistLogCheck(args *RequestAppendEntriesArgs) bool {
	//Check whether the logs have been appended are the same with the leader. Reduction Property.
	DPrintf("Goroutine: %v, rf %v Begin consistLogCheck at term :%v at time %v, logs: %v, args: %v, rf.commitIndex %v, rf.lastApplied : %v", GetGOId(), rf.me, rf.currentTerm,
		time.Now().UnixMilli(), rf.logs, args, rf.commitIndex, rf.lastApplied)

	if len(rf.logs)-1 >= args.PrevLogIndex {

		// Heartbeat.
		if args.Entries == nil {
			return true
		}

		if args.PrevLogIndex < 0 || args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {

			// Leader's append should be idempotent.
			// Sync logs with leader.
			if rf.commitIndex < args.LeaderCommit {
				DPrintf("Goroutine: %v, rf %v Begin AppendLog at term :%v, rf.commitIndex: %v, at time %v, logs: %v, args: %v", GetGOId(), rf.me, rf.currentTerm, rf.commitIndex,
					time.Now().UnixMilli(), rf.logs, args)
				rf.logs = append(rf.logs, args.Entries[(rf.commitIndex-args.PrevLogIndex-1):]...)
				DPrintf("Goroutine: %v,rf %v End AppendLog at term :%v at time %v, logs: %v", GetGOId(), rf.me, rf.currentTerm,
					time.Now().UnixMilli(), rf.logs)
			}

			// leader's prevLogIndex = leader's commitIndex - 1, Here we try apply args[rf.commitIndex, args.prevLogIndex+1).
			// rf.tryQuickApplyLaggedLogs(args)

			// Index of highest log entry known to be committed.
			DPrintf("Goroutine: %v,rf %v End AppendLog at term update rf.commitIndex from %v to %v at time: %v", GetGOId(), rf.me, rf.commitIndex, len(rf.logs), time.Now().UnixMilli())
			rf.commitIndex = len(rf.logs)

			return true
		} else {
			return false
		}
	}
	// Figure2. Receiver Rules for implementation for 3.
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches previousLog Term.
	return false
}

// applyLogs actually do the copy things, copy things into local state.
func (rf *Raft) applyLogs() {

	//DPrintf("Goroutine: %v, %v rf, rf.lastAppliedIndex : %v, rf.commitIndex :%v, rf's logs: %v, at rf.term :%v", GetGOId(), rf.me, rf.lastApplied, rf.commitIndex, rf.logs, rf.currentTerm)
	for i := rf.lastApplied; i < rf.commitIndex; i++ {

		rf.applyCh <- ApplyMsg{CommandValid: false, Command: rf.logs[i].Cmd, CommandIndex: i}
		DPrintf("Goroutine:%v, %v rf apply Log(cmd: %v), index: %v at term %v, at time %v", GetGOId(), rf.me, rf.logs[i].Cmd, i, rf.currentTerm, time.Now().UnixMilli())
	}

	DPrintf("Goroutine:%v, [Follower]%v rf apply Log End, rf.lastApplied changed from %v to %v at term %v, at time %v", GetGOId(), rf.me, rf.lastApplied, rf.commitIndex, rf.currentTerm, time.Now().UnixMilli())
	rf.lastApplied = rf.commitIndex

}

func (rf *Raft) remainFollower(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("Goroutine %v,%v rf [remainFollower] response to %v at term %v, time :%v \n", GetGOId(), rf.me, args.LeaderId, args.Term, time.Now().UnixMilli())
	rf.reElectionForFollower = false
	rf.voteFor = args.LeaderId
	rf.latestSuccessfullyAppendTimeStamp = time.Now().UnixMilli()

	DPrintf("Goroutine %v,before consistLogCheck: %v ", GetGOId(), rf.logs)
	// try Append logs
	reply.Success = rf.consistLogCheck(args)
	DPrintf("Goroutine %v,after consistLogCheck: %v ", GetGOId(), rf.logs)
	if !reply.Success {
		reply.Term = rf.logs[rf.commitIndex-1].Term
	} else {
		reply.Term = rf.currentTerm
	}
	DPrintf("Goroutine: %v,%v rf [remainFollower] ended response to %v, result: %v at term %v, time :%v \n", GetGOId(), rf.me, args.LeaderId, reply.Success, args.Term, time.Now().UnixMilli())

}

func (rf *Raft) applySyncWithLeader(args *RequestAppendEntriesArgs, nextIndexToApply int) {

	var i int
	for i = rf.lastApplied; i < rf.commitIndex && i < nextIndexToApply; i++ {

		rf.applyCh <- ApplyMsg{CommandValid: false, Command: rf.logs[i].Cmd, CommandIndex: i}
		DPrintf("Goroutine:%v, %v rf apply Log(cmd: %v), index: %v at term %v, at time %v", GetGOId(), rf.me, rf.logs[i].Cmd, i, rf.currentTerm, time.Now().UnixMilli())
	}
	DPrintf("Goroutine:%v, [Follower]%v rf applySyncWithLeader End, rf.lastApplied changed from %v to %v at term %v, at time %v", GetGOId(), rf.me, rf.lastApplied, i, rf.currentTerm, time.Now().UnixMilli())
	rf.lastApplied = i
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Goroutine: %v,Follower %v rf, rf.lastApplied: %v, rf.commitIndex: %v logs: %v, args: %v, term:%v, time:%v", GetGOId(), rf.me, rf.lastApplied, rf.commitIndex, rf.logs, args, rf.currentTerm, time.Now().UnixMilli())

	// Figure 2, Rules for Servers
	// If the commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
	if rf.commitIndex <= args.PrevLogIndex {
		rf.applySyncWithLeader(args, rf.commitIndex)
	} else {
		rf.applySyncWithLeader(args, args.PrevLogIndex+1)
	}

	// AppendEntires rule 1.Reply false if term < current Term
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// AppendEntries RPC for Receiver implementation. Figure 2. %5.3

	// Rules for all servers.
	// 2.If RPC request or response contains term T > currentTerm: set currentTerm T, convert to follower $5.1
	// Stale leader can be here, because it can not launch election.
	if rf.currentTerm < args.Term {
		rf.becomeFollower(args, reply)
		return
	}

	if rf.currentTerm == args.Term {
		// heartbeat
		// Rule for Follower: $5.2
		// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate, convert to candidate.
		if rf.role == Follower {
			rf.remainFollower(args, reply)
			return
		}

		// Rule for Candidate: $5.2
		// If AppendEntries RPC received from new leader: convert to follower.
		if rf.role == Candidate {
			rf.becomeFollower(args, reply)
		}
	}
}

// vote log consist check.
// The candidate should contain the majority items.
func (rf *Raft) consistLogCheckForVote(args *RequestVoteArgs) bool {
	DPrintf("%v rf consistLogCheckForVote, rf.logs: %v,args: %v at term %v, time :%v ", rf.me, rf.logs, args, rf.currentTerm, time.Now().UnixMilli())
	if args.LastLogIndex >= 0 && len(rf.logs)-1 >= args.LastLogIndex {
		if rf.logs[args.LastLogIndex].Term == args.LastLogTerm {
			if len(rf.logs)-1 > args.LastLogIndex {
				return false
			}
		}
	}
	return true
}

// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// RequestVote rule 1.Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.voteFor = Nobody
		DPrintf("%v rf rejected peer %v, reason: %v's term: %v > %v's term: %v", rf.me, args.CandidateId, rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// For all servers.
	// 2.If RPC request or response contains term T > currentTerm: set currentTerm T, convert to follower $5.1
	// servers disregard RequestVote
	//RPCs when they believe a current leader exists. Specifically, if a server receives a RequestVote RPC within
	//the minimum election timeout of hearing from a cur-
	//rent leader, it does not update its term or grant its vote.

	if rf.currentTerm < args.Term {

		DPrintf("Goroutine: %v, %v rf [current role: %v to Follower]: currentTerm changed from %v to %v, in RequestVote for the coming request rf %v, args: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, time.Now().UnixMilli())
		rf.currentTerm = args.Term

		rf.role = Follower
		if rf.consistLogCheckForVote(args) {
			DPrintf("Goroutine: %v, %v rf [current role: %v to Follower]: currentTerm changed from %v to %v, in RequestVote for the coming request rf %v, args: %v, consistLogCheckForVote Result: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, true, time.Now().UnixMilli())

			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.reElectionForFollower = false
		} else {
			DPrintf("Goroutine: %v, %v rf [current role: %v to Follower]: currentTerm changed from %v to %v, in RequestVote for the coming request rf %v, args: %v, consistLogCheckForVote Result: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, false, time.Now().UnixMilli())
			reply.VoteGranted = false
			rf.voteFor = Nobody
		}
		// If raft node follows leader, then this request suppress follower's election.
		return
	}

	// RequestVote RPC rule for receiver: 2.0 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	reply.Term = rf.currentTerm
	if rf.voteFor == Nobody || rf.voteFor == args.CandidateId {
		if args.LastLogIndex >= 0 && len(rf.logs)-1 <= args.LastLogIndex || rf.logs[args.LastLogIndex].Term <= args.LastLogTerm {
			//Specifically, if a server receives a RequestVote RPC within
			// the minimum election timeout of hearing from a cur
			// rent leader, it does not update its term or grant its vote.
			if rf.voteFor == Nobody {
				rf.voteFor = args.CandidateId
			}
			rf.reElectionForFollower = false
			rf.role = Follower
			reply.VoteGranted = true
			return
		}
	}
	reply.VoteGranted = false

	// Current Term is less than candidate's. Figure4. Discovers server with high term.

	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteChannel chan RequestVoteReply) bool {
	//DPrintf("%v request vote for [%v] \n", args.CandidateId, server)
	// This is synchronized call.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			DPrintf("Goroutine: %v, [%v]rf got vote support from %v at term %v \n", GetGOId(), args.CandidateId, server, args.Term)
		} else {
			DPrintf("Goroutine %v, [%v] rf got vote reject from %v at term %v \n", GetGOId(), args.CandidateId, server, args.Term)
		}
		voteChannel <- *reply
	} else {
		// If the can not reach the server
		//fmt.Printf("[%v] lost from %v \n", server, args.CandidateId)
		reply.Term = LostConnection
		voteChannel <- *reply
	}
	return ok
}

// Client Interface.
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
	term, isLeader = rf.GetState()

	if isLeader {
		DPrintf("Method Start() Append cmd %v to Leader,at time %v", command, time.Now().UnixMilli())
		index = rf.LeaderAppendEntries(command)
	}
	return index, term, isLeader
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

func (rf *Raft) initElectionRequest(rq *RequestVoteArgs) {
	// Initially Raft's term is 0.
	rq.Term = rf.currentTerm
	rq.CandidateId = rf.me

	// Index of the last log index.
	rq.LastLogIndex = len(rf.logs) - 1
	if rq.LastLogIndex == -1 {
		rq.LastLogTerm = 0
	} else {
		rq.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) prepareRequest(currentTerm *int, rq *RequestVoteArgs) {
	rf.currentTerm += 1

	// If the follower timeout, then we need not care about who is the leader.
	rf.voteFor = rf.me
	rf.latestSuccessfullyAppendTimeStamp = -1

	DPrintf("Goroutine: %v, [%v] rf voted himself at %v, time: %v\n", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
	// Release the lock, then other goroutines can modify the critical data.
	rf.initElectionRequest(rq)

	*currentTerm = rf.currentTerm

}

func (rf *Raft) launchElection() {
	// We probably should store some value on the stack, in case other goroutines change the value to stop this election, for example it has voted for other peer, during this election.
	rf.mu.Lock()

	// Figure 2, Rules for Servers
	// If the commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
	if rf.lastApplied < rf.commitIndex {
		rf.applyLogs()
	}

	// Double-check
	if rf.role != Candidate {
		defer rf.mu.Unlock()
		return
	}

	var currentTerm int
	rq := RequestVoteArgs{}
	// Each reply is different from each other.
	replies := make([]RequestVoteReply, len(rf.peers))
	// Prepare the request
	rf.prepareRequest(&currentTerm, &rq)

	//Clear previous stale vote in the channel
	// cnt := len(rf.voteChannel)
	// for i := 0; i < cnt; i++ {
	// 	<-rf.voteChannel
	// }

	rf.mu.Unlock()
	voteChannel := make(chan RequestVoteReply, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf("Goroutine: %v,%v rf send to %v, rq:{Term: %v} \n", GetGOId(), rq.CandidateId, i, rq.Term)
		// Send request asynchronously.
		go rf.sendRequestVote(i, &rq, &replies[i], voteChannel)
	}

	lostConnectionCnt := 0
	voted := 1
	// Store the largest updatedTerm
	updatedTerm := -1
	// channel for vote
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-voteChannel

		rf.mu.Lock()
		// Network problem
		if reply.Term == LostConnection {
			lostConnectionCnt++

			if lostConnectionCnt >= len(rf.peers)/2+1 {
				defer rf.mu.Unlock()
				rf.processNetworkFailure(VoteRequest, false)
				return
			}
		} else {
			// No Network Problem.
			if reply.VoteGranted {

				// Term changed or have voted for other peer during the waiting.
				if rf.voteFor != rf.me && rf.currentTerm > currentTerm {
					rf.processReElectionCondition(currentTerm, VoteRequest)
					return
				}

				// Count the successful votes
				voted++
				if voted >= len(rf.peers)/2+1 {
					rf.processMajoritySuccessVoteRequest()
					return
				}

			} else {
				if updatedTerm < reply.Term {
					updatedTerm = reply.Term
				}
			}
		}
		rf.mu.Unlock()
	}

	rf.processFailedRequest(VoteRequest, updatedTerm)
}

// Process the failed request condition.

func (rf *Raft) processFailedRequest(typ int, updatedTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Follower
	if typ == VoteRequest {
		if rf.role == Candidate {
			// Rules for Candidate: $5.2
			// If AppendEntries RPC received from new leader: convert to follower.

			if updatedTerm > rf.currentTerm {
				DPrintf("[tryBecomeFollower supported by Minority] %v 's [current role: %v term : %v to Follower]: Term replied : %v at time: %v", rf.me, rf.role, rf.currentTerm, updatedTerm, time.Now().UnixMilli())
			}
			rf.voteSuccessChannel <- MinoritySupport
			rf.reElectionForFollower = true
		}
	}
}

// Update all the followers' nextIndex
func (rf *Raft) updateAllFollowersNextIndex(rejectServer map[int]interface{}, approveServer map[int]interface{}, nextIndexToCommit int) {
	// Update the corresponded next index of the server, if this server has successfully replicated the logs.
	/* For example:
	Two cases:
	Leader has no logs:
	logs (Before Leader appending log):
		[], rf.nextIndex = 0

	logs (After Leader appending log):
		[[0]], len(rf.logs) = 1, nextIndexToCommit = 1

	Leader has logs:
	logs (Before Leader appending log):
	  [0], rf.nextIndex = 1

	logs (After Leader appending log):
	  [[0] [1]], len(rf.logs) = 2, nextIndexToCommit = 2
	  [1] will send to the follower to append.

	*/
	for i := 0; i < len(rf.peers); i++ {
		// Update the follower's next commit index.
		if i == rf.me {
			continue
		}
		if _, found := rejectServer[i]; found {
			DPrintf("Goroutine:%v, %v rf appendEntries returned false, does not update nextIndex", GetGOId(), i)
			continue
		}
		if _, found := approveServer[i]; found {
			DPrintf("Goroutine:%v, %v rf appendEntries has updated nextIndex, nextIndex[%v]: %v", GetGOId(), i, i, rf.nextIndex[i])
			continue
		}
		rf.nextIndex[i] = nextIndexToCommit
		// This structure contains the index that should be committed.
		rf.matchIndex[i] = nextIndexToCommit
	}
}

// Process the success append entries request condition.
func (rf *Raft) processMajoritySuccessAppendRequest(leaderCommitStartIndex int, nextIndexToCommit int, rejectServer map[int]interface{}, approveServer map[int]interface{}) {

	// Update the leader's commitIndex, this means that those logs have been committed on the majority of cluster server.
	rf.updateAllFollowersNextIndex(rejectServer, approveServer, nextIndexToCommit)
	//update rf.commitIndex
	rf.updateLeaderCommitIndex()

	// If the majority of servers have been replicated this log,It is safe now to apply this log.
	/* For example:
	logs (Before Leader appending log):
	  [0], rf.nextIndex = 1

	logs (After Leader appending log):
	  [[0] [1]], len(rf.logs) = 2, nextIndexToCommit = 2
	  [1] will send to the follower to append.
	*/
	// When the majority appended successfully, we update local commitIndex for the next logs to append.
	// The leader can apply these logs safely. logs[leaderCommitStartIndex, localCommitStartIndex)

	DPrintf("Goroutine: %v, %v rf[Leader] begin apply logs: %v at term %v, leaderCommitStartIndex: %v, at time %v", GetGOId(), rf.me, rf.logs, rf.currentTerm, leaderCommitStartIndex, time.Now().UnixMilli())
	for i := rf.lastApplied; i < rf.commitIndex; i += 1 {

		rf.applyCh <- ApplyMsg{CommandValid: false, Command: rf.logs[i].Cmd, CommandIndex: i}
		DPrintf("Goroutine:%v, %v rf apply Log(cmd: %v), index: %v at term %v, at time %v", GetGOId(), rf.me, rf.logs[i].Cmd, i, rf.currentTerm, time.Now().UnixMilli())
	}

	DPrintf("Goroutine:%v,[Leader] %v rf apply Log End, rf.lastApplied changed from %v to %v at term %v, at time %v", GetGOId(), rf.me, rf.lastApplied, rf.commitIndex, rf.currentTerm, time.Now().UnixMilli())

	rf.lastApplied = rf.commitIndex
}

// Update leader commit index.
func (rf *Raft) updateLeaderCommitIndex() {
	// Find the N, that N > commitIndex, a majority of matchIndex[i] >= N and log[N].term == currentTerm: set commitIndex = N.
	cnt := make(map[int]int)
	for server, k := range rf.matchIndex {
		if server != rf.me {
			_, found := cnt[k]
			if found {
				cnt[k]++
			} else {
				cnt[k] = 1
			}
		}
	}
	DPrintf("Goroutine: %v, %v rf Commit index in updateLeaderCommitIndex cnt: %v", GetGOId(), rf.me, cnt)

	// Select the N in the cnt map.
	var n int
	n = -1
	for k, v := range cnt {
		DPrintf("Goroutine :%v, k: %v, v:%v \n", GetGOId(), k, v)
		if v >= (len(rf.peers) / 2) {
			n = k
		}
	}

	DPrintf("Goroutine: %v, %v rf Commit index in updateLeaderCommitIndex rf.commitIndex changed from %v to %v, rf.matchIndex: %v", GetGOId(), rf.me, rf.commitIndex, n, rf.matchIndex)

	rf.commitIndex = n
}

// Process the success vote request condition.
func (rf *Raft) processMajoritySuccessVoteRequest() {
	defer rf.mu.Unlock()
	rf.voteSuccessChannel <- MajoritySupport
}

// Process the network failure condition
func (rf *Raft) processNetworkFailure(typ int, result bool) {
	// rf.mu.Lock()
	DPrintf("Goroutine: %v, %v rf Network problem at term %v", GetGOId(), rf.me, rf.currentTerm)
	if typ == VoteRequest {
		rf.voteSuccessChannel <- NetWorkProblem
	}
}

func (rf *Raft) processReElectionCondition(currentTerm int, typ int) {
	defer rf.mu.Unlock()
	if rf.currentTerm > currentTerm && rf.voteFor != rf.me {
		DPrintf("Goroutine: %v, [AppendEntries encounter new Term] %v rf Term changed from %v to %v, his role: %v, stop election its voteFor : %v, time: %v\n", GetGOId(), rf.me, currentTerm, rf.currentTerm, rf.role, rf.voteFor, time.Now().UnixMilli())
	}
	// rf.role = Follower
	// rf.reElectionForFollower = false
	if typ == VoteRequest {
		rf.voteSuccessChannel <- NewTerm
	} else {
		rf.role = Follower
		//rf.LeaderAppendFailedChannel <- false
	}
}

func (rf *Raft) prepareAppendRequest(rq *RequestAppendEntriesArgs) {
	rq.Term = rf.currentTerm
	rq.LeaderId = rf.me
}

func (rf *Raft) leaderAppendLogLocally(command interface{}) {
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Idx: len(rf.logs), Cmd: command})

	DPrintf("Goroutine:%v, Leader make log added into the %v at term %v", GetGOId(), rf.me, rf.currentTerm)
}

func GetGOId() int64 {
	var (
		buf [64]byte
		n   = runtime.Stack(buf[:], false)
		stk = strings.TrimPrefix(string(buf[:n]), "goroutine")
	)

	idField := strings.Fields(stk)[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Errorf("can not get goroutine id: %v", err))
	}

	return int64(id)
}

func (rf *Raft) updateFollowerNextIndex(server int, followerLogTerm int) {
	DPrintf("Goroutine: %v, %v rf, term: %v, logs: %v, followerLogTerm: %v", GetGOId(), rf.me, rf.currentTerm, rf.logs, followerLogTerm)
	right := len(rf.logs)
	left := 0
	for left < right {
		mid := (left + right) / 2
		if rf.logs[mid].Term >= followerLogTerm {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	DPrintf("Goroutine: %v, result: %v, target index: %v, logs: %v, targetTerm: %v", GetGOId(), right, right+1, rf.logs, followerLogTerm)
	rf.nextIndex[server] = right + 1
}

// Update nextIndex and matchIndex for this server.
func (rf *Raft) updateNextIndexAndMatchIndex(server int, nextIndexToCommit int) {

	rf.nextIndex[server] = nextIndexToCommit
	rf.matchIndex[server] = nextIndexToCommit

}

func (rf *Raft) LeaderAppendEntries(command interface{}) int {
	rf.mu.Lock()

	if rf.role != Leader {
		defer rf.mu.Unlock()
		return InvalidPosition
	}

	// Figure 2, Rules for Servers
	//If the commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
	if rf.lastApplied < rf.commitIndex {
		rf.applyLogs()
	}

	currentTerm := rf.currentTerm

	rq := RequestAppendEntriesArgs{}
	rf.prepareAppendRequest(&rq)

	leaderCommitStartIndex := len(rf.logs)

	if command != nil {
		// First append local log
		// If command received from client: append entry to local log. $5.2 Rules for servers: Leaders part
		rf.leaderAppendLogLocally(command)
	}

	// Figure 2 AppendEntries RPC
	// Update rq.LeaderCommit to leader's commitIndex now.

	//rq.LeaderCommit = rf.commitIndex
	// Store current nextIndex for leader to update,when the majority appended these logs.
	nextIndexToCommit := len(rf.logs)
	rq.LeaderCommit = nextIndexToCommit
	resultChan := make(chan AppendEntriesReplyWithPeerID)
	replies := make([]AppendEntriesReply, len(rf.peers))
	// AppendEntries to other peers.
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// $Figure 2 AppendEntries RPC: may send entries to more than one for efficiency. And Figure 3, Log Matching property, the log's position must be identical to all servers.

			/* For example:
			logs (Before Leader appending log):

			  [0], rf.nextIndex[2] = 1, for follower = 2, means the follower has committed logs [0, rf.nextIndex[2]], in this case.

			logs (After Leader appending log):
			  [[0]       			[1]], len(rf.logs) = 2
			    |   	 		 	 |
			rq.PreviousIndex=0	 rq.LeaderCommit = rf.nextIndex[follower] = len(rf.logs) = 2.
			rq.PreviousIndex is the leader's latest index, at which the log has been committed. [rq.PreviousIndex+1, rq.LeaderCommit) are logs to committed for these follower, at the first time.
			  [1] will send to the follower to append.
			*/
			DPrintf("Goroutine: %v, in LeaderAppendEntries rf.nextIndex[%v] :%v, nextIndexToCommit: %v, at term %v time: %v", GetGOId(), i, rf.nextIndex[i], nextIndexToCommit, rf.currentTerm, time.Now().UnixMilli())
			rq.PrevLogIndex = rf.nextIndex[i] - 1

			rq.PrevLogTerm = -1
			if rq.PrevLogIndex >= 0 {
				rq.PrevLogTerm = rf.logs[rq.PrevLogIndex].Term
			}
			for idx := rf.nextIndex[i]; idx < nextIndexToCommit; idx++ {
				rq.Entries = append(rq.Entries, rf.logs[idx])
			}
			rqCopy := rq
			// rf.mu.UnLock() can not after sendAppendEntries, because deadlock.
			DPrintf("Goroutine: %v, rf [Leader] %v, logs: %v, sends AppendEntry rq %v to follower %v, at term %v \n, time :%v", GetGOId(), rf.me, rf.logs, rqCopy, i, rf.currentTerm, time.Now().UnixMilli())
			go rf.sendAppendEntries(i, &rqCopy, &replies[i], resultChan)
		}
		rq.Entries = nil
	}

	rf.mu.Unlock()
	LostConnectionCnt := 0
	successCnt := 1
	updatedTerm := 0
	rejectServer := make(map[int]interface{})
	approveServer := make(map[int]interface{})

	for i := 0; i < len(rf.peers)-1; i++ {

		resp := <-resultChan
		rf.mu.Lock()

		// if resp.reply.Term > rf.currentTerm {
		// 	DPrintf("Goroutine: %v, %v rf received response from rf:%v, resp: %v, time :%v, Leader->Follower", GetGOId(), rf.me, resp.server, resp, time.Now().UnixMilli())
		// 	defer rf.mu.Unlock()
		// 	rf.role = Follower
		// 	rf.voteFor = Nobody
		// 	rf.currentTerm = resp.reply.Term
		// 	return NoLeader
		// }

		if resp.reply.Term == LostConnection {
			LostConnectionCnt++

			if LostConnectionCnt >= len(rf.peers)/2+1 {
				defer rf.mu.Unlock()
				rf.processNetworkFailure(AppendRequest, true)

				DPrintf("Goroutine: %v, %v rf , currentTerm: %v, updatedTerm: %v", GetGOId(), rf.me, rf.currentTerm, updatedTerm)
				if updatedTerm > rf.currentTerm {
					rf.role = Follower
					rf.voteFor = Nobody
					rf.currentTerm = updatedTerm
				}

				return leaderCommitStartIndex
			}
		} else {
			if resp.reply.Success {

				// New term, or new leader elected during waiting the response.
				if rf.voteFor != rf.me || rf.currentTerm > currentTerm {
					rf.processReElectionCondition(currentTerm, AppendRequest)
					return InvalidPosition
				}

				successCnt++

				//delete(rejectServer, resp.server)
				approveServer[resp.server] = nil

				rf.updateNextIndexAndMatchIndex(resp.server, nextIndexToCommit)

				//rf.updateFollowerNextIndex(resp.server, nextIndexToCommit)
				// The logs has committed on the majority servers.
				if successCnt >= len(rf.peers)/2+1 {
					defer rf.mu.Unlock()

					rf.processMajoritySuccessAppendRequest(leaderCommitStartIndex, nextIndexToCommit, rejectServer, approveServer)

					if updatedTerm > rf.currentTerm {
						rf.role = Follower
						rf.voteFor = Nobody
						rf.currentTerm = updatedTerm
						return leaderCommitStartIndex
					}

					return leaderCommitStartIndex
				}
			} else {
				// Record the most updated Term from the peer.
				if rf.currentTerm < resp.reply.Term {
					if updatedTerm < resp.reply.Term {
						updatedTerm = resp.reply.Term
					}
					rf.mu.Unlock()
					continue
				}
				//Figure2,Rules for server, $5.3 If the AppendEntries fails because of log inconsistency, decrement nextIndex and retry. (Wait next heartbeat)
				// TODO can optimize this.

				// If append log entries, update the corresponded index
				DPrintf("Goroutine: %v, %v rf received %v rf rejected false to this appendEntries, rf.nextIndex from %v to %v, responded term:%v", GetGOId(), rf.me, resp.server, rf.nextIndex[resp.server], rf.nextIndex[resp.server]-1, resp.reply.Term)

				// Update the rf.nextIndex to the first op command of replied term.
				rf.updateFollowerNextIndex(resp.server, resp.reply.Term)
				rejectServer[resp.server] = nil
			}
		}

		rf.mu.Unlock()
	}

	rf.processFailedRequest(AppendRequest, updatedTerm)
	return InvalidPosition
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	// We recommend using a
	// conservative election timeout such as 150–300ms; such time-
	// outs are unlikely to cause unnecessary leader changes and
	// will still provide good availability.

	electionTimeoutV := 150
	heartbeatTimeoutV := 10

	for !rf.killed() {
		r1 := rand.New(rand.NewSource(time.Now().UnixMilli()))
		rf.mu.Lock()
		role := rf.role
		DPrintf("Loop Check, Goroutine: %v,%v rf current role : %v at term %v, logs' length: %v, logs: %v, time : %v", GetGOId(), rf.me, role, rf.currentTerm, len(rf.logs), rf.logs, time.Now().UnixMilli())
		switch role {
		case Leader:
			rf.mu.Unlock()
			<-time.After(time.Duration(heartbeatTimeoutV)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(10)+int32(rf.me)*5))
			// nil means heartbeat
			go rf.LeaderAppendEntries(nil)

		case Follower:
			rf.mu.Unlock()

			<-time.After(time.Duration(time.Duration(electionTimeoutV)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(50)+int32(rf.me)*5)))

			rf.mu.Lock()
			if rf.reElectionForFollower {
				DPrintf("Goroutine: %v, %v rf changed to Candidate now ,term %v, at time %v", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
				rf.role = Candidate
			} else {
				rf.reElectionForFollower = true
			}
			rf.mu.Unlock()

		case Candidate:
			rf.mu.Unlock()

			go rf.launchElection()

			voteSuccess := <-rf.voteSuccessChannel
			rf.mu.Lock()
			if voteSuccess == MajoritySupport {
				rf.role = Leader
				DPrintf("Goroutine: %v, %v rf Leader now at term %v", GetGOId(), rf.me, rf.currentTerm)
				rf.initializeIndexForFollowers()

				rf.mu.Unlock()

				// First insert a on-op log into leader.
				go rf.LeaderAppendEntries("no-op")
				time.Sleep(time.Millisecond * 40)
			} else if voteSuccess == NetWorkProblem {
				time.Sleep(time.Millisecond * 150)
				rf.mu.Unlock()
			} else if voteSuccess == NewTerm {
				//rf.role = Follower
				rf.mu.Unlock()
			} else { // Minority Support
				//rf.role = Follower
				time.Sleep(time.Millisecond * 150)
				rf.mu.Unlock()
			}
		}

	}

	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

}

/// Initialize the channels used by raft.
func (rf *Raft) initializeChannels() {
	rf.voteSuccessChannel = make(chan int)
}

/// Only for Leaders
// Figure 2 on State volatile state on leaders,
func (rf *Raft) initializeIndexForFollowers() {
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			// The next position append to follower.
			rf.nextIndex[i] = len(rf.logs)
			// initialized to 0,
			rf.matchIndex[i] = 0
		}
	}
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
	// each raft service starts as a follower.
	rf.role = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// All elections are launched by the ticker.
	//rf.reElectionForFollower = true

	rf.minimumElectionTimeDelta = 150

	rf.latestSuccessfullyAppendTimeStamp = 0

	rf.rejectServer = make(map[int]interface{})
	rf.initializeChannels()

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
