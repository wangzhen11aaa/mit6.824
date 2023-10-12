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

	"math/rand"
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

	latestSuccessfullyAppendTimeStamp int64

	// Minimum election time for raft election is 100milliseconds
	minimumElectionTimeDelta int64
	// current term
	currentTerm int
	// current leader
	//currentLeaderId int

	// candidateId that received vote in current term (or null if none)
	voteFor int
	// When timeout, goroutine should check whether doest the timeout bool is set to false during the waiting.
	timeoutChannel chan int

	// channel for vote
	voteChannel chan RequestVoteReply

	// channel for get heartbeat
	getHeartbeatChannel chan bool

	// channel for successful election vote
	votedSuccessChannel chan bool

	// channel for apply
	appendSuccessChannel chan bool

	// channel for apply timeout
	applyTimeoutChannel chan bool

	// channel for applyCh
	applyCh chan ApplyMsg

	// channel for heartbeat
	AppendEntriesReplyWithPeerIDChannel chan AppendEntriesReplyWithPeerID

	// quorum
	quorum bool

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

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		//DPrintf("[%v] executed the AppendEntries from leader %v, status: %v \n", server, args.LeaderId, reply.Success)
		rf.AppendEntriesReplyWithPeerIDChannel <- AppendEntriesReplyWithPeerID{reply: *reply, server: server}
	} else {
		//DPrintf("[%v] lost from leader %v \n", server, args.LeaderId)
		reply.Term = LostConnection
		rf.AppendEntriesReplyWithPeerIDChannel <- AppendEntriesReplyWithPeerID{reply: *reply, server: server}
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
	reply.Term = rf.currentTerm
}

func (rf *Raft) consistLogCheck(args *RequestAppendEntriesArgs) bool {

	if args.Entries == nil {
		return true
	}

	// If the log has been committed before, return true.
	if len(rf.logs) != 0 && (len(rf.logs))-1 == args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term == args.Term {

		return true
	}

	// Figure2. Receiver Rules for implementation for 3.
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches previousLog Term.
	// Reduction.
	DPrintf("%v rf's logs %v at term :%v", rf.me, rf.logs, rf.currentTerm)

	if len(rf.logs) != 0 && args.PrevLogIndex >= 0 && (len(rf.logs)-1 < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.Term) {
		return false
	}

	DPrintf("rf %v Begin AppendLog at term :%v at time %v", rf.me, rf.currentTerm, time.Now().UnixMilli())
	// Figure2. If an existing entry conflicts with a new one(same index with but different terms), deleting the existing entry and all follows it.
	if len(rf.logs) != 0 {
		rf.logs = rf.logs[0 : args.PrevLogIndex+1]
	}
	rf.logs = append(rf.logs, args.Entries...)

	DPrintf("Now %v rf's logs %v at term :%v, time : %v", rf.me, rf.logs, rf.currentTerm, time.Now().UnixMilli())

	// If local commitIndex is lagged behind or it's commitIndex is stale.
	if args.LeaderCommit > rf.commitIndex || rf.commitIndex < rf.nextCommitIndex() {
		rf.applyLogs(args)
	}
	return true
}

func (rf *Raft) nextCommitIndex() int {
	return len(rf.logs)
}

func (rf *Raft) applyLogs(args *RequestAppendEntriesArgs) {
	var commitIndex int
	for commitIndex = rf.commitIndex; commitIndex < args.LeaderCommit && commitIndex < rf.nextCommitIndex(); commitIndex++ {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[commitIndex].Cmd, CommandIndex: commitIndex}
	}
	rf.commitIndex = commitIndex
}

func (rf *Raft) remainFollower(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("%v rf [remainFollower] response to %v at term %v, time :%v \n", rf.me, args.LeaderId, args.Term, time.Now().UnixMilli())
	rf.reElectionForFollower = false
	rf.voteFor = args.LeaderId
	rf.latestSuccessfullyAppendTimeStamp = time.Now().UnixMilli()

	// try Append logs
	reply.Success = rf.consistLogCheck(args)
	reply.Term = args.Term
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		}

		// Rule for Candidate: $5.2
		// If AppendEntries RPC received from new leader: convert to follower.
		if rf.role == Candidate {
			rf.becomeFollower(args, reply)
		}
	}
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

		DPrintf("%v 's [current role: %v to Follower]: currentTerm changed from %v to %v, in RequestVote for the coming request rf %v at time: %v", rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, time.Now().UnixMilli())
		rf.currentTerm = args.Term

		rf.role = Follower
		rf.voteFor = args.CandidateId
		// If raft node follows leader, then this request suppress follower's election.
		rf.reElectionForFollower = false
		reply.VoteGranted = true

		return
	}

	// RequestVote RPC rule for receiver: 2.0 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	reply.Term = rf.currentTerm
	if rf.voteFor == Nobody || rf.voteFor == args.CandidateId {
		if len(rf.logs)-1 <= args.LastLogIndex || rf.logs[args.LastLogIndex].Term <= args.LastLogTerm {
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
	return

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("%v request vote for [%v] \n", args.CandidateId, server)
	// This is synchronized call.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted == true {
			DPrintf("[%v]rf got vote from %v at term %v \n", args.CandidateId, server, args.Term)
		} else {
			DPrintf("[%v] rf got reject from %v at term %v \n", args.CandidateId, server, args.Term)
		}
		rf.voteChannel <- *reply
	} else {
		// If the can not reach the server
		//fmt.Printf("[%v] lost from %v \n", server, args.CandidateId)
		reply.Term = LostConnection
		rf.voteChannel <- *reply
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

	if isLeader == true {
		rf.LeaderAppendEntries(command)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm += 1

	// If the follower timeout, then we need not care about who is the leader.
	rf.voteFor = rf.me
	rf.latestSuccessfullyAppendTimeStamp = -1

	DPrintf("[%v] rf voted himself at %v, time: %v\n", rf.me, rf.currentTerm, time.Now().UnixMilli())
	// Release the lock, then other goroutines can modify the critical data.
	rf.initElectionRequest(rq)

	*currentTerm = rf.currentTerm

}

func (rf *Raft) launchElection() {
	// We probably should store some value on the stack, in case other goroutines change the value to stop this election, for example it has voted for other peer, during this election.

	var currentTerm int
	rq := RequestVoteArgs{}
	// Each reply is different from each other.
	replies := make([]RequestVoteReply, len(rf.peers))
	// Prepare the request
	rf.prepareRequest(&currentTerm, &rq)

	//Clear previous stale vote in the channel
	cnt := len(rf.voteChannel)
	for i := 0; i < cnt; i++ {
		<-rf.voteChannel
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf("%v rf send to %v, rq:{Term: %v} \n", rq.CandidateId, i, rq.Term)
		// Send request asynchronously.
		go rf.sendRequestVote(i, &rq, &replies[i])
	}

	lostConnectionCnt := 0
	voted := 1
	// Store the largest updatedTerm
	updatedTerm := -1

	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-rf.voteChannel

		rf.mu.Lock()
		// Network problem
		if reply.Term == LostConnection {
			lostConnectionCnt++

			if lostConnectionCnt >= len(rf.peers)/2+1 {
				rf.processNetworkFailure(VoteRequest, false)
				return
			}
		} else {
			// No Network Problem.
			if reply.VoteGranted == true {

				// Term changed or have voted for other peer during the waiting.
				if rf.voteFor != rf.me || rf.currentTerm > currentTerm {
					rf.processReElectionCondition(currentTerm, VoteRequest)
					return
				}

				// Count the successful votes
				voted++
				if voted >= len(rf.peers)/2+1 {
					rf.processSuccessRequest(VoteRequest, 0)
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

	return

}

// Process the failed request condition.

func (rf *Raft) processFailedRequest(typ int, updatedTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if typ == VoteRequest {
		if rf.role == Candidate {
			// Rules for Candidate: $5.2
			// If AppendEntries RPC received from new leader: convert to follower.

			if updatedTerm > rf.currentTerm {
				DPrintf("[tryBecomeFollower supported by Minority] %v 's [current role: %v term : %v to Follower]: Term replied : %v at time: %v", rf.me, rf.role, rf.currentTerm, updatedTerm, time.Now().UnixMilli())
				rf.currentTerm = updatedTerm
			}

			rf.votedSuccessChannel <- false
			rf.reElectionForFollower = false
		}
	} else {
		rf.appendSuccessChannel <- false
	}

}

// Process the success request condition.
func (rf *Raft) processSuccessRequest(typ int, currentLastLogIndex int) {
	defer rf.mu.Unlock()
	if typ == VoteRequest {
		DPrintf("%v rf return votedSuccessChannel", rf.me)
		rf.votedSuccessChannel <- true
	} else {
		// If the majority of servers have been replicated this log,It is safe now to apply this log.
		rf.commitIndex = currentLastLogIndex
		rf.appendSuccessChannel <- true
	}
}

// Process the network failure condition
func (rf *Raft) processNetworkFailure(typ int, result bool) {
	defer rf.mu.Unlock()
	DPrintf("%v rf Network problem at term %v", rf.me, rf.currentTerm)
	if typ == VoteRequest {
		rf.votedSuccessChannel <- result
	} else {
		rf.appendSuccessChannel <- result
	}
}

func (rf *Raft) processReElectionCondition(currentTerm int, typ int) {
	defer rf.mu.Unlock()
	if rf.currentTerm > currentTerm {
		DPrintf("[tryBecomeLeader Once supported by Majority] %v rf Term changed from %v to %v, his role: %v, stop election its voteFor : %v, time: %v\n", rf.me, currentTerm, rf.currentTerm, rf.role, rf.voteFor, time.Now().UnixMilli())
	}
	rf.voteFor = Nobody
	// rf.role = Follower
	// rf.reElectionForFollower = false
	if typ == VoteRequest {
		rf.votedSuccessChannel <- false
	} else {
		rf.appendSuccessChannel <- false
	}
}

func (rf *Raft) prepareAppendRequest(rq *RequestAppendEntriesArgs) {
	rq.Term = rf.currentTerm
	rq.LeaderId = rf.me
	// Figure2 AppendEntries RPC
	// index of log entry immediately preceding new ones.
	rq.PrevLogIndex = rf.commitIndex - 1

}

func (rf *Raft) leaderAppendLogLocally(command interface{}) {
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Idx: len(rf.logs), Cmd: command})
	DPrintf("log added into the %v at term %v", rf.me, rf.currentTerm)
	// Update the commitIdex, which is the index/position of log *to be committed* next time.[0, len(rf.logs)-1] are logs which have been committed.
	rf.commitIndex = len(rf.logs)
}

func (rf *Raft) LeaderAppendEntries(command interface{}) {
	rf.mu.Lock()

	if rf.role != Leader {
		defer rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm

	for i := 0; i < len(rf.AppendEntriesReplyWithPeerIDChannel); i++ {
		<-rf.AppendEntriesReplyWithPeerIDChannel
	}

	rq := RequestAppendEntriesArgs{}
	rf.prepareAppendRequest(&rq)

	if command != nil {
		// First append local log
		// If command received from client: append entry to local log. $5.2 Rules for servers: Leaders part
		rf.leaderAppendLogLocally(command)
	}

	// Figure 2 AppendEntries RPC
	// Update rq.LeaderCommit to leader's commitIndex now.
	rq.LeaderCommit = rf.commitIndex

	// If committed successfully, update it
	currentLastLogIndex := len(rf.logs)
	rf.mu.Unlock()

	replies := make([]AppendEntriesReply, len(rf.peers))
	// AppendEntries to other peers.
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// $Figure 2 AppendEntries RPC: may send entries to more than one for efficiency. And Figure 3, Log Matching property, the log's position must be identical to all servers.
			for i := rf.nextIndex[i]; i < currentLastLogIndex; i++ {
				rq.Entries = append(rq.Entries, rf.logs[i])
			}
			rqCopy := rq
			go rf.sendAppendEntries(i, &rqCopy, &replies[i])
		}
		rq.Entries = nil
	}

	LostConnectionCnt := 0
	successCnt := 1
	updatedTerm := -1
	for i := 0; i < len(rf.peers)-1; i++ {

		resp := <-rf.AppendEntriesReplyWithPeerIDChannel

		rf.mu.Lock()
		if resp.reply.Term == LostConnection {
			LostConnectionCnt++

			if LostConnectionCnt >= len(rf.peers)/2+1 {
				rf.processNetworkFailure(AppendRequest, true)
				return
			}
		} else {

			if resp.reply.Success == true {

				// New term, or new leader elected during waiting the response.
				if rf.voteFor != rf.me || rf.currentTerm > currentTerm {
					rf.processReElectionCondition(currentTerm, VoteRequest)
					return
				}

				if resp.reply.Success == true {

					successCnt++

					// Update the corresponded next index of the server, if this server has successfully replicated the logs.
					if currentLastLogIndex >= 1 {
						rf.nextIndex[resp.server] = currentLastLogIndex + 1
					}

					// The logs has committed on the majority servers.
					if successCnt >= len(rf.peers)/2+1 {
						rf.processSuccessRequest(AppendRequest, currentLastLogIndex)
						return

					}

				} else {
					// Record the most updated Term from the peer.
					if updatedTerm < resp.reply.Term {
						updatedTerm = resp.reply.Term
					}
					//Figure2,Rules for server, $5.3 If the AppendEntries fails because of log inconsistency, decrement nextIndex and retry. (Wait next heartbeat)
					// TODO can optimize this.

					// If append log entries, update the corresponded index
					if currentLastLogIndex >= 1 {
						rf.nextIndex[resp.server]--
					}
				}
			}
		}

		rf.mu.Unlock()
	}

	rf.processFailedRequest(AppendRequest, updatedTerm)
	return
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	// We recommend using a
	// conservative election timeout such as 150–300ms; such time-
	// outs are unlikely to cause unnecessary leader changes and
	// will still provide good availability.

	electionTimeoutV := 150
	heartbeatTimeoutV := 40

	for rf.killed() == false {
		r1 := rand.New(rand.NewSource(time.Now().UnixMilli()))
		rf.mu.Lock()
		role := rf.role
		DPrintf("%v rf current role : %v at term %v, time : %v", rf.me, role, rf.currentTerm, time.Now().UnixMilli())
		switch role {
		case Leader:
			rf.mu.Unlock()
			<-time.After(time.Duration(heartbeatTimeoutV) * time.Millisecond)
			// nil means heartbeat
			go rf.LeaderAppendEntries(nil)

			applySuccess := <-rf.appendSuccessChannel
			rf.mu.Lock()
			if applySuccess == false {
				rf.role = Follower
			}
			rf.mu.Unlock()

		case Follower:
			rf.mu.Unlock()

			<-time.After(time.Duration(time.Duration(electionTimeoutV)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(100)+int32(rf.me)*5)))

			rf.mu.Lock()
			if rf.reElectionForFollower == true {
				DPrintf("%v rf changed to Candidate now", rf.me)
				rf.role = Candidate
			} else {
				rf.reElectionForFollower = true
			}
			rf.mu.Unlock()

		case Candidate:
			rf.mu.Unlock()

			go rf.launchElection()
			success := <-rf.votedSuccessChannel

			rf.mu.Lock()
			if success == true {
				DPrintf("%v rf Leader now at term %v", rf.me, rf.currentTerm)
				rf.role = Leader
				rf.initializeIndexForFollowers()
			} else {
				rf.role = Follower
			}
			rf.mu.Unlock()

		}
	}

	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

}

/// Initialize the channels used by raft.
func (rf *Raft) initializeChannels() {
	rf.getHeartbeatChannel = make(chan bool)
	//rf.reElectionForCandidateChan = make(chan bool)

	rf.voteChannel = make(chan RequestVoteReply, len(rf.peers)-1)
	rf.votedSuccessChannel = make(chan bool)

	rf.AppendEntriesReplyWithPeerIDChannel = make(chan AppendEntriesReplyWithPeerID, len(rf.peers)-1)

	rf.applyTimeoutChannel = make(chan bool)
	rf.appendSuccessChannel = make(chan bool)

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

	rf.initializeChannels()

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
