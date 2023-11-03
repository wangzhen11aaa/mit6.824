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

	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	lastSnapshotIndex int
	// term of lastSnapshotIndex
	lastSnapshotTerm int
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
	LostConnection = -1
	ReSend         = 0
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

type SnapShotReplyWithPeerID struct {
	reply  SnapShotReply
	server int
}

type VoteRequestReplyWithPeerID struct {
	reply  VoteRequestReply
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

	// Lock for apply
	applyMu sync.Mutex

	// snapshot中的最后一个位置,当snapshot为空时，此值是0
	lastSnapshotIndex int

	electionTimeoutV int

	// snapshot中的最后一个元素的Term,当snapshot为空时，此值没有意义。
	lastSnapshotTerm int

	// 0: follower, 1: candidate, 2. leader
	role int
	// True if no leader or candidate sends the RPC or heartbeat to Follower.
	reElectionForFollower bool

	// True if the candidate does not win the election or lose election.
	//reElectionForCandidateChan chan bool
	logs []LogEntry

	// Recodes the leader cost time.
	timers []int

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

	// 这里的to be不是将来时，而是已知的已经被committed的下标。
	// Index of highest log entry known to be committed.(Initialized to 0, increases monotonically).
	// The leader keeps track of the highest index it knows to be committed, and it includes that index in future AppendEntries RPCs(including heartbeats) so that the other servers eventually find out.
	commitIndex int

	// 这里也是一样，lastApplied也是已知的已经被applied的下标。
	// Index of highest log entry applied to state machine. Initialized to 0, increased monotonically
	lastApplied int

	// The following two need to be Reinitialized after election.
	// 这里的next指明的是已经被committed的下一个要被committed位置。
	// For each server, index of the next log entry to send to that server. (Initialized to leader last log index + 1)
	nextIndex []int

	// 用于记录各个Follower已知的被committed的最后一条的下标。
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	// Leader将所有日志都persist()之后的情况会发生两种，
	// 存在极端情况，即
	//	1. client->Leader, (Leader Append)
	//  2. Leader->Followers (Append),
	//  3. Followers Append Successfully,
	//  4. Leader crash before return to Client.
	// 结果为Client收到Leader的异常，但是集群已经committed成功。但是Leader没有返回到Client.
	// 此时分为两种情况
	// 第一种: Leader 进行了Persist,(那么结果并没有丢,第一种情况如果3节点中，2个Followers都成功进行了Persist,如果此时Leader 也Persist()成功，那么数据就丢不了.第二种情况，即此时只有一个Follower成功进行了Persist(),因为Leader也成功进行了Persist(),即数据已经被committed，数据不会丢)
	// 第二种: Leader 没有完成Persist(),便Crash,如果数据被committed,那么Client虽然收到Leader crash的Error，但是数据还是被写入了。如果是3节点中仅仅有1个Follower进行了Persist(),如果使用的是对rf.logs整体进行的Persist()，那么Leader会在上一次persist()时，将刚刚写入的Logs进行Persist()。
	// 为了满足，只要数据已经存储在Majority的节点即为committed的性质，下面需要对所有的Logs进行 Persist(). // Leader只对已经committed的日志进行Persist.
	// 否则，如果Leader在LeaderAppendEntries开始就进行persist() 会出现两个问题
	// 1. 所有的调用都会调用persist(),那么普通的heartbeat也调用persist()这样开销很大。
	// 2. 会出现混乱提交，比如出现一种情况，比如5个节点，Raft1 刚刚选举为Leader就断网了，那么它会继续接受Client的请求并且一直persist().此时Term为Term0。剩余节点选举出Leader Raft2 此时Term为Term1 (Term1 > Term0),集群并没有做任何Log提交，然后Raft1重启联网，从Follower->Candidate, Term0变为Term1,并且其Logs的长度比其他节点都长，此时Raft1 开始将日志Append给其他Follower,但是其实这些并没有committed,却被当做了Committed的Log。加入存在另外一个Raftx表现和Raft1一致，但是最后状态比Raft1的Term更大，那么Raftx的Log会被再Apply一次，相当于同一个位置的有不同的Log提交，导致冲突。

	if rf.role == Leader {
		e.Encode(rf.logs[:rf.commitIndex-rf.lastSnapshotIndex+1])
	} else {
		e.Encode(rf.logs)
	}
	// 将snapshot的关键信息进行持久化
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)

	raftState := w.Bytes()

	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	rf.persister.SaveRaftState(raftState)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	DPrintf("Goroutine %v, %v rf readPersist at time %v", GetGOId(), rf.me, time.Now().UnixMilli())
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry

	var lastSnapshotIndex int
	var lastSnapshotTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&lastSnapshotTerm) != nil {
		DPrintf("Error")
	} else {
		DPrintf("Goroutine: %v, %v rf readPersist currentTerm: %v, voteFor: %v, logs: %v at term %v, time %v", GetGOId(), rf.me, currentTerm, voteFor, logs, rf.currentTerm, time.Now().UnixMilli())
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs

		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm

		// 用户重启之后的身份是Follower,此时需要再次选举。
		rf.role = Follower

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

//获取当前存储位置的索引
func (rf *Raft) getStoreIndexByLogIndex(logIndex int) int {
	storeIndex := logIndex - rf.lastSnapshotIndex
	if storeIndex < 0 {
		return -1
	}
	return storeIndex
}

//获取持久化的数据
func (rf *Raft) persistRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.logs)
	data := w.Bytes()
	return data
}

//返回当前状态机的最后一条日志的任期和索引
//索引是一直会增大的，但是我们的日志队列却不可能无限增大，在队列中下标0存储快照
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	return rf.logs[len(rf.logs)-1].Term, rf.lastSnapshotIndex + len(rf.logs) - 1
}

func (rf *Raft) getMappedIndex(straightIndex int) int {
	return straightIndex - rf.lastSnapshotIndex
}

// 将snapshot发送到比较落后的Follower，特别是指的是它的
func (rf *Raft) InstallSnapShot(args *SnapShotAppendArgs, reply *SnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, currentLastLogIndex := rf.getLastLogTermAndIndex()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	} else if rf.currentTerm == args.Term {

		if rf.role == Leader {
			return
		}

		if rf.role == Follower && rf.voteFor != args.LeaderId {
			return
		}

		rf.role = Follower
		rf.voteFor = args.LeaderId
		rf.reElectionForFollower = false
		rf.lastSnapshotIndex = args.LastIncludeIndex
		rf.lastSnapshotTerm = args.LastIncludedTerm

	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm

		rf.role = Follower
		rf.voteFor = args.LeaderId
		rf.reElectionForFollower = false
		rf.lastSnapshotIndex = args.LastIncludeIndex
		rf.lastSnapshotTerm = args.LastIncludedTerm
	}

	if currentLastLogIndex <= args.LastIncludeIndex {
		rf.logs = make([]LogEntry, 1)
	} else {
		installLen := currentLastLogIndex - args.LastIncludeIndex
		rf.logs = rf.logs[:installLen]
		rf.logs[0].Cmd = nil
	}

	rf.commitIndex = args.LastIncludeIndex
	rf.lastApplied = args.LastIncludeIndex

	rf.logs[0].Term = args.LastIncludedTerm

	// 应用这些
	rf.persister.SaveStateAndSnapshot(rf.persistRaftState(), args.Data)
}

func (rf *Raft) installSnapShotToFollower(server int, args *SnapShotAppendArgs, reply *SnapShotReply, result chan SnapShotReplyWithPeerID, lastSnapShotIndex int, currentTerm int) {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	if ok {
		rf.mu.Lock()

		DPrintf("Goroutine %v ,%v rf Leader got the installSnapShotToFollower result from  %v, reply: %v \n", GetGOId(), rf.me, server, reply)

		if rf.role == Leader && currentTerm >= reply.Term {
			rf.updateNextIndexAndMatchIndex(server, lastSnapShotIndex)
		}

		rf.mu.Unlock()
		result <- SnapShotReplyWithPeerID{reply: *reply, server: server}

	} else {
		DPrintf("Goroutine: %v, %v rf lost from leader %v \n", GetGOId(), server, args.LeaderId)
		reply.Term = LostConnection
		result <- SnapShotReplyWithPeerID{reply: *reply, server: server}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludeTerm int, lastIncludeIndex int, snapshot []byte) bool {

	// Your code here (2D).

	_, lastIndex := rf.getLastLogTermAndIndex()

	// 如果通过applyCh传递过来的Msg中包含的lastIncludeIndex大于现有的lastIndex,说明了当前logs已经被传送过来的snapshot覆盖，该snapshot将会被install,所以当前rf.logs已经没有必要保留了。
	if lastIncludeIndex >= lastIndex {
		rf.logs = make([]LogEntry, 1)
	} else {
		// lastIncludeIndex < lastIndex,说明需要,有一部分logs已经被installed的snapshot覆盖，需要裁剪掉。
		instalLen := lastIncludeIndex - rf.lastSnapshotIndex
		rf.logs = rf.logs[:instalLen]
		rf.logs[0].Cmd = nil
	}
	rf.logs[0].Term = lastIncludeTerm

	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludeIndex, lastIncludeTerm
	rf.persister.SaveStateAndSnapshot(rf.persistRaftState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, cmd []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastSnapshotIndex >= index {
		DPrintf("Goroutine%v, %v rf rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", GetGOId(), rf.me, index, rf.lastSnapshotIndex, rf.currentTerm)
		return
	}
	oldLastSnapshotIndex := rf.lastSnapshotIndex

	actualIndexInLogs := rf.getStoreIndexByLogIndex(index)

	rf.lastSnapshotTerm = rf.logs[actualIndexInLogs].Term
	rf.lastSnapshotIndex = index

	// 比如这一次snapshot最后一个Log所在位置是index,上一次snapshot的最后位置是oldLastSnapShotIndex.
	rf.logs = rf.logs[index-oldLastSnapshotIndex:]

	// 0位置设置为快照命令
	rf.logs[0].Term = rf.lastSnapshotTerm

	r := bytes.NewBuffer(cmd)
	d := labgob.NewDecoder(r)
	var cmd_ int
	if d.Decode(&cmd_) != nil {
		DPrintf("Error in Decode cmd")
		rf.logs[0].Cmd = nil
	} else {
		rf.logs[0].Cmd = cmd_
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(index)
	var xlog []interface{}
	for j := 0; j <= index; j++ {
		xlog = append(xlog, rf.logs[j])
	}
	e.Encode(xlog)

	DPrintf("Goroutine %v, %v rf Start Snapshot end logs %v at time %v", GetGOId(), rf.me, rf.logs, time.Now().UnixMilli())

	rf.persister.SaveStateAndSnapshot(rf.persistRaftState(), w.Bytes())

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

type VoteRequestReply struct {
	// currentTerm, for candidate to update itself.
	Term int
	// true means candidate received vote.
	VoteGranted bool
	// Your data here (2A).
}

type SnapShotAppendArgs struct {
	// leader's term
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludedTerm int
	Data             []byte
}

type SnapShotReply struct {
	// currentTerm, for leader to update itself.
	Term int
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

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *AppendEntriesReply, result chan AppendEntriesReplyWithPeerID, nextIndexToCommit int) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("Goroutine %v ,%v rf Leader got the AppendEntries result from  %v, status: %v \n", GetGOId(), rf.me, server, reply.Success)
		// 如果Follower响应. 把nextIndex的更新放在这里的原因是，因为调用者会在收集到Majority的success之后会直接退出，导致对应的栈不能使用，从而后面的响应无法进行更新。然而，sendAppendEntries是独立的栈，可以对后面返回的结果进行更新。
		rf.mu.Lock()
		if rf.role == Leader {
			if reply.Success {
				if nextIndexToCommit > rf.nextIndex[server] {
					DPrintf("Goroutine %v, %v rf Leader (UpdateNextIndexAndMatchIndex Method), got the AppendEntries from peer %v, status: %v, nextIndexToCommit: %v \n", GetGOId(), rf.me, server, reply.Success, nextIndexToCommit)
					rf.updateNextIndexAndMatchIndex(server, nextIndexToCommit)
				}
			} else {
				DPrintf("Goroutine %v, %v Leader (UpdateFollowerNextIndex Method), got the AppendEntries from peer %v, status: %v,args: %v, reply: %v \n", GetGOId(), rf.me, server, reply.Success, args, reply)
				// 如果Peer响应失败，
				// 有两种情况
				// 第一种情况是 这个Leader 遇到了更大的Term,
				// 				reply.Term > args.Term, 这种情况需要当前节点的role从Leader->Follower。
				// 第二种情况是，Peer需要重新传送日志,
				//				reply.Term <= args.Term, 这种情况需要更新nextIndex值，需要下一次传送从新的位置开始发送。
				if reply.Term <= args.Term {
					rf.updateFollowerNextIndex(server, reply.Term)
					// 标记reply.Term 为-1，表示是需要再次传输日志而导致的失败，这种情况Leader不会变成Follower
					reply.Term = ReSend
				}
			}
		}
		rf.mu.Unlock()
		result <- AppendEntriesReplyWithPeerID{reply: *reply, server: server}
		DPrintf("Goroutine %v ,%v rf executed the AppendEntries from leader Ended %v, status: %v \n", GetGOId(), server, args.LeaderId, reply.Success)
	} else {
		DPrintf("Goroutine: %v, %v rf lost from leader %v \n", GetGOId(), server, args.LeaderId)
		reply.Term = LostConnection
		result <- AppendEntriesReplyWithPeerID{reply: *reply, server: server}
		DPrintf("Goroutine: %v, %v rf lost from leader Ended %v \n", GetGOId(), server, args.LeaderId)
	}
	return ok
}

// 初始化rf.nextIndex和rf.matchIndex
func (rf *Raft) initializeNextIndexAndMatchIndex() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + rf.lastSnapshotIndex
		rf.matchIndex[i] = 0
	}
}

// 这里直接对日志进行操作。
func (rf *Raft) tryAppendLogs(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("Goroutine: %v, %v rf  Begin tryAppendLogs at term :%v at time %v, logs: %v, args: %v, rf.commitIndex %v, rf.lastApplied : %v", GetGOId(), rf.me, rf.currentTerm,
		time.Now().UnixMilli(), rf.logs, args, rf.commitIndex, rf.lastApplied)

	lastTerm, lastIndex := rf.getLastLogTermAndIndex()

	if lastIndex < args.PrevLogIndex {
		reply.Term = lastTerm
		reply.Success = false
		return
	}

	mappedPrevLogIndex := rf.getMappedIndex(args.PrevLogIndex)
	if rf.logs[mappedPrevLogIndex].Term == args.PrevLogTerm {
		if len(args.Entries) != 0 {

			rf.logs = rf.logs[:mappedPrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)

			rf.persist()
		}
		rf.commitIndex = args.LeaderCommit

		reply.Term = rf.currentTerm
		reply.Success = true
	} else {

		reply.Term = Min(rf.logs[mappedPrevLogIndex].Term, args.PrevLogTerm)
		reply.Success = false

		if rf.logs[mappedPrevLogIndex].Term > args.PrevLogTerm {
			DPrintf("Goroutine %v, %v rf fatal error, rf.logs[mappedPrevLogIndex].Term: %v > args.PrevLogTerm: %v at time %v", GetGOId(), rf.me, rf.logs[mappedPrevLogIndex].Term, args.PrevLogTerm, time.Now().UnixMilli())
		} else {
			DPrintf("Goroutine %v, %v rf rf.logs[mappedPrevLogIndex].Term: %v != args.PrevLogTerm: %v at time %v", GetGOId(), rf.me, rf.logs[mappedPrevLogIndex].Term, args.PrevLogTerm, time.Now().UnixMilli())
		}
	}
	DPrintf("Goroutine: %v,%v rf End AppendLog at term :%v at time %v, logs: %v", GetGOId(), rf.me, rf.currentTerm,
		time.Now().UnixMilli(), rf.logs)

}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	DPrintf("Goroutine: %v, in AppendEntries %v rf, rf.lastApplied: %v, rf.commitIndex: %v logs: %v, args: %v, term:%v, time:%v", GetGOId(), rf.me, rf.lastApplied, rf.commitIndex, rf.logs, args, rf.currentTerm, time.Now().UnixMilli())

	// AppendEntires rule 1.Reply false if term < current Term
	// 如果当前的Term 比 Leader发送过来的Term大，那么就返回自己这边比较大的Term给发送Append的节点。
	if rf.currentTerm > args.Term {
		DPrintf("Goroutine: %v, %v rf returned false because of %v < %v at term %v at time:%v", GetGOId(), rf.me, args.Term, rf.currentTerm, rf.currentTerm, time.Now().UnixMilli())
		return
	}

	if rf.currentTerm == args.Term {

		if rf.role == Leader {
			DPrintf("Goroutine: %v,Leader %v rf fatal error, encounter another Leader %v rf with same term %v", GetGOId(), rf.me, args.LeaderId, time.Now().UnixMilli())
			return
		}

		if rf.role == Follower && rf.voteFor != args.LeaderId {
			DPrintf("Goroutine: %v, Follower %v rf fatal error, encounter another Leader %v rf with same term %v", GetGOId(), rf.me, args.LeaderId, time.Now().UnixMilli())
			return
		}

		// 如果是Candidate或者Follower
		DPrintf("%v 's [current role: %v to Follower]: currentTerm changed from %v to %v, in AppendEntries for the coming request %v", rf.me, rf.role, rf.currentTerm, args.Term, args.LeaderId)

		rf.role = Follower
		rf.voteFor = args.LeaderId
		// 并且禁止自己参与下一次的选举
		rf.reElectionForFollower = false

		rf.tryAppendLogs(args, reply)
	}

	if rf.currentTerm < args.Term {

		rf.role = Follower
		rf.currentTerm = args.Term
		rf.voteFor = args.LeaderId
		rf.reElectionForFollower = false

		rf.tryAppendLogs(args, reply)
	}

	if rf.lastApplied < rf.lastSnapshotIndex {

		rf.lastApplied = rf.lastSnapshotIndex
		rf.commitIndex = rf.lastSnapshotIndex

		return
	}

	// 说明Follower的已知commitIndex小于Leader的commitIndex(即args.LeaderCommit)，此时Follower需要进行Apply.
	if rf.lastApplied < rf.commitIndex {

		// applySyncWithHeader是同步操作，时携带Lock的，这把Lock是锁Raft本身的,因为这个同步操作可能会比较耗时，所以在这里持有锁做这个事情，明显事不对的。
		// 为了避免sync上出现out of order的情况，我们需要另外一把锁，专门用来同步apply的操作，但是不影响Raft其他的逻辑。

		// 将当前rf.logs复制出一份
		logsCp := make([]LogEntry, len(rf.logs))
		copy(logsCp, rf.logs)

		if rf.lastApplied < rf.commitIndex {
			lastApplied := rf.getMappedIndex(rf.lastApplied)
			commitIndex := rf.getMappedIndex(rf.commitIndex)
			lastSnapshotIndex := rf.lastSnapshotIndex

			DPrintf("Goroutine: %v, %v rf apply logs %v from %v to %v", GetGOId(), rf.me, logsCp, lastApplied, commitIndex)
			// Figure 2, Rules for Servers
			// If the commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
			go rf.applyLogs(commitIndex, lastApplied, logsCp, rf.currentTerm, lastSnapshotIndex)
		}
		rf.lastApplied = rf.commitIndex
	}

}

// vote log consist check.
// 选举时，需要比较候选人和当前节点的日志，哪个更新。
// 如果候选人的日志更新，那么就支持, 返回true。
// 如果候选人的日志没有当前节点的日志新，那么就返回false。
func (rf *Raft) checkLogsInVote(args *RequestVoteArgs) bool {
	DPrintf("Goroutine: %v, %v rf checkLogsInVote, rf.logs: %v,args: %v at term %v, time :%v ", GetGOId(), rf.me, rf.logs, args, rf.currentTerm, time.Now().UnixMilli())

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		return false
	}
	return true
}

// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *VoteRequestReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Goroutine: %v, %v rf in requestVote, role: %v, rf.logs:%v, rf.voteFor :%v, args: %v at term %v, time: %v", GetGOId(), rf.me, rf.role, rf.logs, rf.voteFor, args, rf.currentTerm, time.Now().UnixMilli())

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// RequestVote rule 1.Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		DPrintf("%v rf rejected peer %v, reason: %v's term: %v > %v's term: %v", rf.me, args.CandidateId, rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	} else if rf.currentTerm == args.Term {
		if rf.role == Leader {
			return
		}
		// 如果由于网路问题，发送了两次Vote
		if rf.voteFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}

		if rf.voteFor != Nobody && rf.voteFor != args.CandidateId {
			return
		}
	} else {
		rf.currentTerm = args.Term
		rf.role = Follower

		if rf.checkLogsInVote(args) {
			DPrintf("Goroutine: %v, %v rf [current role: %v to Follower]: currentTerm changed from %v to %v, in RequestVote for the coming request %v rf, args: %v, checkLogsInVote Result: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, true, time.Now().UnixMilli())

			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.reElectionForFollower = false

		} else {
			DPrintf("Goroutine: %v, %v rf [current role: %v unchanged]: currentTerm changed from %v to %v, in RequestVote for the coming request %v rf, args: %v, checkLogsInVote Result: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, false, time.Now().UnixMilli())
			reply.VoteGranted = false

			rf.reElectionForFollower = true
			rf.voteFor = Nobody
		}

		rf.persist()

		// If raft node follows leader, then this request suppress follower's election.
		return
	}

	// RequestVote RPC rule for receiver: 2.0 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *VoteRequestReply, voteChannel chan VoteRequestReplyWithPeerID) bool {
	//DPrintf("%v request vote for [%v] \n", args.CandidateId, server)
	// This is synchronized call.
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			DPrintf("Goroutine: %v, %v rf got vote support from %v at term %v \n", GetGOId(), args.CandidateId, server, args.Term)
		} else {
			DPrintf("Goroutine %v, %v rf got vote reject from %v at term %v \n", GetGOId(), args.CandidateId, server, args.Term)
		}
		voteChannel <- VoteRequestReplyWithPeerID{reply: *reply, server: server}
	} else {
		// If the can not reach the server
		//fmt.Printf("[%v] lost from %v \n", server, args.CandidateId)
		reply.Term = LostConnection
		voteChannel <- VoteRequestReplyWithPeerID{reply: *reply, server: server}

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
	//rf.mu.Lock()
	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs) + rf.lastSnapshotIndex
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Idx: index, Cmd: command})
		rf.mu.Unlock()
		DPrintf("Method Start() %v rf Append cmd %v to Leader at index %v,at time %v", rf.me, command, index, time.Now().UnixMilli())
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
	rq.Term = rf.currentTerm
	rq.CandidateId = rf.me
	rq.LastLogTerm, rq.LastLogIndex = rf.getLastLogTermAndIndex()
}

func (rf *Raft) prepareRequest(currentTerm *int, rq *RequestVoteArgs) {
	rf.currentTerm += 1

	// If the follower timeout, then we need not care about who is the leader.
	rf.voteFor = rf.me

	DPrintf("Goroutine: %v, [%v] rf voted himself at %v, time: %v\n", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
	rf.initElectionRequest(rq)

	*currentTerm = rf.currentTerm

}

func (rf *Raft) launchElection() {
	rf.mu.Lock()

	if rf.role != Candidate {
		defer rf.mu.Unlock()
		return
	}

	var currentTerm int
	rq := RequestVoteArgs{}

	replies := make([]VoteRequestReply, len(rf.peers))
	rf.prepareRequest(&currentTerm, &rq)

	rf.mu.Unlock()
	voteChannel := make(chan VoteRequestReplyWithPeerID, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf("Goroutine: %v,%v rf send to %v, rq:{Term: %v} \n", GetGOId(), rq.CandidateId, i, rq.Term)
		go rf.sendRequestVote(i, &rq, &replies[i], voteChannel)
	}

	lostConnectionCnt := 1
	voted := 1
	denyCnt := 0

	// 使用一个Map将这些结果收集到map中，方便使用日志分析问题，因为由于日志是由各个goroutine交叉打印，造成了很多不便。
	logAppendResultMap := make(map[int]VoteRequestReplyWithPeerID)

	for i := 0; i < len(rf.peers)-1; i++ {
		response := <-voteChannel

		logAppendResultMap[i] = response

		rf.mu.Lock()
		DPrintf("Goroutine: %v,%v rf requestVote receive %v, total reply: %v, at term %v, time :%v", GetGOId(), rf.me, response, logAppendResultMap, rf.currentTerm, time.Now().UnixMilli())

		if currentTerm != rf.currentTerm {
			defer rf.mu.Unlock()
			DPrintf("Goroutine: %v, %v rf Leader Term changed from %v to %v , at time %v", GetGOId(), rf.me, currentTerm, rf.currentTerm, rf.currentTerm)
			return
		}

		if currentTerm < response.reply.Term {
			defer rf.mu.Unlock()
			rf.currentTerm = response.reply.Term
			rf.role = Follower
			rf.voteFor = Nobody

			rf.reElectionForFollower = false
			rf.persist()
			return
		}

		// 如果rf.role有变化,不更改当前Raft的任何状态，直接返回。
		if rf.role != Candidate {
			defer rf.mu.Unlock()
			DPrintf("Goroutine: %v, %v rf changed role from 1 to %v at term : %v, time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, time.Now().UnixMilli())
			return
		}

		// 网络问题，拿不到Peer的返回结果,所以不需要包含Term的大小对比。
		// 这一种情况肯定是最后从channel中获取，因为耗时太长。
		if response.reply.Term == LostConnection {
			lostConnectionCnt++

			if lostConnectionCnt >= len(rf.peers)/2+1 {

				defer rf.mu.Unlock()
				rf.processNetworkFailure(VoteRequest)
				return

			}
		} else {
			// Peer正常返回结果。因为reply.Term > currentTerm肯定意味着reply.VoteGranted等于false。
			if response.reply.VoteGranted {

				// Count the successful votes
				voted++
				// 如果选举个数超过一半，那么就可以更新Raft状态，并退出。
				if voted >= len(rf.peers)/2+1 {
					rf.processMajoritySuccessVoteRequest()
					return
				}

			} else {
				denyCnt++
				if denyCnt >= len(rf.peers)/2+1 {
					defer rf.mu.Unlock()

					rf.role = Follower
					rf.voteFor = Nobody
					rf.reElectionForFollower = false

					return
				}
			}
		}
		rf.mu.Unlock()
	}

}

func Max(x int, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func Min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// Process the success append entries request condition.
func (rf *Raft) applyLogs(commitIndex int, lastApplied int, logs []LogEntry, currentTerm int, lastSnapshotIndex int) {
	// When the majority appended successfully, we update local commitIndex for the next logs to append.
	// The leader can apply these logs safely. logs[leaderCommitStartIndex, localCommitStartIndex)
	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()

	DPrintf("Goroutine: %v, %v rf[Leader] begin apply logs: %v  at time %v, rf.lastApplied :%v, rf.commitIndex: %v", GetGOId(), rf.me, logs, time.Now().UnixMilli(), lastApplied, commitIndex+1)

	for i := lastApplied + 1; i < commitIndex+1; i += 1 {
		DPrintf("logs: %v", logs)
		if logs[i].Cmd == nil {
			rf.applyCh <- ApplyMsg{SnapshotValid: true, Command: logs[i].Cmd, CommandIndex: i + lastSnapshotIndex}
		} else {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: logs[i].Cmd, CommandIndex: i + lastSnapshotIndex}
		}
	}

	DPrintf("Goroutine:%v,[Leader] %v rf apply Log End, rf.lastApplied changed from %v to %v at term %v, at time %v", GetGOId(), rf.me, lastApplied, commitIndex, currentTerm, time.Now().UnixMilli())

}

// Update leader commit index.
// 算法解决的是，在matchIndex[] 数组中，找到N，这个N是大于matchIndex中超过一半的最大值。
func (rf *Raft) updateLeaderCommitIndex() {
	// Find the N, that N > commitIndex, a majority of matchIndex[i] >= N and log[N].term == currentTerm: set commitIndex = N.
	cnt := make(map[int]int)
	keySet := make(map[int]interface{})
	keys := []int{}
	// auxMap[i]的value对应所有比i小的元素组成的slice.
	auxMap := make(map[int][]int)
	// matchIndex中存放的是 各个server已经commit的位置。
	// 先获取各个位置的元素。 然后按照顺序插入到slice中，按照从小到大进行排序。

	for _, k := range rf.matchIndex {
		keySet[k] = nil
	}

	// 需要一个map 记录比当前 x所有小的值。
	for k := range keySet {
		keys = append(keys, k)
	}

	sort.Ints(keys)
	DPrintf("keys: %v", keys)

	// keys: [1,2,3] => auxMap{1:[],2:[1],3:[1,2]}
	auxSlice := []int{}
	for i := 0; i < len(keys); i++ {
		// 将对应的位置作为key, 所有比key小的组成的slice作为value.
		auxMap[keys[i]] = auxSlice
		auxSlice = append(auxSlice, keys[i])
	}

	for k, v := range auxMap {
		DPrintf("auxMap: k: %v, v: %v", k, v)
	}

	for _, k := range rf.matchIndex {
		// 直接将对应map中的v+1.golang中的map默认值为0.
		DPrintf("k: %v", k)
		cnt[k]++
		if v, ok := auxMap[k]; ok {
			// 将小于k的所有的值 sk，所在cnt[sk]++
			for _, sk := range v {
				DPrintf("sk: %v", sk)
				cnt[sk]++
			}
		}

	}
	DPrintf("Goroutine: %v, %v rf Commit index in updateLeaderCommitIndex cnt: %v", GetGOId(), rf.me, cnt)

	// Select the N in the cnt map.
	// 使用map采用计数的方式不可行。这里需要找到一个K，这个K>=map中的k的最大值，并且这个值的数量大于len(rf.peers).
	// map[0:2, 1:1, 2:1]这里面得到的K应该是1.而不是0.
	var n int
	n = -1
	for k, v := range cnt {
		DPrintf("Goroutine :%v, k: %v, v:%v \n", GetGOId(), k, v)
		if v >= (len(rf.peers)/2 + 1) {
			if k > n {
				n = k
			}
		}
	}

	DPrintf("Goroutine: %v, %v rf Leader updateLeaderCommitIndex rf.commitIndex changed from %v to %v, rf.matchIndex: %v, rf.nextIndex: %v", GetGOId(), rf.me, rf.commitIndex, n, rf.matchIndex, rf.nextIndex)
	rf.commitIndex = n

}

// Process the success vote request condition.
// 如果此选举人被超过1/2的人支持，那么就将当前节点进行初始化。
func (rf *Raft) processMajoritySuccessVoteRequest() {
	rf.role = Leader

	DPrintf("Goroutine: %v, %v rf Leader now at term %v, rf's logs: %v, cost time %v", GetGOId(), rf.me, rf.currentTerm, rf.logs, int(time.Now().UnixMilli())-rf.timers[rf.me])

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) + rf.lastSnapshotIndex
		rf.matchIndex[i] = rf.nextIndex[i] - 1
	}

	rf.mu.Unlock()
}

// Process the network failure condition
// 保持Candidate 角色不变, Leader角色也不变。
func (rf *Raft) processNetworkFailure(typ int) {
	// rf.mu.Lock()
	if typ == VoteRequest {
		DPrintf("Goroutine: %v, %v rf Remain Candidate (Network Problem) at term %v, at time:%v", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
	} else {
		DPrintf("Goroutine: %v, %v rf Remain Leader (Network Problem) at term %v, at time:%v", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
	}
}

func (rf *Raft) prepareAppendRequest(rq *RequestAppendEntriesArgs) {
	rq.Term = rf.currentTerm
	rq.LeaderId = rf.me
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

// 返回小于等于term的最大元素的位置
func (rf *Raft) bfMinimumLargePos(term int) int {
	right := len(rf.logs)
	left := 0
	for left < right {
		mid := (left + right) / 2
		if rf.logs[mid].Term >= term {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return left
}

// 通过二分查找，找到 潜在follower返回Term的最后一个元素。
func (rf *Raft) updateFollowerNextIndex(server int, followerExpectTerm int) {

	DPrintf("Goroutine: %v, %v rf, term: %v, logs: %v, followerExpectTerm: %v, commitIndex: %v", GetGOId(), rf.me, rf.currentTerm, rf.logs, followerExpectTerm, rf.commitIndex)

	// 这种情况发生在，当Leader重启时，连续两次发送LeaderAppendEntries信号给server s, server 都进行了返回。
	// 因为如果第一次Leader在收到server s 的返回时就将rf.nextIndex[s]设置为0，那么后面返回给leader的请求就会直接以0作为计算。
	if rf.nextIndex[server] == 0 {
		rf.nextIndex[server] = 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		return
	}

	prevTerm := rf.logs[rf.nextIndex[server]-1].Term

	// 二分法，找到大于等于preTerm的第一个元素
	leftPos := rf.bfMinimumLargePos(prevTerm)

	rf.nextIndex[server] = (leftPos + rf.nextIndex[server]) / 2
	rf.matchIndex[server] = rf.nextIndex[server] - 1

	DPrintf("Goroutine: %v, %v rf, rf.nextIndex%v :changed from %v to %v term: %v, logs: %v, followerExpectTerm: %v", GetGOId(), rf.me, server, leftPos, rf.nextIndex[server], rf.currentTerm, rf.logs, followerExpectTerm)
}

// Update nextIndex and matchIndex for this server.
func (rf *Raft) updateNextIndexAndMatchIndex(server int, nextIndexToCommit int) {

	DPrintf("Goroutine: %v, %v rf, rf.nextIndex%v :changed from %v to %v term: %v, logs: %v", GetGOId(), rf.me, server, rf.nextIndex[server], nextIndexToCommit, rf.currentTerm, rf.logs)

	rf.nextIndex[server] = nextIndexToCommit

	rf.matchIndex[server] = nextIndexToCommit - 1

}

func (rf *Raft) LeaderAppendEntries(command interface{}) {
	rf.mu.Lock()

	// 已经不是Leader
	if rf.role != Leader {
		defer rf.mu.Unlock()
		return
	}

	// 保存当前raft的Term到当前栈
	currentTerm := rf.currentTerm

	logAppendRequest := RequestAppendEntriesArgs{}
	rf.prepareAppendRequest(&logAppendRequest)

	// 保存当前raft的logs的长度到当前栈
	_, lastIndex := rf.getLastLogTermAndIndex()

	logAppendRequest.LeaderCommit = rf.commitIndex

	logAppendReplyChan := make(chan AppendEntriesReplyWithPeerID)
	snapShotAppendReplyChan := make(chan SnapShotReplyWithPeerID)

	logAppendReplies := make([]AppendEntriesReply, len(rf.peers))
	snapAppendReplies := make([]SnapShotReply, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {

			DPrintf("Goroutine: %v, %v rf in LeaderAppendEntries rf.nextIndex%v :%v, rf.commitIndex: %v, nextIndexToCommit: %v, at term %v time: %v ", GetGOId(), rf.me, i, rf.nextIndex[i], rf.commitIndex, lastIndex+1, rf.currentTerm, time.Now().UnixMilli())

			prevLogIndex := rf.nextIndex[i] - 1
			if prevLogIndex < 0 {
				prevLogIndex = 0
			}

			mappedPrevLogIndex := rf.getMappedIndex(prevLogIndex)

			// 落后集群，需要InstallSnapShot
			if mappedPrevLogIndex < 0 {

				snapShotRequestArgs := SnapShotAppendArgs{}
				snapShotRequestArgs.Term = rf.currentTerm
				snapShotRequestArgs.LeaderId = rf.me
				snapShotRequestArgs.LastIncludeIndex = rf.lastSnapshotIndex
				snapShotRequestArgs.LastIncludedTerm = rf.lastSnapshotTerm
				snapShotRequestArgs.Data = rf.persister.ReadSnapshot()

				go rf.installSnapShotToFollower(i, &snapShotRequestArgs, &snapAppendReplies[i], snapShotAppendReplyChan, rf.lastSnapshotIndex+1, rf.currentTerm)

			} else {

				prevLogTerm := rf.logs[mappedPrevLogIndex].Term
				logAppendRequest.PrevLogIndex = prevLogIndex
				logAppendRequest.PrevLogTerm = prevLogTerm

				if lastIndex >= logAppendRequest.PrevLogIndex+1 {
					logAppendRequest.Entries = make([]LogEntry, lastIndex-logAppendRequest.PrevLogIndex)
					copy(logAppendRequest.Entries, rf.logs[mappedPrevLogIndex+1:])
				}

				logAppendRequestCopy := logAppendRequest
				DPrintf("Goroutine: %v, rf [Leader] %v, logs: %v, sends AppendEntry args %v to follower %v, at term %v \n, time :%v", GetGOId(), rf.me, rf.logs, logAppendRequest, i, rf.currentTerm, time.Now().UnixMilli())
				go rf.sendAppendEntries(i, &logAppendRequestCopy, &logAppendReplies[i], logAppendReplyChan, lastIndex+1)
			}
		}
		logAppendRequest.Entries = nil
	}

	// 更新Leader本身的nextIndex以及matchIndex.
	rf.nextIndex[rf.me] = lastIndex + 1
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	rf.mu.Unlock()

	LostConnectionCnt := 0
	reSendLogCnt := 0

	successCnt := 1

	logAppendResultMap := make(map[int]AppendEntriesReplyWithPeerID)
	snapShotAppendResultMap := make(map[int]SnapShotReplyWithPeerID)

	// 这里需要等待 其余节点
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case logAppendResp := <-logAppendReplyChan:
			rf.mu.Lock()
			logAppendResultMap[i] = logAppendResp

			DPrintf("Goroutine: %v, %v rf received logAppendResponse from rf:%v, logAppendResp: %v, total reply %v, time :%v ", GetGOId(), rf.me, logAppendResp.server, logAppendResp, logAppendResultMap, time.Now().UnixMilli())

			// 如果自身的Term发生了变化,那么直接退出，因为当前的goroutine栈已经失效，不可以修改Raft的状态.
			if currentTerm < rf.currentTerm {
				DPrintf("Goroutine: %v, %v rf Leader term changed from %v to %v at time %v", GetGOId(), rf.me, currentTerm, rf.currentTerm, time.Now().UnixMilli())
				defer rf.mu.Unlock()
				return
			}

			// 如果从Peer返回了的比当前Term更大的Term,那么当前节点变化身份从Leader->Follower.

			if logAppendResp.reply.Term > currentTerm {
				DPrintf("Goroutine %v, %v rf changed role to Follower, term changed from rf.currentTerm: %v to logAppendResp.reply.Term: %v at time %v", GetGOId(), rf.me, rf.currentTerm, logAppendResp.reply.Term, time.Now().UnixMilli())
				rf.role = Follower
				rf.voteFor = Nobody
				rf.currentTerm = logAppendResp.reply.Term
				rf.reElectionForFollower = false
				rf.mu.Unlock()
				return
			}

			// 如果自身的Role发生了变化，直接退出，当前栈已经失效,不可以修改Raft的状态。
			if rf.role != Leader {
				DPrintf("Goroutine: %v, %v rf Leader role changed from Leader to %v at time %v", GetGOId(), rf.me, rf.role, time.Now().UnixMilli())
				defer rf.mu.Unlock()
				return

			}

			if logAppendResp.reply.Term == LostConnection {
				LostConnectionCnt++
				DPrintf("Goroutine: %v, %v rf received timeout.\n", GetGOId(), rf.me)

				if LostConnectionCnt >= len(rf.peers)/2+1 {
					defer rf.mu.Unlock()
					rf.processNetworkFailure(AppendRequest)

					DPrintf("Goroutine: %v, %v rf , currentTerm: %v, currentTerm: %v", GetGOId(), rf.me, rf.currentTerm, currentTerm)

					return
				} else {
					rf.mu.Unlock()
					continue
				}
			} else {
				// 有节点成功进行了commit。
				if logAppendResp.reply.Success {
					successCnt++

					// The logs has committed on the majority servers.
					if successCnt >= len(rf.peers)/2+1 {
						defer rf.mu.Unlock()

						rf.updateLeaderCommitIndex()
						// 将最新的日志进行持久化
						rf.persist()

						if rf.lastApplied < rf.lastSnapshotIndex {
							return
						}

						logsCp := append([]LogEntry(nil), rf.logs...)

						commitIndex := rf.getMappedIndex(rf.commitIndex)
						appliedIndex := rf.getMappedIndex(rf.lastApplied)

						lastSnapshotIndex := rf.lastSnapshotIndex

						DPrintf("Goroutine: %v, leader %v rf apply logs %v from %v to %v", GetGOId(), rf.me, logsCp, appliedIndex, commitIndex)

						go rf.applyLogs(commitIndex, appliedIndex, logsCp, rf.currentTerm, lastSnapshotIndex)

						rf.lastApplied = rf.commitIndex

						return
					}
					rf.mu.Unlock()
				} else {
					// 失败只有两种情况，第一需要重新传输数据,此时失败标记为Resend，第二，节点返回中出现了更大的Term.
					// 判断是否是因为需要重新发送日志而失败。
					if logAppendResp.reply.Term == ReSend {
						reSendLogCnt++
						DPrintf("Goroutine: %v, %v rf Leader need to resend log to rf %v, index start at: %v at term %v, at time :%v, value reSendLogCnt now: %v", GetGOId(), rf.me, logAppendResp.server, rf.nextIndex[logAppendResp.server], rf.currentTerm, time.Now().UnixMilli(), reSendLogCnt)

						// 如果超过大部分都要求重新传送数据，那么此时需要连同Leader的commitIndex也需要更新。
						if reSendLogCnt >= len(rf.peers)/2+1 {
							defer rf.mu.Unlock()
							rf.updateLeaderCommitIndex()
							return
						}
						rf.mu.Unlock()
						continue
					}
					rf.mu.Unlock()
				}
			}
		// snapshot是已经committed的数据压缩。
		case snapShotAppendResp := <-snapShotAppendReplyChan:

			rf.mu.Lock()
			snapShotAppendResultMap[i] = snapShotAppendResp

			DPrintf("Goroutine: %v, %v rf received logAppendResponse from rf:%v, snapShotAppendResp: %v, total reply %v, time :%v ", GetGOId(), rf.me, snapShotAppendResp.server, snapShotAppendResp, snapShotAppendResultMap, time.Now().UnixMilli())

			if snapShotAppendResp.reply.Term == LostConnection {
				LostConnectionCnt++
				DPrintf("Goroutine: %v, %v rf received timeout.\n", GetGOId(), rf.me)

				if LostConnectionCnt >= len(rf.peers)/2+1 {
					defer rf.mu.Unlock()
					rf.processNetworkFailure(AppendRequest)

					DPrintf("Goroutine: %v, %v rf , currentTerm: %v, currentTerm: %v", GetGOId(), rf.me, rf.currentTerm, currentTerm)

					return
				} else {
					rf.mu.Unlock()
					continue
				}
			}

			if snapShotAppendResp.reply.Term > currentTerm {
				DPrintf("Goroutine %v, %v rf changed role to Follower, term changed from rf.currentTerm: %v to logAppendResp.reply.Term: %v at time %v", GetGOId(), rf.me, rf.currentTerm, snapShotAppendResp.reply.Term, time.Now().UnixMilli())
				rf.role = Follower
				rf.voteFor = Nobody
				rf.currentTerm = snapShotAppendResp.reply.Term
				rf.reElectionForFollower = false
				rf.mu.Unlock()
				return
			}
			// 这里也算作成功append数据的一次请求。
			successCnt++
			// The logs has committed on the majority servers.
			if successCnt >= len(rf.peers)/2+1 {
				defer rf.mu.Unlock()
				rf.updateLeaderCommitIndex()
				// 将最新的日志进行持久化
				rf.persist()

				logsCp := make([]LogEntry, len(rf.logs))
				copy(logsCp, rf.logs)

				if rf.lastApplied < rf.lastSnapshotIndex {
					return
				}

				if rf.lastApplied < rf.commitIndex {
					commitIndex := rf.getMappedIndex(rf.commitIndex)
					appliedIndex := rf.getMappedIndex(rf.lastApplied)
					lastSnapshotIndex := rf.lastSnapshotIndex

					DPrintf("Goroutine: %v, leader %v rf apply logs %v from %v to %v", GetGOId(), rf.me, logsCp, appliedIndex, commitIndex)

					go rf.applyLogs(commitIndex, appliedIndex, logsCp, rf.currentTerm, lastSnapshotIndex)

					rf.lastApplied = rf.commitIndex
				}

				return
			}
			rf.mu.Unlock()
		}

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {

	// We recommend using a
	// conservative election timeout such as 150–300ms; such time-
	// outs are unlikely to cause unnecessary leader changes and
	// will still provide good availability.
	// 如果选举时长太短，会导致过多的选举产生。

	// 如果heartbeatTimeout比较小，会导致日志可能重复发送。
	heartbeatTimeoutV := 40

	for !rf.killed() {
		r1 := rand.New(rand.NewSource(time.Now().UnixMilli()))
		rf.mu.Lock()
		role := rf.role
		DPrintf("Loop Check, Goroutine: %v,%v rf current role : %v at term %v, logs' length: %v, logs: %v, time : %v", GetGOId(), rf.me, role, rf.currentTerm, len(rf.logs), rf.logs, time.Now().UnixMilli())
		switch role {
		case Leader:
			rf.mu.Unlock()
			<-time.After(time.Duration(heartbeatTimeoutV)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(10)+int32(rf.me)*5))
			// 发起一次信息同步或者数据传输
			go rf.LeaderAppendEntries(nil)

		case Follower:
			rf.mu.Unlock()

			<-time.After(time.Duration(time.Duration(rf.electionTimeoutV)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(50)+int32(rf.me)*5)))

			rf.mu.Lock()
			// 选举周期内没有被阻止发起选举。
			if rf.reElectionForFollower {
				DPrintf("Goroutine: %v, %v rf changed to Candidate now ,term %v, at time %v", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
				rf.role = Candidate
			} else {
				// 选举周期内被阻止发起选举。
				rf.reElectionForFollower = true
			}
			rf.mu.Unlock()

		case Candidate:
			rf.mu.Unlock()
			rf.timers[rf.me] = int(time.Now().UnixMilli())
			// 发起选举
			go rf.launchElection()

			rf.mu.Lock()

			rf.commitIndex = 0
			rf.lastApplied = 0

			rf.mu.Unlock()
			// 每隔一段时间Candidate会重新发起选举。
			<-time.After(time.Duration(time.Duration(rf.electionTimeoutV/3)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(50)+int32(rf.me)*5)))

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

	DPrintf("New Raft %v at time %v", rf.me, time.Now().UnixMilli())
	// All elections are launched by the ticker.
	//rf.reElectionForFollower = true

	rf.timers = make([]int, len(rf.peers))

	rf.minimumElectionTimeDelta = 150

	rf.initializeChannels()

	// 当创建Raft使，logs中就有一条LogEntry.
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// 当Raft重新启动时，会从persist中读取之前保留的状态。
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.electionTimeoutV = 150
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.initializeNextIndexAndMatchIndex()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
