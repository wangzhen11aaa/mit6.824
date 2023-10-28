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

	// 一个标记，当是数据重传模式时，置为true
	resendModeMap map[int]interface{}

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

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	if rf.role == Leader {
		e.Encode(rf.logs[0 : rf.commitIndex+1])
	} else {
		e.Encode(rf.logs)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		DPrintf("Error")
	} else {
		DPrintf("Goroutine: %v, %v rf readPersist currentTerm: %v, voteFor: %v, logs: %v at term %v, time %v", GetGOId(), rf.me, currentTerm, voteFor, logs, rf.currentTerm, time.Now().UnixMilli())
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
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

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *AppendEntriesReply, result chan AppendEntriesReplyWithPeerID, nextIndexToCommit int) bool {
	// DPrintf("Goroutine %v Leader %v sendAppendEntries logs :%v to server %v at term %v, time: %v", GetGOId(), rf.me, args.Entries, server, rf.currentTerm, time.Now().UnixMilli())
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("Goroutine %v ,%v rf Leader got the AppendEntries result from  %v, status: %v \n", GetGOId(), rf.me, server, reply.Success)
		// 如果Follower响应. 把nextIndex的更新放在这里的原因是，因为调用者会在收集到Majority的success之后会直接退出，导致对应的栈不能使用，从而后面的响应无法进行更新。然而，sendAppendEntries是独立的栈，可以对后面返回的结果进行更新。
		rf.mu.Lock()
		if reply.Success {
			if nextIndexToCommit > rf.nextIndex[server] {
				DPrintf("Goroutine %v, %v rf Leader (UpdateNextIndexAndMatchIndex Method), got the AppendEntries from peer %v, status: %v, nextIndexToCommit: %v \n", GetGOId(), rf.me, server, reply.Success, nextIndexToCommit)
				// 如果Peer响应成功，那么就可以更新matchIndex和nextIndex数组。
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
		rf.mu.Unlock()
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
		if rf.commitIndex == 0 {
			if len(rf.logs) > 0 {
				reply.Term = rf.logs[0].Term
			} else {
				// Raft Server的日志第一条Log的Term不一定是1,因为本地没有日志，所以就返回1。
				reply.Term = 1
			}
		} else {
			reply.Term = rf.logs[rf.commitIndex-1].Term
		}
	} else {
		reply.Term = rf.currentTerm
	}
}

// If the consistLogCheck failed, return false.(This makes the leader reduce the nextIndex for this server)
func (rf *Raft) consistLogCheck(args *RequestAppendEntriesArgs) bool {
	//Check whether the logs have been appended are the same with the leader. Reduction Property.
	DPrintf("Goroutine: %v, %v rf  Begin consistLogCheck at term :%v at time %v, logs: %v, args: %v, rf.commitIndex %v, rf.lastApplied : %v", GetGOId(), rf.me, rf.currentTerm,
		time.Now().UnixMilli(), rf.logs, args, rf.commitIndex, rf.lastApplied)

	// 这些是特殊情况，表明当前的参数是由一个刚刚初始化的Leader发送的,这是非正常的AppendEntries Log序列。
	if len(args.Entries) != 0 && args.PrevLogIndex == 0 && args.Entries[0].Idx != 1 {
		DPrintf("Goroutine: %v,%v rf receive first Log from resumed Leader %v at term :%v at time %v, logs: %v", GetGOId(), rf.me, args.LeaderId, rf.currentTerm,
			time.Now().UnixMilli(), rf.logs)

		if rf.commitIndex >= args.Entries[0].Idx && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			rf.logs = rf.logs[:args.LeaderCommit-len(args.Entries)]
			rf.logs = append(rf.logs, args.Entries...)

			rf.persist()
			DPrintf("Goroutine: %v, %v rf  End AppendLog at term :%v at time %v, logs: %v", GetGOId(), rf.me, rf.currentTerm,
				time.Now().UnixMilli(), rf.logs)

			return true
		}
		// 如果当前Follower还没有数据，那么也不能接受这样的第一个Log
		return false
	}

	// 如果传送过来的args.PrevLogIndex < 0说明，这是全部的日志集合
	if args.PrevLogIndex < 0 {
		rf.logs = args.Entries
		rf.persist()
		return true
	}

	// 如果当前logs是空的。
	if len(rf.logs) == 0 {
		// 并且Leader传送过来的Entries是从起始来的。即args.PrevLogIndex == -1，并且args.Entries[0].Idx == 0
		// 那么可以直接赋值Logs到当前节点的rf.logs。
		if args.PrevLogIndex == 0 && args.Entries[0].Idx == 1 {
			rf.logs = append(rf.logs, args.Entries...)
			// 当有日志新增进来就进行persist().
			rf.persist()
			DPrintf("Goroutine: %v, %v rf End AppendLog at term :%v at time %v, logs: %v", GetGOId(), rf.me, rf.currentTerm,
				time.Now().UnixMilli(), rf.logs)
			return true
		} else {
			// 否则，还是缺少日志，需要Leader的nextIndex减去1，然后下个append周期重新发送。
			return false
		}
	}

	// 当出现上一个Append请求还未让Leader的nextIndex[i]生效，然而Leader的rf.commitIndex缺被更新时,
	// 那么prevLogIndex就不是Leader唤醒后第一次发送的-1，而是Leader的commitIndex-1,而nextIndex还是Leader唤醒时的
	// len(rf.logs)， 此时，判断日志的长度是多么重要。
	// 当args.PrevLogIndex没有在当前server的日志中时，需要Leader对本地已知的commit的最新Term进行全面补充。
	if len(rf.logs) != 0 && len(rf.logs)-1 < args.PrevLogIndex {
		DPrintf("Goroutine: %v, %v rf len(rf.logs)-1 :%v < args.PrevLogIndex: %v", GetGOId(), rf.me, len(rf.logs)-1, args.PrevLogIndex)
		return false
	}

	// 如果当前节点的下标的长度至少与Leader中已经committed长度相同。
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 如果args.Entries是空,那么直接返回。
		if len(args.Entries) == 0 {
			return false
		}
		DPrintf("Goroutine: %v, %v rf args.PrevLogTerm: %v != args.Entries[0].Term: %v, args.PrevLogIndex+1:%v != args.Entries[0].Idx: %v, at term: %v, time: %v ", GetGOId(), rf.me, args.PrevLogTerm, args.Entries[0].Term, args.PrevLogIndex+1, args.Entries[0].Idx, rf.currentTerm, time.Now().UnixMilli())
		return false
	} else {
		// 如果args.Entries是空,那么直接返回。
		if len(args.Entries) == 0 {
			return true
		}
		if rf.commitIndex < args.LeaderCommit {
			DPrintf("Goroutine: %v, %v rf Begin AppendLog at term :%v, rf.commitIndex: %v, at time %v, logs: %v, args: %v", GetGOId(), rf.me, rf.currentTerm, rf.commitIndex,
				time.Now().UnixMilli(), rf.logs, args)
			// e.g:
			// {4 0 0 1 [{4 2 no-op}] 3}
			// Leader发送过来的是已经commit的下一个元素。 Leader 视角的计算公式. rf.commitIndex(先于Follower的更新) + len(args.Entries) + 1 = args.LeaderCommit
			// Leader的commitIndex和Follower的commitIndex是在两个步骤内完成的，并且Follower的commitIndex是后于Leader的commitIndex而更新的。
			// Leader的 commitIndex 是需要Majority的Follower已经将这个位置复制到本地之后才会更新的。
			//
			// Leader发送过来的 args.Entries[0]是下一个位置，当前commitIndex+1.
			// 如果args.PrevLogIndex >= rf.commitIndex，说明本节点需要对 [rf.commitIndex, rf.PrevLogIndex]的数据进行apply并且，更新
			// args.commitIndex = args.PrevLogIndex+1.

			// 这里有一个问题，就是如果当前节点是一直领先于Majority节奏进行日志复制。那么会出现一种情况，就是args.PrevLogIndex+1 < args.Entries[0].Idx.那样的话会造成下面的rf.logs[:args.PrevLogIndex+1]数量变少。

			// Leader记录的此节点复制Leader日志的速度比Majority快，也就是说，当前节点有一些日志还没有被Majority复制成功。

			//1059 2023/10/27 23:10:09 Goroutine: 344, 4 rf Begin AppendLog at term :3, rf.commitIndex: 1, at time 1698419409282, logs: [{1 0 no-op} {1 1 1886} {1 2 9142} {1 3 8387}], args: &{3 1 2 1 [{3 4 no-op} {3 5 1870}] 6}
			// 说明本地commit的速度超过了Majority.
			if args.PrevLogIndex+1 < args.Entries[0].Idx {
				rf.logs = rf.logs[:args.Entries[0].Idx]
				rf.logs = append(rf.logs, args.Entries...)
			} else {
				// Leader发送时保证args.PrevLogIndex+1 > args.Entries[0].Idx的情况不会发生。
				if args.PrevLogIndex+1 > args.Entries[0].Idx {
					DPrintf("not correct")
				}
				rf.logs = rf.logs[:args.PrevLogIndex+1]
				rf.logs = append(rf.logs, args.Entries...)
			}

			// 当有日志新增进来就进行persist().
			rf.persist()
		}
		DPrintf("Goroutine: %v,%v rf End AppendLog at term :%v at time %v, logs: %v", GetGOId(), rf.me, rf.currentTerm,
			time.Now().UnixMilli(), rf.logs)

		return true
	}

}

func (rf *Raft) remainFollower(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("Goroutine %v, %v rf [remainFollower] response to %v at term %v, time :%v \n", GetGOId(), rf.me, args.LeaderId, args.Term, time.Now().UnixMilli())
	rf.reElectionForFollower = false
	rf.voteFor = args.LeaderId
	rf.latestSuccessfullyAppendTimeStamp = time.Now().UnixMilli()

	DPrintf("Goroutine %v, %v rf before consistLogCheck: %v ", GetGOId(), rf.me, rf.logs)
	// try Append logs
	reply.Success = rf.consistLogCheck(args)
	DPrintf("Goroutine %v, %v rf after consistLogCheck: %v ", GetGOId(), rf.me, rf.logs)
	if !reply.Success {
		// 如果日志检测失败，那么Follower需要让Leader从本地需要的commitIndex位置开始发送。
		if rf.commitIndex == 0 {
			// 特殊情况
			// 如果本地还没有顺利commit过，那么说明本地的Log只有从头开始重新传输，因为可能这些没有commit的本地Log有旧数据。
			// Leader收到之后，会查找到比Term大的最小Log,然后尝试传输给Follower.
			reply.Term = 0
		} else {
			// 如果已有过commitIndex，那么就传送给Leader已知的最大已经committed的Term.
			reply.Term = rf.logs[rf.commitIndex].Term
		}
	} else {
		reply.Term = rf.currentTerm
	}
	DPrintf("Goroutine: %v, %v rf [remainFollower] ended response to %v, reply: %v at term %v, time :%v \n", GetGOId(), rf.me, args.LeaderId, reply, args.Term, time.Now().UnixMilli())

}

func (rf *Raft) applySyncWithLeader(args *RequestAppendEntriesArgs, lastApplied int, commitIndex int, logs []LogEntry, currentTerm int) {
	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()
	var i int
	for i = lastApplied + 1; i < commitIndex+1; i++ {

		rf.applyCh <- ApplyMsg{CommandValid: false, Command: logs[i].Cmd, CommandIndex: i}
		DPrintf("Goroutine:%v, %v rf apply Log(cmd: %v), index: %v at term %v, at time %v", GetGOId(), rf.me, logs[i].Cmd, i+1, currentTerm, time.Now().UnixMilli())
	}
	DPrintf("Goroutine:%v, [Follower]%v rf applySyncWithLeader End, lastApplied changed from %v to %v at term %v, at time %v", GetGOId(), rf.me, lastApplied, i, currentTerm, time.Now().UnixMilli())

}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Goroutine: %v, in AppendEntries %v rf, rf.lastApplied: %v, rf.commitIndex: %v logs: %v, args: %v, term:%v, time:%v", GetGOId(), rf.me, rf.lastApplied, rf.commitIndex, rf.logs, args, rf.currentTerm, time.Now().UnixMilli())

	// AppendEntires rule 1.Reply false if term < current Term
	// 如果当前的Term 比 Leader发送过来的Term大，那么就返回自己这边比较大的Term给Leader.
	if rf.currentTerm > args.Term {
		DPrintf("Goroutine: %v, %v rf returned false because of %v < %v at term %v at time:%v", GetGOId(), rf.me, args.Term, rf.currentTerm, rf.currentTerm, time.Now().UnixMilli())
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

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

	// 如果Append Log校验失败,那么直接返回，不能apply这些logs，因为这里面可能有过期的log.
	if !reply.Success {
		DPrintf("Goroutine: %v, %v rf will not apply logs (for append failed) between[rf.appliedIndex+1:%v, args.PrevLogIndex:%v ]at term %v at time:%v", GetGOId(), rf.me, rf.lastApplied+1, args.PrevLogIndex+1, rf.currentTerm, time.Now().UnixMilli())

		return
	}

	// Figure 2, Rules for Servers
	// If the commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
	// 有三种情况，
	// 1.rf.commitIndex < args.PrevLogIndex+1:在Follower不断从Leader那边获取日志的时候,就是这种情况。说明有新的需要committed的Log被apply.
	// 2.rf.commitIndex >= args.PrevLogIndex+1的 应该是比较少见的。即使是Follower的本地log需要Overwrite,也是从Leader最少的Log就可以，没有必要让args.PrevLogIndex回退超过Follower的rf.commitIndex-1的位置。
	if rf.commitIndex < args.PrevLogIndex {
		DPrintf("Goroutine: %v, %v rf will apply logs between[rf.commitIndex:%v, args.PrevLogIndex:%v) at term %v at time:%v", GetGOId(), rf.me, rf.commitIndex, args.PrevLogIndex, rf.currentTerm, time.Now().UnixMilli())
		rf.commitIndex = args.PrevLogIndex
		// applySyncWithHeader是同步操作，时携带Lock的，这把Lock是锁Raft本身的,因为这个同步操作可能会比较耗时，所以在这里持有锁做这个事情，明显事不对的。
		// 为了避免sync上出现out of order的情况，我们需要另外一把锁，专门用来同步apply的操作，但是不影响Raft其他的逻辑。

		// 将当前rf.logs复制出一份
		logs := append([]LogEntry(nil), rf.logs...)
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex

		go rf.applySyncWithLeader(args, lastApplied, commitIndex, logs, rf.me)

		rf.lastApplied = rf.commitIndex

	}

}

// vote log consist check.
// 选举时，需要比较候选人和当前节点的日志，哪个更新。
// 如果候选人的日志更新，那么就支持, 返回true。
// 如果候选人的日志没有当前节点的日志新，那么就返回false。
func (rf *Raft) consistLogCheckForVote(args *RequestVoteArgs) bool {
	DPrintf("Goroutine: %v, %v rf consistLogCheckForVote, rf.logs: %v,args: %v at term %v, time :%v ", GetGOId(), rf.me, rf.logs, args, rf.currentTerm, time.Now().UnixMilli())

	if len(rf.logs) == 0 {
		return true
	} else {
		// 这里需要判断(不必是同样的index下的Log).
		// 如果 本地的 rf.logs的最后一个位置p0的Term > 候选人传递过来的最后一个位置p1的Term,那么说明候选人那边的日志有无效的写入.
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
			return false
		} else if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm {
			// 如果 本地的 rf.logs的最后位置p0中的Log的Term 与 候选人传递过来的最后位置p1的Term相同，那么谁logs长度长，谁为Leader.
			if len(rf.logs)-1 <= args.LastLogIndex {
				return true
			} else {
				return false
			}
		} else {
			return true
		}
	}

}

// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Goroutine: %v, %v rf in requestVote, role: %v, rf.logs:%v, rf.voteFor :%v, args: %v at term %v, time: %v", GetGOId(), rf.me, rf.role, rf.logs, rf.voteFor, args, rf.currentTerm, time.Now().UnixMilli())

	// RequestVote rule 1.Reply false if term < currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false

		// 返回的Term等于较大的Term。
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

		// 返回的Term等于较大的Term。
		rf.currentTerm = args.Term

		if rf.consistLogCheckForVote(args) {
			DPrintf("Goroutine: %v, %v rf [current role: %v to Follower]: currentTerm changed from %v to %v, in RequestVote for the coming request %v rf, args: %v, consistLogCheckForVote Result: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, true, time.Now().UnixMilli())

			rf.role = Follower
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			rf.reElectionForFollower = false
		} else {
			DPrintf("Goroutine: %v, %v rf [current role: %v unchanged]: currentTerm changed from %v to %v, in RequestVote for the coming request %v rf, args: %v, consistLogCheckForVote Result: %v at time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, args.Term, args.CandidateId, args, false, time.Now().UnixMilli())
			reply.VoteGranted = false
			// 因为Candidate's Term 大于 currentTerm,但是Log没有本地全，所以，抓紧让本地节点进行选举Leader。
			rf.role = Follower
			rf.reElectionForFollower = true
			rf.voteFor = Nobody
		}
		// If raft node follows leader, then this request suppress follower's election.
		return
	}

	// RequestVote RPC rule for receiver: 2.0 If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
	reply.Term = rf.currentTerm

	if rf.voteFor == Nobody || rf.voteFor == args.CandidateId {
		if len(rf.logs)-1 < 0 || args.LastLogIndex >= 0 && (len(rf.logs)-1 < args.LastLogIndex || len(rf.logs)-1 == args.LastLogIndex && rf.logs[args.LastLogIndex].Term <= args.LastLogTerm) {
			DPrintf("Goroutine:%v, %v rf role changed from %v to %v, local logs: %v, args: %v", GetGOId(), rf.me, rf.role, Follower, rf.logs, args)
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
	//rf.mu.Lock()
	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Idx: len(rf.logs), Cmd: command})
		rf.mu.Unlock()
		DPrintf("Method Start() Append cmd %v to Leader at index %v,at time %v", command, index, time.Now().UnixMilli())
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
	replyLargestTerm := -1
	lostConnectionCnt := 1
	voted := 1

	// channel for vote
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-voteChannel
		rf.mu.Lock()
		DPrintf("Goroutine: %v,%v rf requestVote receive %v, at term %v, time :%v", GetGOId(), rf.me, reply, rf.currentTerm, time.Now().UnixMilli())

		// 如果当前节点自身的Term有变化,说明currentTerm所在的选举已经失效。不改变当前的Raft的任何状态,
		// 直接退出。
		if currentTerm != rf.currentTerm {
			DPrintf("Goroutine: %v, %v rf Leader Term changed from %v to %v , at time %v", GetGOId(), rf.me, currentTerm, rf.currentTerm, rf.currentTerm)
			defer rf.mu.Unlock()
			return
		}

		// 如果rf.role有变化,不更改当前Raft的任何状态，直接返回。
		if rf.role != Candidate {
			DPrintf("Goroutine: %v, %v rf changed role from 1 to %v at term : %v, time: %v", GetGOId(), rf.me, rf.role, rf.currentTerm, time.Now().UnixMilli())
			defer rf.mu.Unlock()
			return
		}

		// 网络问题，拿不到Peer的返回结果,所以不需要包含Term的大小对比。
		// 这一种情况肯定是最后从channel中获取，因为耗时太长。
		if reply.Term == LostConnection {
			lostConnectionCnt++

			if lostConnectionCnt >= len(rf.peers)/2+1 {

				defer rf.mu.Unlock()
				rf.processNetworkFailure(VoteRequest)
				return

			}
		} else {
			// Peer正常返回结果。因为reply.Term > currentTerm肯定意味着reply.VoteGranted等于false。
			if reply.VoteGranted {

				// Count the successful votes
				voted++
				// 如果选举个数超过一半，那么就可以更新Raft状态，并退出。
				if voted >= len(rf.peers)/2+1 {
					rf.processMajoritySuccessVoteRequest()
					return
				}

			} else {
				if replyLargestTerm < reply.Term {
					replyLargestTerm = reply.Term
				}
				// 如果返回失败，并且失败原因是遇到了更大的Term,那么需要更改当前Raft的状态。
				if replyLargestTerm > rf.currentTerm {
					defer rf.mu.Unlock()
					rf.processFailedRequest(VoteRequest, replyLargestTerm)
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

// Process the failed request condition.
// 如果选举过程中遇到了更大的Term,那么当前节点身份变为Follower。此时并不更新当前Raft的currentTerm,
// 当前节点的currentTerm需要在后续选举其他Candidate为Leader时，更新。
func (rf *Raft) processFailedRequest(typ int, replyLargestTerm int) {

	DPrintf("Goroutine %v, %v rf changed to Follower", GetGOId(), rf.me)
	if typ == VoteRequest {
		if rf.role == Candidate {
			// Rules for Candidate: $5.2
			// If AppendEntries RPC received from new leader: convert to follower.

			if replyLargestTerm > rf.currentTerm {
				DPrintf("[tryBecomeFollower supported by Minority] %v 's [current role: %v term : %v to Follower]: Term replied : %v at time: %v", rf.me, rf.role, rf.currentTerm, replyLargestTerm, time.Now().UnixMilli())
			}

			DPrintf("Goroutine: %v, %v rf Candidate-> Follower at term %v, time :%v", GetGOId(), rf.me, rf.currentTerm, time.Now().UnixMilli())
			//rf.voteSuccessChannel <- MinoritySupport
			time.Sleep(time.Millisecond * 150)

			rf.reElectionForFollower = false
		}
	}
	rf.role = Follower
	rf.voteFor = Nobody
}

// Process the success append entries request condition.
func (rf *Raft) processMajoritySuccessAppendRequest(commitIndex int, lastApplied int, logs []LogEntry, currentTerm int) { ///.rejectServer map[int]interface{}, approveServer map[int]interface{}) {

	// When the majority appended successfully, we update local commitIndex for the next logs to append.
	// The leader can apply these logs safely. logs[leaderCommitStartIndex, localCommitStartIndex)
	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()
	DPrintf("Goroutine: %v, %v rf[Leader] begin apply logs: %v  at time %v, rf.lastApplied :%v, rf.commitIndex: %v", GetGOId(), rf.me, logs, time.Now().UnixMilli(), lastApplied, commitIndex)
	for i := lastApplied + 1; i < commitIndex+1; i += 1 {

		rf.applyCh <- ApplyMsg{CommandValid: false, Command: logs[i].Cmd, CommandIndex: i}
		DPrintf("Goroutine:%v, %v rf apply Log(cmd: %v), index: %v at term %v, at time %v", GetGOId(), rf.me, logs[i].Cmd, i+1, currentTerm, time.Now().UnixMilli())
	}

	DPrintf("Goroutine:%v,[Leader] %v rf apply Log End, rf.lastApplied changed from %v to %v at term %v, at time %v", GetGOId(), rf.me, lastApplied, commitIndex, currentTerm, time.Now().UnixMilli())

	// rf.lastApplied = rf.commitIndex

}

// Update leader commit index.
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

	for server, k := range rf.matchIndex {
		if server != rf.me {
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
	}
	DPrintf("Goroutine: %v, %v rf Commit index in updateLeaderCommitIndex cnt: %v", GetGOId(), rf.me, cnt)

	// Select the N in the cnt map.
	// 使用map采用计数的方式不可行。这里需要找到一个K，这个K>=map中的k的最大值，并且这个值的数量大于len(rf.peers).
	// map[0:2, 1:1, 2:1]这里面得到的K应该是1.而不是0.
	var n int
	n = -1
	for k, v := range cnt {
		DPrintf("Goroutine :%v, k: %v, v:%v \n", GetGOId(), k, v)
		if v >= (len(rf.peers) / 2) {
			if k > n {
				n = k
			}
		}
	}

	DPrintf("Goroutine: %v, %v rf Leader updateLeaderCommitIndex rf.commitIndex changed from %v to %v, rf.matchIndex: %v, rf.nextIndex: %v", GetGOId(), rf.me, rf.commitIndex, n, rf.matchIndex, rf.nextIndex)
	rf.commitIndex = n

}

// Process the success vote request condition.
func (rf *Raft) processMajoritySuccessVoteRequest() {
	rf.role = Leader
	DPrintf("Goroutine: %v, %v rf Leader now at term %v, cost time %v", GetGOId(), rf.me, rf.currentTerm, int(time.Now().UnixMilli())-rf.timers[rf.me])
	rf.initializeIndexForFollowers()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = rf.nextIndex[i] - 1
		}
	}

	//rf.voteSuccessChannel <- MajoritySupport
	rf.mu.Unlock()
	// First insert a on-op log into leader.
	//go rf.LeaderAppendEntries("no-op")
	time.Sleep(time.Millisecond * 40)
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

func (rf *Raft) leaderAppendLogLocally(command interface{}) {
	rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Idx: len(rf.logs), Cmd: command})

	DPrintf("Goroutine:%v, %v rf Leader appended log  at term %v", GetGOId(), rf.me, rf.currentTerm)
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

// 返回小于term的最大元素
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
	// 使当前server变为resend模式.
	rf.resendModeMap[server] = true

	DPrintf("Goroutine: %v, %v rf, term: %v, logs: %v, followerExpectTerm: %v, commitIndex: %v", GetGOId(), rf.me, rf.currentTerm, rf.logs, followerExpectTerm, rf.commitIndex)

	if rf.nextIndex[server] == 0 {
		rf.nextIndex[server] = 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		return
	}
	// nextIndex数组中存储的是下一个需要发送的位置。所以直接拿rf.logs去访问理论上会出错的。
	if rf.logs[rf.nextIndex[server]-1].Term == followerExpectTerm {

		n := rf.nextIndex[server] - 1
		// 将rf.nextIndex[server]的下一个发送位置向前位置减一。
		// 使用二分法,[大于上一个Term的第一个元素位置,rf.nextIndex[server]-1]。设置rf.nextIndex为中间mid.
		left := rf.bfMinimumLargePos(followerExpectTerm)
		rf.nextIndex[server] = (left + rf.nextIndex[server]) / 2

		DPrintf("Goroutine: %v, %v rf, rf.nextIndex%v :changed from %v to %v term: %v, logs: %v, followerExpectTerm: %v", GetGOId(), rf.me, server, n, rf.nextIndex[server], rf.currentTerm, rf.logs, followerExpectTerm)

		rf.matchIndex[server] = rf.nextIndex[server] - 1
		return

	} else {
		// 下面二分查找，查找刚刚好>followerExpectTerm的第一个Log的位置。
		right := len(rf.logs)
		left := 0
		for left < right {
			mid := (left + right) / 2
			if rf.logs[mid].Term > followerExpectTerm {
				right = mid
			} else {
				// rf.logs[mid].Term <= followerExpectTerm
				left = mid + 1
			}
		}

		// If append log entries, update the corresponded index
		// 找到的位置是

		DPrintf("Goroutine: %v, %v rf, rf.nextIndex%v :changed from %v to %v term: %v, logs: %v, followerExpectTerm: %v", GetGOId(), rf.me, server, rf.nextIndex[server], left, rf.currentTerm, rf.logs, followerExpectTerm)

		rf.nextIndex[server] = left
		rf.matchIndex[server] = rf.nextIndex[server] - 1

	}
}

// Update nextIndex and matchIndex for this server.
func (rf *Raft) updateNextIndexAndMatchIndex(server int, nextIndexToCommit int) {

	DPrintf("Goroutine: %v, %v rf, rf.nextIndex%v :changed from %v to %v term: %v, logs: %v", GetGOId(), rf.me, server, rf.nextIndex[server], nextIndexToCommit, rf.currentTerm, rf.logs)

	rf.nextIndex[server] = nextIndexToCommit

	rf.matchIndex[server] = nextIndexToCommit - 1

}

func (rf *Raft) LeaderAppendEntries(command interface{}) int {
	rf.mu.Lock()

	if rf.role != Leader {
		defer rf.mu.Unlock()
		return InvalidPosition
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

			DPrintf("Goroutine: %v, %v rf in LeaderAppendEntries rf.nextIndex%v :%v, rf.commitIndex: %v, nextIndexToCommit: %v, at term %v time: %v, resendModeMap: %v", GetGOId(), rf.me, i, rf.nextIndex[i], rf.commitIndex, nextIndexToCommit, rf.currentTerm, time.Now().UnixMilli(), rf.resendModeMap)

			// PrevLogIndex 需要是在集群中已经committed的值,其中rf.commitIndex < rf.nextIndex中的值.
			// 当logs中没有值时，前一个Log是-1.
			//
			// 如果日志需要从某个Term的第一个Log开始重新传输到Follower时，此时会使所有Follower都要求重新传送.
			// 存在某种情况即，有少数的server需要传输更多的Log,因此导致rf.nextIndex[s] < rf.commitIndex，这些需要特殊考虑。
			// rq.PrevLogIndex 只等于rf.commitIndex与rf.nextIndex[i]中的较小者。
			// 比较特殊的一种情况是，当Leader crash重新启动时,Leader进行的第一步AppendEntries的需要一定失败，需要
			// Follower返回给Leader的信息，这个信息包含需要传送的信息的Term(这个Term一般等于 Peer的 rf.logs[rf.commitIndex-1].Term)。因为Follower的commitIndex更新时跟随Leader变化的，Leader变化大部分是跟随Majority的Follower而更新。
			// 一共存在三种组合:
			//  1. 当rq.PrevLogIndex < rf.commitIndex:
			//		 发送到的节点i 是落后于Majority数据的。因为rq.PrevLogIndex等于rf.nextIndex[i]-1 = server i 的commitIndex-1，所以发送给当前节点是正确的。
			//  2. 当rq.PrevLogIndex == rf.commitIndex:
			//		 发送到的节点i, 与Majority数据同步，所以是正确的。
			//  3. 当rq.PrevLogIndex > rf.commitIndex:
			//	  	 发送到的节点i,超前于Majority的数据，此时 请求中的rq.PrevLogIndex+1并不等于 args.Entries[0].Idx，但是没有关系，提前发送过去的Log并不会被apply。因为Follower的apply动作是由 args.PrevLogIndex决定的，apply的位置不会超过 args.PrevLogIndex。

			//  情况3存在一种特殊情况，即当Leader重启时，Leader的commitIndex是根据Follower的CommitIndex-1所在位置的Log的Term提供的。
			//  返回到Leader,我们会利用这个Term找到在Leader的Logs中比此Term大的最小元素的下标作为nextIndex需要传输的起始点,并用来更新matchIndex.
			//  根据matchIndex中的数值，我们找到某个最大的位置Pos，这个Pos刚刚好超过matchIndex中Majority的数量。
			//  此时，有一种情况,对于某一个server s，即rf.commitIndex < rf.nextIndex[s]; 即Leader的下一次传输，args.PrevLogIndex = rf.commitIndex-1,但是 Logs起始地址是从 rf.nextIndex[s]开始的，这样会让rf.logs[rf.commitIndex, rf.nextIndex[s]-1]的日志不能够重写。导致数据错误。

			// 如果数据属于重新传输的模式，那么args.PrevLogIndex和args.Entries[0].Idx必须是相邻的,不能跳跃。

			// 有一种极特殊的情况，就是将args.PrevLogIndex+1等于Follower的commitIndex的日志传送过去之后，本来是希望，通过Follower的返回值来更新Leader的commitIndex以及nextIndex的值，如果一切正常那么会正常运行下去，但是在Follower返回值到来之前，却又触发了一次心跳，那么Leader会利用上一次的commitIndex以及nextIndex来进行AppendLogs，此时Leader的commitIndex由于没有被更新，还是0，由此出现-1的argsPrevLogIndex又发送到Follower。这个Bug通过下面的代码，理论上可以修复，即在resend逻辑中，增加通过matchIndex来更新Leader commitIndex的逻辑。这样做会让后面的心跳触发的AppendEntries的逻辑会使用上Leader正确的commitIndex.

			if _, ok := rf.resendModeMap[i]; ok {
				rq.PrevLogIndex = rf.nextIndex[i] - 1
				if rq.PrevLogIndex == -1 {
					rq.PrevLogTerm = 0
				} else {
					rq.PrevLogTerm = rf.logs[rq.PrevLogIndex].Term
				}
				for idx := rq.PrevLogIndex + 1; idx < nextIndexToCommit; idx++ {
					rq.Entries = append(rq.Entries, rf.logs[idx])
				}
				delete(rf.resendModeMap, i)
			} else {
				if rf.nextIndex[i] < rf.commitIndex {
					rq.PrevLogIndex = rf.nextIndex[i] - 1
				} else if rf.nextIndex[i] > rf.commitIndex {
					rq.PrevLogIndex = rf.commitIndex
				} else {
					rq.PrevLogIndex = rf.commitIndex - 2
				}

				rq.PrevLogTerm = 0
				if rq.PrevLogIndex >= 0 {
					rq.PrevLogTerm = rf.logs[rq.PrevLogIndex].Term
				}
				for idx := rf.nextIndex[i]; idx < nextIndexToCommit; idx++ {
					rq.Entries = append(rq.Entries, rf.logs[idx])
				}
			}
			rqCopy := rq
			// 当新选为Leader并且从Persist中恢复了Logs时，发送给所有的集群其他节点都是此logs模式:{term, leader, -1, -1,[{term, rf.logs[len(rf.logs)], no-op},...]}]}
			// ...表示刚刚成为Leader的节点马上接受到了新的Append请求。
			// 为了等待Follower返回为false,并且希望更新commitIndex以及新的nextIndex值。
			// 如果到某个follower s 出现网络问题，那么Leader会重复以rf.commitIndex以及rf.nextIndex[s]进行重复发送。
			DPrintf("Goroutine: %v, rf [Leader] %v, logs: %v, sends AppendEntry args %v to follower %v, at term %v \n, time :%v", GetGOId(), rf.me, rf.logs, rqCopy, i, rf.currentTerm, time.Now().UnixMilli())
			go rf.sendAppendEntries(i, &rqCopy, &replies[i], resultChan, nextIndexToCommit)
		}
		rq.Entries = nil
	}

	// 记录当前的Term
	rf.mu.Unlock()
	LostConnectionCnt := 1
	successCnt := 1
	reSendLogCnt := 1
	// rejectServer := make(map[int]interface{})
	// approveServer := make(map[int]interface{})

	for i := 0; i < len(rf.peers)-1; i++ {

		resp := <-resultChan
		rf.mu.Lock()

		DPrintf("Goroutine: %v, %v rf received response from rf:%v, resp: %v, time :%v ", GetGOId(), rf.me, resp.server, resp, time.Now().UnixMilli())

		// 如果自身的Term发生了变化,那么直接退出，因为当前的goroutine栈已经失效，不可以修改Raft的状态.
		if currentTerm < rf.currentTerm {
			DPrintf("Goroutine: %v, %v rf Leader term changed from %v to %v at time %v", GetGOId(), rf.me, currentTerm, rf.currentTerm, time.Now().UnixMilli())
			defer rf.mu.Unlock()
			return InvalidPosition
		}

		// 如果自身的Role发生了变化，直接退出，当前栈已经失效,不可以修改Raft的状态。
		if rf.role != Leader {
			DPrintf("Goroutine: %v, %v rf Leader role changed from Leader to %v at time %v", GetGOId(), rf.me, rf.role, time.Now().UnixMilli())
			defer rf.mu.Unlock()
			return InvalidPosition

		}

		if resp.reply.Term == LostConnection {
			LostConnectionCnt++
			DPrintf("Goroutine: %v, %v rf received timeout.\n", GetGOId(), rf.me)

			if LostConnectionCnt >= len(rf.peers)/2+1 {
				defer rf.mu.Unlock()
				rf.processNetworkFailure(AppendRequest)

				DPrintf("Goroutine: %v, %v rf , currentTerm: %v, currentTerm: %v", GetGOId(), rf.me, rf.currentTerm, currentTerm)

				return leaderCommitStartIndex
			} else {
				rf.mu.Unlock()
				continue
			}
		} else {
			// 有节点成功进行了commit。
			if resp.reply.Success {
				successCnt++

				// The logs has committed on the majority servers.
				if successCnt >= len(rf.peers)/2+1 {
					defer rf.mu.Unlock()

					rf.updateLeaderCommitIndex()
					logs := append([]LogEntry(nil), rf.logs...)

					// 将最新的日志进行持久化
					rf.persist()
					go rf.processMajoritySuccessAppendRequest(rf.commitIndex, rf.lastApplied, logs, rf.currentTerm)

					rf.lastApplied = rf.commitIndex

					return leaderCommitStartIndex
				}
				rf.mu.Unlock()
			} else {
				// 失败只有两种情况，第一需要重新传输数据,此时失败标记为Resend，第二，节点返回中出现了更大的Term.
				// 判断是否是因为需要重新发送日志而失败。
				if resp.reply.Term == ReSend {
					reSendLogCnt++
					DPrintf("Goroutine: %v, %v rf Leader need to resend log to rf %v, index start at: %v at term %v, at time :%v, value reSendLogCnt now: %v", GetGOId(), rf.me, resp.server, rf.nextIndex[resp.server], rf.currentTerm, time.Now().UnixMilli(), reSendLogCnt)

					// 因为Leader重新选为Leader时,预期是所有连接的server，都会返回false,并且返回的Term要比请求的Term小，新的Leader会首先发送[Term', no-op],Term' 是要大于任何从Follower返回的Term的。如果由于网络问题，没有得到响应呢？
					// 如果超过大部分都要求重新传送数据，那么此时需要连同Leader的commitIndex也需要更新。
					if reSendLogCnt >= len(rf.peers)/2+1 {
						defer rf.mu.Unlock()
						rf.updateLeaderCommitIndex()

						return InvalidPosition
					}
					rf.mu.Unlock()
					continue
				}

				// 如果从Peer返回了的比当前Term更大的Term,那么当前节点变化身份从Leader->Follower.
				if resp.reply.Term > currentTerm {
					DPrintf("Goroutine %v, %v rf changed role to Follower, term changed from rf.currentTerm: %v to resp.reply.Term: %v at time %v", GetGOId(), rf.me, rf.currentTerm, resp.reply.Term, time.Now().UnixMilli())
					defer rf.mu.Unlock()
					rf.role = Follower
					rf.voteFor = Nobody
					rf.currentTerm = resp.reply.Term
					// 需要当前节点尽快去选新的Leader.
					rf.reElectionForFollower = true
					return InvalidPosition
				}
				rf.mu.Unlock()
			}
		}
	}

	return InvalidPosition
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {

	// We recommend using a
	// conservative election timeout such as 150–300ms; such time-
	// outs are unlikely to cause unnecessary leader changes and
	// will still provide good availability.
	// 如果选举时长太短，会导致过多的选举产生。
	electionTimeoutV := 1000
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
			rf.timers[rf.me] = int(time.Now().UnixMilli())
			go rf.launchElection()

			rf.mu.Lock()

			rf.commitIndex = 0
			rf.lastApplied = 0

			rf.mu.Unlock()
			<-time.After(time.Duration(time.Duration(electionTimeoutV/3)*time.Millisecond + time.Millisecond*time.Duration(r1.Int31n(50)+int32(rf.me)*5)))

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
			rf.matchIndex[i] = rf.nextIndex[i] - 1
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

	rf.resendModeMap = make(map[int]interface{})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	DPrintf("New Raft %v at time %v", rf.me, time.Now().UnixMilli())
	// All elections are launched by the ticker.
	//rf.reElectionForFollower = true

	rf.timers = make([]int, len(rf.peers))

	rf.minimumElectionTimeDelta = 150

	rf.latestSuccessfullyAppendTimeStamp = 0

	rf.rejectServer = make(map[int]interface{})
	rf.initializeChannels()

	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
