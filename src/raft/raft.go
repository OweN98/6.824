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
	"6.824/labgob"
	"bytes"
	"math"
	"math/rand"
	//	"bytes"
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

type State string

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

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

	// 2A
	currentTerm int
	votedFor    int
	state       State
	logEntry    []logEntry

	// election
	heartBeatTimeout time.Duration
	electionTimeout  time.Duration
	lastLiveTime     int64
	leaderID         int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	voteCount int
	applyCh   chan ApplyMsg

	lastIncludedIndex int
	lastIncludedTerm  int
}

type Command interface{}

type logEntry struct {
	Command Command
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 2C
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntry)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
// 2C
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntry = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

type installSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type installSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *installSnapshotArgs, reply *installSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if reply.Term > args.Term {
		return
	}
	// discard out of time snapshot
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	index := args.LastIncludedIndex
	temp := make([]logEntry, 0)
	temp = append(temp, logEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		temp = append(temp, rf.logEntry[i])
	}
	rf.logEntry = temp
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	DPrintf("[%d]Server %d installSnapshot from Leader %d, logslen=%d, rf.commitIndex=%d, rf.lastApplied=%d, rf.lastIncludedIndex=%d, rf.lastIncludedTerm=%d", rf.currentTerm, rf.me, rf.leaderID, len(rf.logEntry)-1, rf.commitIndex, rf.lastApplied, rf.lastIncludedIndex, rf.lastIncludedTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntry)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Data)
	mess := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.applyCh <- mess
	//rf.resetTime()
	return
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 2D
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 2D
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d Snapshot]Server %d Snapshot index: %d", rf.currentTerm, rf.me, index)

	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}
	for trim_index, entry := range rf.logEntry {
		if entry.Index == index {
			rf.lastIncludedIndex = index
			rf.lastIncludedTerm = entry.Term
			rf.logEntry = rf.logEntry[trim_index+1:]
			var temp = make([]logEntry, 1)
			rf.logEntry = append(temp, rf.logEntry...)
		}
	}

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntry)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) sendInstallSnapshot(server int, args *installSnapshotArgs, reply *installSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int) {
	if rf.state != LEADER {
		return
	}
	rf.mu.Lock()
	args := installSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	DPrintf("[%d]Leader %d sends snapshot to server %d, lastIncludedIndex: %d, lastIncludedTerm:%d", rf.currentTerm, rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()
	reply := installSnapshotReply{}
	for !rf.killed() && !rf.sendInstallSnapshot(server, &args, &reply) {
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.enFollower(reply.Term)
		rf.resetTime()
		rf.persist()
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int

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
	if rf.state == LEADER {
		return
	}
	CandiID := args.CandidateID
	//DPrintf("[%d] Server %d get RV from Candi %d.", rf.currentTerm, rf.me, CandiID)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("[%d] Server %d has larger term.", rf.currentTerm, rf.me)
		return
	}
	var lastindex int
	var lastTerm int
	if rf.getLastIndex() > 0 {
		lastindex = rf.logEntry[rf.getLastIndex()].Index
		lastTerm = rf.logEntry[rf.getLastIndex()].Term
	} else {
		lastindex = rf.lastIncludedIndex
		lastTerm = rf.lastIncludedTerm
	}
	DPrintf("[%d]args.lastIndex: %d, args.lastTerm: %d, lastIndex %d, lastTerm %d", rf.currentTerm, args.LastLogIndex, args.LastLogTerm, lastindex, lastTerm)
	CandiTerm := args.Term
	// The last log with larger term is more up-to-date
	if args.LastLogTerm < lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex < lastindex) {
		// Server is more up-to-date
		DPrintf("[%d] Server %d has more up-to-date logs than Candidate %d, reject voteRequest, reply %t", rf.currentTerm, rf.me, CandiID, reply.VoteGranted)
		return
	}
	// Candidate is mote up-to-date, keep on voting process
	if CandiTerm > rf.currentTerm {
		reply.VoteGranted = true
		reply.Term = CandiTerm
		rf.enFollower(CandiTerm)
		rf.votedFor = CandiID
		rf.resetTime()
		rf.persist()
		DPrintf("[%d]Server %d update term from term %d, vote for %d, reply %t", CandiTerm, rf.me, rf.currentTerm, CandiID, reply.VoteGranted)
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == CandiID) && rf.currentTerm == CandiTerm {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.enFollower(CandiTerm)
		rf.votedFor = CandiID
		rf.resetTime()
		rf.persist()
		DPrintf("[%d]Server %d, vote for %d, reply %t", rf.currentTerm, rf.me, CandiID, reply.VoteGranted)
	} else {
		// have voted
		if rf.votedFor != -1 && rf.votedFor != CandiID {
			DPrintf("[%d] Server %d,have voted candidate %d, reply %t", rf.currentTerm, rf.me, rf.votedFor, reply.VoteGranted)

		} else {
			DPrintf("[%d] Server %d, reject voteRequest from %d, reply %t", rf.currentTerm, rf.me, rf.votedFor, reply.VoteGranted)
		}
	}
	return
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

func (rf *Raft) goElection() {
	rf.mu.Lock()
	rf.enCandidate()
	DPrintf("[%d]Server %d start election.", rf.currentTerm, rf.me)
	rf.mu.Unlock()
	// send to every server
	for i := range rf.peers {
		if i != rf.me && !rf.killed() {
			rf.mu.Lock()
			term := rf.currentTerm
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLastLogTerm(),
			}
			if rf.state != CANDIDATE {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(i int) {
				reply := RequestVoteReply{}
				// if not successful, keep sending requests
				for !rf.killed() && !rf.sendRequestVote(i, &args, &reply) {
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if term != rf.currentTerm {
					return
				}
				if reply.Term > term {
					// find up-to-date term
					rf.enFollower(reply.Term)
					rf.persist()
					return
				}

				if reply.VoteGranted == true && rf.state == CANDIDATE && reply.Term == rf.currentTerm {
					rf.voteCount += 1
					if rf.voteCount*2 > len(rf.peers) && rf.leaderID == -1 {
						DPrintf("[%d]VoteCount: %d, peers_len = %d", rf.currentTerm, rf.voteCount, len(rf.peers))
						rf.enLeader()
						for j := 0; j < len(rf.peers); j++ {
							rf.nextIndex[j] = rf.getLastLogIndex() + 1
							rf.matchIndex[j] = rf.getLastLogIndex()
						}
						go rf.broadcastHeartBeat()
						return
					}
				}
			}(i)
		}
	}
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	LogInconsistency bool
	Next             int
}

// follower needs to tell the leader the next index to send logEntry
func (rf *Raft) getNextIndex() int {
	return len(rf.logEntry)
}

func (rf *Raft) getLastIndex() int {
	return len(rf.logEntry) - 1
}

func (rf *Raft) getLastLogIndex() int {
	if rf.getLastIndex() > 0 {
		return rf.logEntry[rf.getLastIndex()].Index
	} else {
		return rf.lastIncludedIndex
	}
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logEntry) > 1 {
		return rf.logEntry[rf.getLastIndex()].Term
	} else {
		return rf.lastIncludedTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]Server %d receive heartbeat from leader %d,args.PrevLogIndex: %d, args.PrevLogTerm: %d, args.LeaderCommit:%d, rf.getLastIndex(): %d, LastLogIndex: %d,  lastLogTerm: %d , logLength: %d", rf.currentTerm, rf.me, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.getLastIndex(), rf.logEntry[rf.getLastIndex()].Index, rf.logEntry[rf.getLastIndex()].Term, len(args.LogEntries))
	reply.Success = false
	reply.LogInconsistency = false
	reply.Term = rf.currentTerm
	// ae rpc / 1
	if args.Term < rf.currentTerm {
		DPrintf("[%d]Server:%d, argsTerm: %d < currentTerm:%d", rf.currentTerm, rf.me, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.enFollower(args.Term)
		rf.persist()
	}
	// Rules for servers / Candidates / 3
	if rf.state == CANDIDATE {
		rf.enFollower(args.Term)
		rf.leaderID = args.LeaderID
		rf.persist()
	}
	rf.leaderID = args.LeaderID
	rf.resetTime()

	if rf.lastIncludedIndex > args.PrevLogIndex {
		DPrintf("[%d]Server %d lastIncludedIndex %d > args.PrevLogIndex %d", rf.currentTerm, rf.me, rf.lastIncludedIndex, args.PrevLogIndex)
		reply.Next = rf.commitIndex + 1
		reply.LogInconsistency = true
		return
	}

	// ae rpc / 2
	if args.PrevLogIndex > rf.getLastLogIndex() {
		DPrintf("[%d]Server %d PrevLogIndex %d> Server %d lastIndex %d", rf.currentTerm, args.LeaderID, args.PrevLogIndex, rf.me, rf.logEntry[rf.getLastIndex()].Index)
		reply.LogInconsistency = true
		reply.Next = rf.commitIndex + 1
		return
	}
	// when failed, the nextIndex should be decreased until the Follower's logEntry match Leader's.
	// DPrintf("[%d]Server %d, %q, args.PrevLogIndex: %d, rf.getLastIndex(): %d, rf.logEntry[args.PrevLogIndex].Term: %d,  args.PrevLogTerm : %d", rf.currentTerm, rf.me, rf.state, args.PrevLogIndex, rf.getLastIndex(), rf.logEntry[args.PrevLogIndex].Term, args.PrevLogTerm)
	if args.PrevLogIndex-rf.lastIncludedIndex > 0 && args.PrevLogIndex <= rf.logEntry[rf.getLastIndex()].Index &&
		rf.logEntry[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		DPrintf("[%d]: Conflict in AppendEntries between leader %d and follower %d", args.Term, args.LeaderID, rf.me)
		//include the term of the conflicting entry and the first index it stores for that term.
		for i := args.PrevLogIndex - rf.lastIncludedIndex; i > 0 && i > rf.lastApplied-rf.lastIncludedIndex; i-- {
			if rf.logEntry[i].Term != rf.logEntry[args.PrevLogIndex-rf.lastIncludedIndex].Term {
				reply.Next = i + rf.lastIncludedIndex + 1
				break
			}
		}
		reply.LogInconsistency = true
		return
	}

	// ae rpc / 3

	for _, entry := range args.LogEntries {
		// entry conflicts
		if entry.Index-rf.lastIncludedIndex <= rf.getLastIndex() && entry.Term != rf.logEntry[entry.Index-rf.lastIncludedIndex].Term {
			rf.logEntry = rf.logEntry[:entry.Index-rf.lastIncludedIndex]
			DPrintf("[%d]SErver %d delete the entry %d and all after it ", rf.currentTerm, rf.me, entry.Index)
		}
	}

	rf.logEntry = append(rf.logEntry[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.LogEntries...)

	// ae rpc / 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.logEntry[rf.getLastIndex()].Index)))
		DPrintf("[%d]args.LeaderCommit: %d, lenofServerlog: %d, last new entry index: %d, Server %d commits commitIndex: %d", rf.currentTerm, args.LeaderCommit, len(rf.logEntry), rf.logEntry[rf.getLastIndex()].Index, rf.me, rf.commitIndex)

	}

	reply.Success = true
	reply.Next = rf.getLastLogIndex() + 1
	rf.persist()

	return
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	//DPrintf("[%d]Leader %d sends periodic heartbeat! HeartTimeout %s", rf.currentTerm, rf.me, rf.heartBeatTimeout)
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && !rf.killed() {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			DPrintf("rf.nextIndex[%d]-1 = %d, rf.lastIncludedIndex = %d", i, rf.nextIndex[i]-1, rf.lastIncludedIndex)
			if rf.nextIndex[i]-1 < rf.lastIncludedIndex {
				go rf.sendSnapShot(i)
				rf.mu.Unlock()
				continue
			}
			term := rf.currentTerm
			leader_id := rf.leaderID
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leader_id,
				LeaderCommit: rf.commitIndex,
				LogEntries:   nil,
			}
			// Rules for servers / Leader / 3
			next := rf.nextIndex[i]
			//DPrintf("[%d]Leader %d LeaderCommit %d ", rf.currentTerm, rf.me, rf.commitIndex)
			DPrintf("[%d]Leader %d send to %d; next: %d, args last index: %d, leaderlast: %d, loglen: %d", rf.currentTerm, rf.me, i, next, next-1, rf.logEntry[rf.getLastIndex()].Index, len(rf.logEntry)-1)
			args.PrevLogIndex = next - 1
			args.PrevLogTerm = rf.logEntry[args.PrevLogIndex-rf.lastIncludedIndex].Term
			if args.PrevLogTerm == 0 {
				args.PrevLogTerm = rf.lastIncludedTerm
			}
			if rf.getLastIndex() > 0 && next <= rf.logEntry[rf.getLastIndex()].Index {
				args.LogEntries = rf.logEntry[next-rf.lastIncludedIndex:]
				DPrintf("[%d]Leader %d sends entries from index %d to %d to server %d, len = %d", rf.currentTerm, rf.me, rf.nextIndex[i], rf.logEntry[rf.getLastIndex()].Index, i, len(args.LogEntries))
			}
			rf.mu.Unlock()
			go func(i int) {
				reply := AppendEntriesReply{}

				if !rf.killed() && !rf.SendAppendEntries(i, &args, &reply) {
				}
				if args.Term == term {
					rf.handleReply(i, args, reply)
				}
				//DPrintf("[%d]Server: %d, Leader: %d, old reply from Term %d", rf.currentTerm, i, rf.leaderID, args.Term)

			}(i)
		}
	}
}

func (rf *Raft) commitlogTicker() {
	for !rf.killed() {
		time.Sleep(time.Millisecond * 30)
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		//DPrintf("[%d]Server %d, %q, commit from %d to %d", rf.currentTerm, rf.me, rf.state, rf.lastApplied+1, rf.commitIndex)
		mess := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.logEntry[rf.getLastIndex()].Index {
			rf.lastApplied += 1
			command := rf.logEntry[rf.lastApplied-rf.lastIncludedIndex].Command
			index := rf.lastApplied
			mess = append(mess, ApplyMsg{
				SnapshotValid: false,
				CommandValid:  true,
				Command:       command,
				CommandIndex:  index,
			})
		}
		rf.mu.Unlock()
		for _, m := range mess {
			rf.applyCh <- m
			DPrintf("[%d]Server %d, %q, commit %d", rf.currentTerm, rf.me, rf.state, m.CommandIndex)
		}

	}
}

func (rf *Raft) handleReply(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// when leader finds its term out of date, reverts to Follower
		rf.enFollower(reply.Term)
		rf.persist()
		return
	}
	if reply.Success {
		//DPrintf("[%d]SUCCESS Leader %d to server %d", rf.currentTerm, rf.me, i)
		// Rules for Servers / Leader / 3.1
		rf.nextIndex[i] = reply.Next
		rf.matchIndex[i] = rf.nextIndex[i] - 1
		//rf.commitIndex = rf.lastIncludedIndex
		DPrintf("[%d]Success Leader %d to server %d, nextINdex[%d]:%d, commmitIndex %d, lastApplied %d", rf.currentTerm, rf.me, i, i, rf.nextIndex[i], rf.commitIndex, rf.lastApplied)

		// TODO commit
		for k := rf.commitIndex + 1 - rf.lastIncludedIndex; k <= rf.getLastIndex(); k++ {
			count_commit := 0
			if rf.logEntry[k].Term == rf.currentTerm {
				for server := range rf.peers {
					if rf.matchIndex[server] >= k {
						count_commit += 1
					}
				}
				if count_commit*2 > len(rf.peers) {
					//DPrintf("[%d]Leader %d Index %d Count commit: %d", rf.currentTerm, rf.me, k, count_commit)
					rf.commitIndex = k + rf.lastIncludedIndex
				} else {
					return
				}
			}
		}
		return
	} else if reply.LogInconsistency == true {
		////Rules for Leaders / 3
		if reply.Next > 0 {
			rf.nextIndex[i] = reply.Next
			rf.matchIndex[i] = rf.nextIndex[i] - 1
		} else {
			rf.nextIndex[i] = 1
			rf.matchIndex[i] = 0
		}
		DPrintf("[%d]LogInconsistency Leader %d to server %d, nextIndex[%d]:%d", rf.currentTerm, rf.me, i, i, rf.nextIndex[i])
	}
	return
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

// Start() make sense only on leader
// put the command into logs
// leader sends out AppendEntries RPCs
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//isleader: false if this server isn't the leader, client should try another
	//term: currentTerm, to help caller detect if leader is later demoted
	//index: log entry to watch to see if the command was committed
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	//if !rf.killed() {
	//	return index, term, isLeader
	//}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	state := rf.state
	if rf.getLastIndex() > 0 {
		index = rf.logEntry[rf.getLastIndex()].Index + 1
	} else {
		index = rf.lastIncludedIndex + 1
	}
	term = rf.currentTerm

	if state != LEADER {
		return index, term, isLeader
	} else {
		DPrintf("**********[%d]Leader %d called Start(), command %d**********", rf.currentTerm, rf.me, command)
		isLeader = true
		logentry := logEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logEntry = append(rf.logEntry, logentry)
		rf.nextIndex[rf.me] = len(rf.logEntry)
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
		rf.persist()
		//for p := range rf.peers {
		//	DPrintf("nextIndex[%d] : %d", p, rf.nextIndex[p])
		//}
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		state := rf.state
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		time.Sleep(timeout)
		if state == LEADER {
			continue
		} else {
			rf.mu.Lock()
			lasttimelive := rf.lastLiveTime
			state1 := rf.state
			rf.mu.Unlock()
			duration := time.Duration(time.Now().UnixNano() - lasttimelive)
			if duration > timeout && state1 != LEADER {
				//DPrintf("[%d]Server %d meets ElectionTimeout over %dms", rf.currentTerm, rf.me, duration/1e6)
				rf.goElection()
			}
		}
	}
}

func (rf *Raft) broadcastHeartBeat() {
	rf.mu.Lock()
	hbTimeout := rf.heartBeatTimeout
	rf.mu.Unlock()
	for rf.killed() == false {
		rf.sendHeartBeat()
		time.Sleep(hbTimeout)
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetTime() {
	rf.resetElectionTimeout()
	rf.lastLiveTime = time.Now().UnixNano()
}

func (rf *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(200)+300)
	//DPrintf("[%d]Server %d ElectionTimeout size resetted to %dms", rf.currentTerm, rf.me, rf.electionTimeout/1e6)

}

func (rf *Raft) enFollower(term int) {
	rf.state = FOLLOWER
	rf.leaderID = -1
	rf.votedFor = -1
	rf.voteCount = 0
	rf.currentTerm = term
	//DPrintf("[%d]Sever %d become the Follower.\n", rf.currentTerm, rf.me)
}

func (rf *Raft) enCandidate() {
	rf.state = CANDIDATE
	rf.leaderID = -1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.currentTerm += 1
	rf.resetTime()
	//DPrintf("[%d]Sever %d become the candidate.\n", rf.currentTerm, rf.me)
	rf.persist()
}

func (rf *Raft) enLeader() {
	rf.state = LEADER
	rf.votedFor = rf.me
	rf.leaderID = rf.me
	rf.voteCount = 0
	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.logEntry)
		rf.matchIndex[peer] = 0
	}

	DPrintf("[%d]Server %d become leader.", rf.currentTerm, rf.me)
	rand.Seed(time.Now().UnixNano())
	rf.heartBeatTimeout = time.Millisecond * time.Duration(rand.Intn(50)+100)
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
	rf.voteCount = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.leaderID = -1
	rf.logEntry = []logEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	// The tester requires that the leader send heartbeat RPCs no more than ten times per second.
	rf.heartBeatTimeout = time.Millisecond * time.Duration(rand.Intn(50)+100)
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(200)+300)
	rf.lastLiveTime = time.Now().UnixNano()
	//DPrintf(" [%d]Sever %d Initialized.\n", rf.currentTerm, rf.me)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
	// The first index of the servers is 1, so add one emtpy entries at first
	rf.logEntry = append(rf.logEntry, logEntry{Index: 0})
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commitlogTicker()

	return rf
}
