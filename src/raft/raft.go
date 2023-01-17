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
	"log"
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
// 2C
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

	// 2A process election vote: compare terms
	// when ==, if has voted:
	//             not voted before,
	// when >, be follower and vote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		return
	}
	CandiTerm := args.Term
	CandiID := args.CandidateID

	if args.LastLogIndex > 0 && (
	// The last log with larger term is more up-to-date
	args.LastLogTerm < rf.logEntry[len(rf.logEntry)-1].Term ||
		(args.LastLogTerm == rf.logEntry[len(rf.logEntry)-1].Term && args.LastLogIndex < len(rf.logEntry)-1)) {
		// Server is more up-to-date
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[%d] Server %d has more up-to-date logs than Candidate %d, reject voteRequest, reply %t", rf.currentTerm, rf.me, CandiID, reply.VoteGranted)
		return
	}
	// Candidate is mote up-to-date, keep on voting process
	if CandiTerm > rf.currentTerm {
		reply.VoteGranted = true
		reply.Term = CandiTerm
		rf.enFollower(CandiTerm)
		rf.votedFor = CandiID
		DPrintf("[%d]Server %d update term from term %d, vote for %d, reply %t", CandiTerm, rf.me, rf.currentTerm, CandiID, reply.VoteGranted)
	} else if rf.votedFor != -1 && rf.currentTerm == CandiTerm {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.resetTime()
		rf.votedFor = CandiID
		DPrintf("[%d]Server %d, vote for %d, reply %t", rf.currentTerm, rf.me, CandiID, reply.VoteGranted)
	} else {
		// have voted or term not match
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[%d] Server %d, reject voteRequest from %d, reply %t", rf.currentTerm, rf.me, CandiID, reply.VoteGranted)

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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

// follower needs to tell the leader the next index to send logEntry
func (rf *Raft) getNextIndex() int {
	return len(rf.logEntry)
	//lenLog := len(rf.logEntry)
	//if lenLog > 1 {
	//	return rf.logEntry[lenLog-1].Index + 1
	//} else {
	//	return 1
	//}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A only heartbeat
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	// ae rpc / 1
	if args.Term < rf.currentTerm {
		DPrintf("Server:%d, argsTerm: %d currentTerm:%d", rf.me, args.Term, rf.currentTerm)
		return
	} else if args.Term >= rf.currentTerm {
		rf.enFollower(args.Term)
		rf.leaderID = args.LeaderID
		//DPrintf("[%d]Server %d convert to Follower from term %d\n", args.Term, rf.me, rf.currentTerm)
	}

	// Rules for servers / Candidates / 3
	if rf.state == CANDIDATE {
		rf.enFollower(args.Term)
		rf.leaderID = args.LeaderID
		//DPrintf("[%d]Server %d convert from Candidate to Follower\n", args.Term, rf.me)
	}
	// heartbeat message
	//if args.LogEntries == nil {
	//	rf.enFollower(args.Term)
	//	rf.votedFor = args.LeaderID
	//	rf.leaderID = args.LeaderID
	//	DPrintf("[%d]Server %d receive HeartBeat from Leader %d\n", args.Term, rf.me, args.LeaderID)
	//
	//} else {
	//	DPrintf("[%d]Server %d receive HeartBeat with entries from Leader %d\n", args.Term, rf.me, args.LeaderID)
	//}

	// ae rpc / 2
	if args.PrevLogIndex > len(rf.logEntry)-1 {
		//reply.NextIndex = 0
		DPrintf("[%d]Server %d PrevLogIndex %d> Server %d lastIndex %d", rf.currentTerm, args.LeaderID, args.PrevLogIndex, rf.me, len(rf.logEntry)-1)
	}
	// when failed, the nextIndex should be decreased until the Follower's logEntry match Leader's.
	if args.PrevLogIndex > 0 && args.PrevLogIndex < len(rf.logEntry) &&
		rf.logEntry[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("PrevLogIndex: %d, lastlogINdex: %d", args.PrevLogIndex, len(rf.logEntry)-1)
		for i := args.PrevLogIndex; i > 1; i-- {
			if rf.logEntry[i].Term != args.PrevLogTerm {
				rf.logEntry = rf.logEntry[:i]
			}
		}
		DPrintf("[%d]: Conflict in AppendEntries between leader %d and follower %d\n ", args.Term, args.LeaderID, rf.me)
		return
	}

	for _, entry := range args.LogEntries {
		// ae rpc / 3
		// entry conflicts
		if entry.Index < len(rf.logEntry) && entry.Term != rf.logEntry[entry.Index].Term {
			rf.logEntry = rf.logEntry[:entry.Index]
		}
		// ae rpc / 4
		// append new entries not in the log
		if entry.Index >= len(rf.logEntry) {
			rf.logEntry = append(rf.logEntry, entry)
		}
	}

	//rf.commitIndex = len(rf.logEntry) - 1
	// ae rpc / 5
	if len(rf.logEntry) > 1 && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.logEntry[len(rf.logEntry)-1].Index)))
		DPrintf("[%d]Server %d commits commitIndex: %d", rf.currentTerm, rf.me, rf.commitIndex)
		rf.commitLogs(rf.commitIndex)
	}
	//DPrintf("[%d]Server %d LeaderINdex: %d", rf.currentTerm, rf.me, args.LeaderCommit)
	//DPrintf("[%d]Server %d commits commitIndex: %d", rf.currentTerm, rf.me, rf.commitIndex)

	reply.Success = true
	DPrintf("[%d]Server:%d lastIndex: %d, Leader:%d PrevIndex: %d", rf.currentTerm, rf.me, len(rf.logEntry)-1, args.LeaderID, args.PrevLogIndex)
	reply.NextIndex = rf.getNextIndex()

	//rf.commitIndex = args.LeaderCommit
	if len(rf.logEntry) > 1 {
		DPrintf("[%d]Server:%d, len = %d, command=%d", rf.currentTerm, rf.me, len(rf.logEntry)-1, rf.logEntry[rf.getNextIndex()-1].Command)
	}
	return
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	// only leader can send heartbeat
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.leaderID,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && !rf.killed() {
			// 2B, entries will be appended
			args.PrevLogIndex = len(rf.logEntry) - 1
			// when log exists
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.logEntry[args.PrevLogIndex].Term
			}
			// Rules for servers / Leader / 3
			// leader needs to send
			if rf.nextIndex[i] < len(rf.logEntry) {
				args.LogEntries = rf.logEntry[rf.nextIndex[i]:]
			}
			args.LeaderCommit = rf.commitIndex

			go func(i int) {
				reply := AppendEntriesReply{}

				// if not success,keep sending heartbeat
				for !rf.killed() && !rf.SendAppendEntries(i, &args, &reply) {
				}
				DPrintf("[%d]Leader %d send heartbeat to server %d", rf.currentTerm, rf.me, i)
				rf.mu.Lock()
				if args.Term == rf.currentTerm {
					rf.handleReply(i, reply)
				}
				rf.mu.Unlock()
				return
			}(i)
		}
	}
}

func (rf *Raft) handleReply(i int, reply AppendEntriesReply) {

	if rf.state != LEADER {
		return
	}
	if reply.Term > rf.currentTerm {
		// when leader finds its term out of date, reverts to Follower
		// rf.currentTerm = reply.Term
		rf.enFollower(reply.Term)
		DPrintf("update leader's term")
		return
	}
	if reply.Success {

		rf.nextIndex[i] = reply.NextIndex
		rf.matchIndex[i] = reply.NextIndex - 1

		if rf.nextIndex[i] > len(rf.logEntry) {
			rf.nextIndex[i] = len(rf.logEntry)
			rf.matchIndex[i] = len(rf.logEntry) - 1
		}

		// TODO commit
		count_commit := 0
		for j := 0; j < len(rf.peers); j++ {
			//if j == rf.me {
			//	continue
			//}

			if rf.matchIndex[j] >= rf.matchIndex[i] {
				count_commit += 1
			}
		}
		// an entry from its current term is committed once that entry is stored in a majority of servers
		// in Paper 5.4.2

		DPrintf("length of peers: %d, count_commit = %d", len(rf.peers), count_commit)
		if count_commit*2 > len(rf.peers) &&
			rf.commitIndex < rf.matchIndex[i] &&
			rf.logEntry[rf.matchIndex[i]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[i]
			rf.commitLogs(rf.commitIndex)
		}

	} else {
		// Rules for Leaders / 3
		if rf.nextIndex[i] > 1 {
			rf.nextIndex[i] -= 1
			rf.matchIndex[i] = rf.nextIndex[i] - 1
			DPrintf("[%d]NextIndex[%d] - 1 = %d", rf.currentTerm, i, rf.nextIndex[i])
		}
		//rf.sendHeartBeat()
	}
	return
}

func (rf *Raft) commitLogs(commitindex int) {

	if rf.commitIndex > len(rf.logEntry)-1 {
		log.Fatal("Error in commitlogs")
	}

	for i := rf.lastApplied + 1; i <= commitindex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntry[i].Command,
			CommandIndex: rf.logEntry[i].Index,
			// 2D
			//SnapshotValid: false,
			//Snapshot:      nil,
			//SnapshotTerm:  0,
			//SnapshotIndex: 0,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) goElection() {
	rf.mu.Lock()
	DPrintf("[%d] Server %d call election.\n", rf.currentTerm, rf.me)
	rf.enCandidate()

	term := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := len(rf.logEntry) - 1
	lastLogTerm := 0
	if lastLogIndex > 0 {
		lastLogTerm = rf.logEntry[lastLogIndex].Term
	}
	rf.mu.Unlock()
	// send to every server
	for i := range rf.peers {
		if i != rf.me && !rf.killed() {
			go func(i int) {
				args := RequestVoteArgs{
					Term:         term,
					CandidateID:  candidateID,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				// if not successful, keep sending requests
				for !rf.killed() && !rf.sendRequestVote(i, &args, &reply) {
				}

				rf.mu.Lock()
				if reply.VoteGranted == true && rf.state == CANDIDATE {
					rf.voteCount += 1
					if rf.voteCount*2 > len(rf.peers) {
						rf.enLeader()
						rf.mu.Unlock()
						return
					}
				} else if term < reply.Term {
					// find up-to-date term
					rf.enFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(i)
		}
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
	if rf.state != LEADER {
		return index, term, isLeader
	} else {
		DPrintf("Leader %d called Start()", rf.me)
		isLeader = true
		index = rf.getNextIndex()
		term = rf.currentTerm
		logentry := logEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logEntry = append(rf.logEntry, logentry)
		rf.nextIndex[rf.me] = rf.getNextIndex()
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
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
		time.Sleep(rf.electionTimeout)

		rf.mu.Lock()
		state := rf.state
		lasttimelive := rf.lastLiveTime
		rf.mu.Unlock()
		if state == LEADER {
			continue
		} else {
			duration := time.Duration(time.Now().UnixNano() - lasttimelive)
			if duration > rf.electionTimeout {
				DPrintf("[%d]Server %d meets ElectionTimeout over %dms", rf.currentTerm, rf.me, duration/1e6)
				rf.goElection()
			}
		}
	}
}

func (rf *Raft) broadcastHeartBeat() {
	for rf.killed() == false && rf.state == LEADER {
		rf.sendHeartBeat()
		DPrintf("[%d]Server %d sends periodic heartbeat! HeartTimeout %s", rf.currentTerm, rf.me, rf.heartBeatTimeout)
		time.Sleep(rf.heartBeatTimeout)
	}
}

func (rf *Raft) resetTime() {
	rf.resetElectionTimeout()
	rf.lastLiveTime = time.Now().UnixNano()
}

func (rf *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(100)+300)
	DPrintf("[%d]Server %d ElectionTimeout size resetted to %dms", rf.currentTerm, rf.me, rf.electionTimeout/1e6)

}

func (rf *Raft) enFollower(term int) {
	rf.state = FOLLOWER
	rf.leaderID = -1
	rf.votedFor = -1
	rf.voteCount = 0
	rf.currentTerm = term
	rf.resetTime()
	//DPrintf("[%d]Sever %d become the Follower.\n", rf.currentTerm, rf.me)

}

func (rf *Raft) enCandidate() {
	rf.state = CANDIDATE
	rf.leaderID = -1
	rf.votedFor = rf.me
	rf.voteCount += 1
	rf.currentTerm += 1
	rf.resetTime()
	DPrintf("[%d]Sever %d become the candidate.\n", rf.currentTerm, rf.me)
}

func (rf *Raft) enLeader() {
	rf.state = LEADER
	rf.votedFor = rf.me
	rf.leaderID = rf.me
	rf.voteCount = 0
	lastlogindex := len(rf.logEntry) - 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastlogindex + 1
		rf.matchIndex[i] = lastlogindex
	}
	DPrintf("[%d]Server %d become leader.", rf.currentTerm, rf.me)
	rand.Seed(time.Now().UnixNano())
	rf.heartBeatTimeout = time.Millisecond * time.Duration(rand.Intn(50)+100)
	go rf.broadcastHeartBeat()
	//rf.sendHeartBeat()
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
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(100)+300)
	rf.lastLiveTime = time.Now().UnixNano()
	DPrintf(" [%d]Sever %d Initialized.\n", rf.currentTerm, rf.me)

	// The first index of the servers is 1, so add one emtpy entries at first
	rf.logEntry = append(rf.logEntry, logEntry{Index: 0})
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
