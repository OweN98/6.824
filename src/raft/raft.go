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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntry)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntry = logs
	}
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
	CandiID := args.CandidateID

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	CandiTerm := args.Term
	if args.LastLogIndex > 0 && (
	// The last log with larger term is more up-to-date
	args.LastLogTerm < rf.logEntry[len(rf.logEntry)-1].Term ||
		(args.LastLogTerm == rf.logEntry[len(rf.logEntry)-1].Term && args.LastLogIndex < len(rf.logEntry)-1)) {
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
			lastlogindex := rf.getLastIndex()
			term := rf.currentTerm
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastlogindex,
				LastLogTerm:  rf.logEntry[lastlogindex].Term,
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

const (
	LogInConsistensy = 1
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if rf.state == LEADER {
	//	return
	//}
	DPrintf("[%d]Server %d receive heartbeat from leader %d,args.PrevLogIndex: %d, args.PrevLogTerm: %d, args.LeaderCommit:%d, rf.getLastIndex(): %d, lastLogTerm: %d , logLength: %d", rf.currentTerm, rf.me, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.getLastIndex(), rf.logEntry[rf.getLastIndex()].Term, len(args.LogEntries))
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
		//return
		//DPrintf("[%d]Server %d convert to Follower from term %d\n", args.Term, rf.me, rf.currentTerm)
	}
	// Rules for servers / Candidates / 3
	if rf.state == CANDIDATE {
		rf.enFollower(args.Term)
		rf.leaderID = args.LeaderID
		rf.persist()
		//return
		//DPrintf("[%d]Server %d convert from Candidate to Follower\n", args.Term, rf.me)
	}
	rf.leaderID = args.LeaderID
	rf.resetTime()
	// ae rpc / 2
	if args.PrevLogIndex > rf.getLastIndex() {
		//DPrintf("[%d]Server %d PrevLogIndex %d> Server %d lastIndex %d", rf.currentTerm, args.LeaderID, args.PrevLogIndex, rf.me, rf.getNextIndex())
		reply.LogInconsistency = true
		reply.Next = rf.getNextIndex()
		return
	}
	// when failed, the nextIndex should be decreased until the Follower's logEntry match Leader's.
	// DPrintf("[%d]Server %d, %q, args.PrevLogIndex: %d, rf.getLastIndex(): %d, rf.logEntry[args.PrevLogIndex].Term: %d,  args.PrevLogTerm : %d", rf.currentTerm, rf.me, rf.state, args.PrevLogIndex, rf.getLastIndex(), rf.logEntry[args.PrevLogIndex].Term, args.PrevLogTerm)
	if args.PrevLogIndex > 0 && args.PrevLogIndex <= rf.getLastIndex() &&
		rf.logEntry[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d]: Conflict in AppendEntries between leader %d and follower %d", args.Term, args.LeaderID, rf.me)
		//include the term of the conflicting entry and the first index it stores for that term.
		for i := args.PrevLogIndex; i > 0 && i > rf.lastApplied; i-- {
			if rf.logEntry[i].Term != rf.logEntry[args.PrevLogIndex].Term {
				reply.Next = i + 1
				break
			}
		}
		reply.LogInconsistency = true
		return
	}

	// ae rpc / 3

	for _, entry := range args.LogEntries {
		// entry conflicts
		if entry.Index <= rf.getLastIndex() && entry.Term != rf.logEntry[entry.Index].Term {
			rf.logEntry = rf.logEntry[:entry.Index]
			DPrintf("[%d]SErver %d delete the entry %d and all after it ", rf.currentTerm, rf.me, entry.Index)
		}
	}
	// ae rpc / 4
	for _, entry := range args.LogEntries {
		// add new entries
		if entry.Index > rf.getLastIndex() {
			rf.logEntry = append(rf.logEntry, entry)
			//DPrintf("[%d]Server %d appended  index %d ", rf.currentTerm, rf.me, entry.Index)
		}
	}

	// ae rpc / 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastIndex())))
		DPrintf("[%d]args.LeaderCommit: %d, lenofServerlog: %d, last new entry index: %d, Server %d commits commitIndex: %d", rf.currentTerm, args.LeaderCommit, len(rf.logEntry), rf.getLastIndex(), rf.me, rf.commitIndex)
		rf.commitLogs(rf.commitIndex)

	}

	reply.Success = true
	rf.persist()
	if len(rf.logEntry) > 1 {
		//DPrintf("[%d]Server:%d, len = %d, command=%d, leader: %d, nextindex[%d]: %d", rf.currentTerm, rf.me, len(rf.logEntry)-1, rf.logEntry[rf.getNextIndex()-1].Command, rf.leaderID, rf.me, rf.nextIndex[rf.me])
	}

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
			DPrintf("[%d]Leader %d send to %d; next: %d, args last index: %d, leaderlast: %d", rf.currentTerm, rf.me, i, next, next-1, rf.getLastIndex())
			args.PrevLogIndex = next - 1
			if next <= rf.getLastIndex() {
				args.LogEntries = rf.logEntry[next:]
				args.PrevLogTerm = rf.logEntry[args.PrevLogIndex].Term
				//DPrintf("[%d]Leader %d sends entries from index %d to %d to server %d, len = %d", rf.currentTerm, rf.me, rf.nextIndex[i], len(rf.logEntry)-1, i, len(args.LogEntries))
			} else {
				args.PrevLogIndex = rf.getLastIndex()
				args.PrevLogTerm = rf.logEntry[args.PrevLogIndex].Term
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
		rf.nextIndex[i] += len(args.LogEntries)
		rf.matchIndex[i] = rf.nextIndex[i] - 1
		DPrintf("[%d]Success Leader %d to server %d, nextINdex[%d]:%d", rf.currentTerm, rf.me, i, i, rf.nextIndex[i])

		// TODO commit
		for k := rf.commitIndex + 1; k <= rf.getLastIndex(); k++ {
			count_commit := 0
			if rf.logEntry[k].Term == rf.currentTerm {
				for server := range rf.peers {
					if rf.matchIndex[server] >= k {
						count_commit += 1
					}
				}
				if count_commit*2 > len(rf.peers) {
					DPrintf("[%d]Leader %d Index %d Count commit: %d", rf.currentTerm, rf.me, k, count_commit)
					rf.commitIndex = k
				} else {
					break
				}
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.commitLogs(rf.commitIndex)
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

func (rf *Raft) commitLogs(ToCommitIndex int) {

	if ToCommitIndex > len(rf.logEntry)-1 {
		log.Fatal("Error in commitlogs")
	}

	for c := rf.lastApplied + 1; c <= ToCommitIndex; c++ {
		command := rf.logEntry[c].Command
		index := rf.logEntry[c].Index
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
			// 2D
			//SnapshotValid: false,
			//Snapshot:      nil,
			//SnapshotTerm:  0,
			//SnapshotIndex: 0,
		}
		//rf.logEntry[index].Index = index
	}
	DPrintf("[%d]Server %d, %q, commit from %d to %d", rf.currentTerm, rf.me, rf.state, rf.lastApplied+1, rf.commitIndex)
	rf.lastApplied = rf.commitIndex
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
	index = rf.getNextIndex()
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
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(300)+200)
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
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(300)+200)
	rf.lastLiveTime = time.Now().UnixNano()
	//DPrintf(" [%d]Sever %d Initialized.\n", rf.currentTerm, rf.me)

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
