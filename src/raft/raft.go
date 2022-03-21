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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Identity int

const (
	FOLLOWER  Identity = 0
	CANDIDATE Identity = 1
	LEADER    Identity = 2

	NULL int = -1

	HEARTBEAT_INTERVAL      = 100
	ELECTION_TIMEOUT_FACTOR = 2

	MAX_ENTRIES_LEN = 50
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
	currentTerm int
	votedFor    int
	log         []LogEntry

	identity      Identity
	currentLeader int

	commitIndex int
	lastApplied int
	applyCh     *chan ApplyMsg

	nextIndex  []int
	matchIndex []int

	heartbeat chan bool
}

type LogEntry struct {
	Term    int
	Command interface{}
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
	isleader = rf.identity == LEADER
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
	// Your data here (2A, 2B).
	CandidateId int
	Term        int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
	} else {
		reply.Term = args.Term
		if rf.identity == FOLLOWER {
			rf.heartbeat <- true
		}
		if rf.currentTerm < args.Term {
			rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = args.Term, FOLLOWER, NULL, NULL
		}
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		if rf.votedFor == NULL && (args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, ch *chan *RequestVoteReply) {
	if server != rf.me {
		reply := RequestVoteReply{}
		if rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
			*ch <- &reply
		}
	}
}

//
// AppendEntries related stuffs
//
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) applyCommittedEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
		*rf.applyCh <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	} else {
		if rf.identity == FOLLOWER {
			rf.heartbeat <- true
		}
		rf.currentTerm, rf.identity, rf.currentLeader = args.Term, FOLLOWER, args.LeaderId
		reply.Term = rf.currentTerm
		if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			for i, entry := range args.Entries {
				if args.PrevLogIndex+i+1 < len(rf.log) {
					rf.log[args.PrevLogIndex+i+1] = entry
				} else {
					rf.log = append(rf.log, entry)
				}
			}
			rf.log = rf.log[:args.PrevLogIndex+len(args.Entries)+1]
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				go rf.applyCommittedEntries()
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	if server != rf.me {
		if len(args.Entries) > MAX_ENTRIES_LEN {
			args.Entries = args.Entries[:MAX_ENTRIES_LEN]
		}
		reply := &AppendEntriesReply{}
		if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = reply.Term, FOLLOWER, NULL, NULL
			} else if len(args.Entries) > 0 {
				if reply.Success {
					rf.nextIndex[server] += len(args.Entries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				} else {
					rf.nextIndex[server]--
				}
			}
			rf.mu.Unlock()
		}
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.identity != LEADER {
		isLeader = false
	} else {
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		rf.matchIndex[rf.me] = len(rf.log) - 1
		index, term = len(rf.log)-1, rf.currentTerm
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

func (rf *Raft) getElectionTimeout() time.Duration {
	base := HEARTBEAT_INTERVAL * ELECTION_TIMEOUT_FACTOR
	return time.Duration(base+rand.Intn(base)) * time.Millisecond
}

func (rf *Raft) follower() {
	rf.mu.Unlock()
begin:
	timeout := time.After(rf.getElectionTimeout())
	select {
	case <-rf.heartbeat:
		goto begin
	case <-timeout:
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = rf.currentTerm+1, CANDIDATE, NULL, rf.me
	}
}

func (rf *Raft) candidate() {
	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{rf.me, rf.currentTerm, lastLogIndex, rf.log[lastLogIndex].Term}
	rf.mu.Unlock()
	ch := make(chan *RequestVoteReply, len(rf.peers))
	voted, finished := 1, 1
	for i := range rf.peers {
		go rf.sendRequestVote(i, args, &ch)
	}
	for loop, timeout := true, time.After(rf.getElectionTimeout()); loop; {
		select {
		case res := <-ch:
			loop = false
			rf.mu.Lock() // critical section start
			if res.Term > rf.currentTerm {
				rf.currentTerm, rf.identity, rf.votedFor, rf.votedFor = res.Term, FOLLOWER, NULL, NULL
			} else if rf.identity == CANDIDATE {
				finished++
				if res.VoteGranted {
					voted++
				}
				if voted > len(rf.peers)/2 {
					rf.identity, rf.currentLeader = LEADER, rf.me
				} else if finished-voted > len(rf.peers)/2 {
					rf.identity = FOLLOWER
				} else {
					loop = true
				}
			}
			rf.mu.Unlock() // critical section end
		case <-timeout:
			loop = false
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.identity == CANDIDATE {
				rf.currentTerm++
			}
		}
	}
}

func (rf *Raft) prepareAppendAppendEntriesArgs() []AppendEntriesArgs {
	// in critical section
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] > matchIndex[j]
	})
	if rf.log[matchIndex[len(matchIndex)/2]].Term == rf.currentTerm && matchIndex[len(matchIndex)/2] > rf.commitIndex {
		rf.commitIndex = matchIndex[len(matchIndex)/2]
		go rf.applyCommittedEntries()
	}

	base := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.currentLeader, LeaderCommit: rf.commitIndex}
	ret := make([]AppendEntriesArgs, len(rf.peers))

	for i := range ret {
		ret[i] = base
		ret[i].PrevLogIndex = rf.nextIndex[i] - 1
		ret[i].PrevLogTerm = rf.log[ret[i].PrevLogIndex].Term
		ret[i].Entries = rf.log[rf.nextIndex[i]:]
	}
	return ret
}

func (rf *Raft) leader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i], rf.matchIndex[i] = len(rf.log), 0
	}
	rf.mu.Unlock()
	for tick, loop := time.NewTicker(HEARTBEAT_INTERVAL*time.Millisecond), true; loop; {
		<-tick.C
		rf.mu.Lock()
		if rf.identity == LEADER {
			args := rf.prepareAppendAppendEntriesArgs()
			for i := range rf.peers {
				go rf.sendAppendEntries(i, &args[i])
			}
		} else {
			loop = false
		}
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		switch rf.identity {
		case FOLLOWER:
			rf.follower()
		case CANDIDATE:
			rf.candidate()
		case LEADER:
			rf.leader()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]LogEntry, 1) // first log's index is 1

	rf.identity = FOLLOWER
	rf.currentLeader = NULL

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = &applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartbeat = make(chan bool, len(rf.peers)*ELECTION_TIMEOUT_FACTOR*2)

	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// helper functions
func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
