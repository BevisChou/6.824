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
	"errors"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type Identity int

const (
	FOLLOWER  Identity = 0
	CANDIDATE Identity = 1
	LEADER    Identity = 2

	NULL int = -1

	HEARTBEAT_INTERVAL   = 100
	ELECTION_TIMEOUT_MIN = 400
	ELECTION_TIMEOUT_MAX = 800
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

	lastIncludedTerm  int
	lastIncludedIndex int
	snapshot          []byte
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
	// in critical section
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
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
	currentTerm, votedFor, log, lastIncludedTerm, lastIncludedIndex := 0, NULL, make([]LogEntry, 0), NULL, -1

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// index is always smaller than or equal to rf.lastApplied
	// for more info read applierSnap method in config.go
	relativeIndex, term, _ := rf.entryInfo(index)
	rf.lastIncludedTerm = term
	rf.log = rf.log[relativeIndex+1:]
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persist()
}

//
// given the absolute index, return the relative index and the term
//
func (rf *Raft) entryInfo(absoluteIndex int) (int, int, error) {
	// in critical section
	index := absoluteIndex - rf.lastIncludedIndex - 1
	switch {
	case absoluteIndex < 0:
		return index, NULL, errors.New("out of range")
	case index < -1:
		return index, NULL, nil
	case index == -1:
		return -1, rf.lastIncludedTerm, nil
	case index < len(rf.log):
		return index, rf.log[index].Term, nil
	case index == len(rf.log):
		// special case for prepareArgs
		return index, NULL, nil
	default:
		return NULL, NULL, errors.New("out of range")
	}
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
		rf.heartbeat <- true

		reply.Term = args.Term
		shouldPersist := false

		if rf.currentTerm < args.Term {
			rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = args.Term, FOLLOWER, NULL, NULL
			shouldPersist = true
		}
		lastLogIndex := rf.lastIncludedIndex + len(rf.log)
		_, lastLogTerm, _ := rf.entryInfo(lastLogIndex)
		if rf.votedFor == NULL && (args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			shouldPersist = true
		}

		if shouldPersist {
			rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch *chan *RequestVoteReply) {
	if server != rf.me {
		reply := &RequestVoteReply{}
		if rf.peers[server].Call("Raft.RequestVote", args, reply) {
			*ch <- reply
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

	// used when RPC fails
	FirstIndex int
}

func (rf *Raft) applyCommittedEntries() {
	for loop := true; loop; {
		rf.mu.Lock()
		var msg ApplyMsg
		i := rf.lastApplied + 1 // lastApplied is always greater than or equal to lastIncludedIndex
		if i <= rf.commitIndex {
			index, _, _ := rf.entryInfo(i)
			msg.CommandValid, msg.Command, msg.CommandIndex = true, rf.log[index].Command, i
			rf.lastApplied = i
		} else {
			loop = false
		}
		rf.mu.Unlock()
		*rf.applyCh <- msg
	}
}

func (rf *Raft) LogInfo() (int, int) {
	len, lastLogTerm := len(rf.log), NULL
	if len > 0 {
		lastLogTerm = rf.log[len-1].Term
	}
	return len, lastLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	} else {
		rf.heartbeat <- true

		reply.Term = args.Term
		shouldPersist := false

		if rf.currentTerm < args.Term {
			rf.currentTerm, rf.votedFor = args.Term, NULL
			shouldPersist = true
		}
		rf.identity, rf.currentLeader = FOLLOWER, args.LeaderId

		convertedPrevLogIndex, term, _ := rf.entryInfo(args.PrevLogIndex)
		if len(rf.log) > convertedPrevLogIndex && term == args.PrevLogTerm {
			oldLen, oldLastLogTerm := rf.LogInfo()

			rf.log = rf.log[:convertedPrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)

			newLen, newLastLogTerm := rf.LogInfo()
			shouldPersist = newLen != oldLen || newLastLogTerm != oldLastLogTerm

			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, rf.lastIncludedIndex+len(rf.log))
				go rf.applyCommittedEntries()
			}
		} else {
			reply.FirstIndex = rf.lastIncludedIndex + 1        // most conservative guess
			index := min(convertedPrevLogIndex, len(rf.log)-1) // relative index
			if index >= 0 {
				for i := index; i >= 0 && rf.log[i].Term == rf.log[index].Term; i-- {
					reply.FirstIndex = rf.lastIncludedIndex + i + 1
				}
			}
			if reply.FirstIndex == 0 {
				reply.FirstIndex = 1
			}
		}
		if shouldPersist {
			rf.persist()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	if server != rf.me {
		reply := &AppendEntriesReply{}
		if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = reply.Term, FOLLOWER, NULL, NULL
				rf.persist()
			} else if rf.identity == LEADER {
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				} else if reply.Term == args.Term {
					rf.nextIndex[server] = reply.FirstIndex
				}
			}
			rf.mu.Unlock()
		}
	}
}

//
// InstallSnapshot related stuffs
//
type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedTerm  int
	LastIncludedIndex int
	Snapshot          []byte

	// no need for LeaderCommit
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else {
		reply.Term = args.Term
		rf.heartbeat <- true

		if rf.currentTerm < args.Term {
			rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = args.Term, FOLLOWER, args.LeaderId, NULL
		} else if args.LastIncludedIndex > rf.lastApplied { // robust
			rf.lastIncludedIndex, rf.lastIncludedTerm, rf.snapshot, rf.log =
				args.LastIncludedIndex,
				args.LastIncludedTerm,
				args.Snapshot,
				make([]LogEntry, 0)
			*rf.applyCh <- ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex}
			rf.lastApplied = rf.lastIncludedIndex
			rf.persist()
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	if server != rf.me {
		reply := &InstallSnapshotReply{}
		if rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm, rf.identity, rf.currentLeader, rf.votedFor = reply.Term, FOLLOWER, NULL, NULL
				rf.persist()
			} else if reply.Term == args.Term {
				rf.matchIndex[server] = args.LastIncludedIndex
				rf.nextIndex[server] = rf.matchIndex[server] + 1
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
		rf.persist()
		rf.matchIndex[rf.me] = rf.lastIncludedIndex + len(rf.log)
		index, term = rf.matchIndex[rf.me], rf.currentTerm
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
	return time.Duration(ELECTION_TIMEOUT_MIN+rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)) * time.Millisecond
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
		rf.persist()
	}
}

func (rf *Raft) candidate() {
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) // absolute index
	_, lastLogTerm, _ := rf.entryInfo(lastLogIndex)
	args := RequestVoteArgs{rf.me, rf.currentTerm, lastLogIndex, lastLogTerm}
	rf.mu.Unlock()
	ch := make(chan *RequestVoteReply, len(rf.peers))
	voted, finished := 1, 1
	for i := range rf.peers {
		go rf.sendRequestVote(i, &args, &ch)
	}
	for loop, timeout := true, time.After(rf.getElectionTimeout()); loop; {
		select {
		case res := <-ch:
			loop = false
			rf.mu.Lock() // critical section start
			if res.Term > rf.currentTerm {
				rf.currentTerm, rf.identity, rf.votedFor, rf.votedFor = res.Term, FOLLOWER, NULL, NULL
				rf.persist()
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
				rf.persist()
			}
		}
	}
}

func (rf *Raft) prepareArgs() ([]*AppendEntriesArgs, []*InstallSnapshotArgs) {
	// in critical section
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] > matchIndex[j]
	})
	_, term, _ := rf.entryInfo(matchIndex[len(matchIndex)/2])
	if term == rf.currentTerm && matchIndex[len(matchIndex)/2] > rf.commitIndex {
		rf.commitIndex = matchIndex[len(matchIndex)/2]
		go rf.applyCommittedEntries()
	}

	appendEntriesArgs, installSnapshotArgs := make([]*AppendEntriesArgs, len(rf.peers)), make([]*InstallSnapshotArgs, len(rf.peers))

	for i := range rf.peers {
		index, _, _ := rf.entryInfo(rf.nextIndex[i])
		_, prevLogTerm, _ := rf.entryInfo(rf.nextIndex[i] - 1)
		if index >= 0 {
			appendEntriesArgs[i] = &AppendEntriesArgs{
				rf.currentTerm,
				rf.me,
				rf.nextIndex[i] - 1,
				prevLogTerm,
				rf.log[index:],
				rf.commitIndex}
		} else {
			installSnapshotArgs[i] = &InstallSnapshotArgs{
				rf.currentTerm,
				rf.me,
				rf.lastIncludedTerm,
				rf.lastIncludedIndex,
				rf.snapshot}
		}
	}
	return appendEntriesArgs, installSnapshotArgs
}

func (rf *Raft) leader() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i], rf.matchIndex[i] = rf.lastIncludedIndex+len(rf.log)+1, 0
	}
	rf.mu.Unlock()
	for tick, loop := time.NewTicker(HEARTBEAT_INTERVAL*time.Millisecond), true; loop; {
		<-tick.C
		rf.mu.Lock()
		if rf.identity == LEADER {
			appendEntriesArgs, installSnapshotArgs := rf.prepareArgs()
			for i := range rf.peers {
				if appendEntriesArgs[i] != nil {
					go rf.sendAppendEntries(i, appendEntriesArgs[i])
				} else {
					go rf.sendInstallSnapshot(i, installSnapshotArgs[i])
				}
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

	rf.heartbeat = make(chan bool, len(rf.peers)*ELECTION_TIMEOUT_MAX/HEARTBEAT_INTERVAL)

	rf.lastIncludedTerm = NULL
	rf.lastIncludedIndex = -1 // must be -1
	// no need to allocate space to rf.snapshot

	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex > 0 {
		rf.snapshot = persister.ReadSnapshot()
		rf.lastApplied = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// helper functions
func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
