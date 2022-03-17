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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Identity int32
type ElectionResult int

const (
	FOLLOWER  Identity = 0
	CANDIDATE Identity = 1
	LEADER    Identity = 2

	WON   ElectionResult = 0
	LOST  ElectionResult = 1
	SPLIT ElectionResult = 2

	NOBODY int = -1
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
	currentTerm   int
	currentLeader int
	identity      Identity
	votedFor      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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

func (rf *Raft) updateTerm(term int, leader int) {
	rf.currentLeader = leader
	rf.currentTerm = term
	rf.votedFor = NOBODY
	rf.setIdentity(FOLLOWER)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Me   int
	Term int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Voted bool
	Term  int
}

func (rf *Raft) voteFor(args *RequestVoteArgs) bool {
	if rf.currentTerm > args.Term {
		return false
	} else {
		// for 2A we blindly vote for whoever sends the request first
		return rf.votedFor == NOBODY
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		log.Printf("%v set term to %v in RequestVote and become follower\n", rf.me, args.Term)
		rf.updateTerm(args.Term, NOBODY)
	}
	if rf.voteFor(args) {
		log.Printf("%v current votedFor: %v at term %v\n", rf.me, rf.votedFor, rf.currentTerm)
		rf.votedFor = args.Me
		reply.Voted = true
	} else {
		reply.Voted = false
	}
	reply.Term = rf.currentTerm
	log.Printf("%v(term: %v) response to %v(term: %v): %v\n", rf.me, rf.currentTerm, args.Me, args.Term, reply.Voted)
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
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		*ch <- reply
	}
}

//
// AppendEntries related stuffs
//
type AppendEntriesArgs struct {
	Term   int
	Leader int
}

type AppendEntriesReply struct {
	Term   int
	Leader int

	// addition info
	Replied bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("%v received heartbeat from %v\n", rf.me, args.Leader)
	if rf.currentTerm > args.Term {
		reply.Term, reply.Leader = rf.currentTerm, rf.currentLeader
		return
	}
	if rf.currentTerm < args.Term {
		// currentTerm might be updated in RequestVote or after lost election but currentLeader might not
		rf.updateTerm(args.Term, args.Leader)
	} else if rf.currentLeader == NOBODY {
		rf.currentLeader = args.Leader
	}
	if rf.me != rf.currentLeader {
		if rf.identity != FOLLOWER { // debug
			log.Printf("server %v from %v to FOLLOWER\n", rf.me, identityStr(rf.identity))
		}
		// reset identity
		rf.setIdentity(FOLLOWER)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, ch *chan *AppendEntriesReply) {
	reply := &AppendEntriesReply{}
	reply.Replied = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	*ch <- reply
}

func (rf *Raft) doAppendEntries() (int, int) {
	args, ch := AppendEntriesArgs{rf.currentTerm, rf.me}, make(chan *AppendEntriesReply, len(rf.peers))
	for i := range rf.peers {
		go rf.sendAppendEntries(i, &args, &ch)
	}
	term, leader := rf.currentTerm, rf.me
	for range rf.peers {
		res := <-ch
		if res.Replied && res.Term > term {
			term, leader = res.Term, res.Leader
		}
	}
	return term, leader
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
	return time.Duration(250+rand.Intn(250)) * time.Millisecond
}

func (rf *Raft) setIdentity(identity Identity) {
	atomic.StoreInt32((*int32)(&rf.identity), int32(identity))
}

func (rf *Raft) getIdentity() Identity {
	return Identity(atomic.LoadInt32((*int32)(&rf.identity)))
}

func (rf *Raft) doElection() (ElectionResult, int) {
	args, ch := RequestVoteArgs{rf.me, rf.currentTerm}, make(chan *RequestVoteReply)
	for i := range rf.peers {
		go rf.sendRequestVote(i, &args, &ch)
	}
	voted, finished := 0, 0
	for timeout := time.After(rf.getElectionTimeout()); ; {
		select {
		case res := <-ch:
			if res.Term > rf.currentTerm {
				return LOST, res.Term
			}
			finished++
			if res.Voted {
				voted++
			}
			if voted > len(rf.peers)/2 {
				log.Printf("term election result: server %v - %v/%v\n", rf.me, voted, finished)
				return WON, rf.currentTerm
			} else if finished-voted > len(rf.peers)/2 {
				log.Printf("term election result: server %v - %v/%v\n", rf.me, voted, finished)
				return LOST, rf.currentTerm
			}
		case <-timeout:
			if voted == len(rf.peers)/2 {
				log.Printf("term election result: server %v - %v/%v\n", rf.me, voted, finished)
				return SPLIT, rf.currentTerm
			} else {
				log.Printf("term election result: server %v - %v/%v\n", rf.me, voted, finished)
				return LOST, rf.currentTerm
			}
		default:
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// where to add lock?
		switch {
		// getIdentity each time.
		case rf.getIdentity() == FOLLOWER:
			rf.setIdentity(CANDIDATE)
			log.Printf("%v FOLLOWER to CANDIDATE in term %v\n", rf.me, rf.currentTerm)
			time.Sleep(rf.getElectionTimeout())
		case rf.getIdentity() == CANDIDATE:
			log.Printf("%v state: CANDIDATE, term: %v\n", rf.me, rf.currentTerm)
			// identity might be changed
			rf.updateTerm(rf.currentTerm+1, NOBODY)
			log.Printf("%v change term to %v and start requesting votes\n", rf.me, rf.currentTerm)
			res, term := rf.doElection() // term is at least the same as rf.currentTerm
			log.Printf("%v election result: %v, term %v\n", rf.me, electionResultStr(res), term)
			if res == LOST {
				rf.currentTerm = term
				rf.setIdentity(FOLLOWER)
			} else if res == WON {
				rf.setIdentity(LEADER)
				rf.currentLeader = rf.me
			}
			log.Printf("%v CANDIDATE to %v\n", rf.me, identityStr(rf.identity))
		// temporarily place heartbeats sending logic here for lab2a
		case rf.getIdentity() == LEADER:
			term, leader := rf.doAppendEntries()
			if term > rf.currentTerm {
				rf.updateTerm(term, leader)
				log.Printf("%v LEADER to %v\n", rf.me, identityStr(rf.identity))
			}
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
	rf.currentLeader = NOBODY
	rf.identity = FOLLOWER
	rf.votedFor = NOBODY

	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// debug functions
func identityStr(identity Identity) string {
	ret := ""
	switch identity {
	case FOLLOWER:
		ret = "FOLLOWER"
	case CANDIDATE:
		ret = "CANDIDATE"
	case LEADER:
		ret = "LEADER"
	}
	return ret
}

func electionResultStr(res ElectionResult) string {
	ret := ""
	switch res {
	case WON:
		ret = "WON"
	case LOST:
		ret = "LOST"
	case SPLIT:
		ret = "SPLIT"
	}
	return ret
}
