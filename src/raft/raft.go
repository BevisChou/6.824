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

type Identity int

const (
	FOLLOWER  Identity = 0
	CANDIDATE Identity = 1
	LEADER    Identity = 2

	NULL int = -1
	NA   int = -2
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

	// log         []interface{}
	// commitIndex int

	// nextIndex  []int
	// matchIndex []int

	statePatchChan chan *statePatch
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term, reply.Voted = rf.currentTerm, false
		rf.mu.Unlock()
	} else {
		reply.Term = args.Term
		patch := NO_ACTION()
		if rf.currentTerm < args.Term {
			patch.term, patch.leader, patch.votedFor, patch.identity = args.Term, NULL, NULL, FOLLOWER
		}
		if (rf.votedFor == NULL || patch.votedFor == NULL) && true {
			// for 2A we blindly vote for whoever sends the request first
			reply.Voted = true
			patch.votedFor = args.Me
		}
		rf.statePatchChan <- &patch
		// mutex would be unlocked in applyPatch
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
	Term   int
	Leader int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		reply.Term, reply.Success = rf.currentTerm, true // Success is always true for lab2a
		rf.statePatchChan <- &statePatch{args.Term, args.Leader, NA, FOLLOWER}
		// mutex would be unlocked in applyPatch
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, ch *chan *AppendEntriesReply) {
	if server != rf.me {
		reply := AppendEntriesReply{}
		if rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
			*ch <- &reply
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

func NO_ACTION() statePatch {
	return statePatch{NA, NA, NA, Identity(NA)}
}

type statePatch struct {
	// fields pending
	term     int
	leader   int
	votedFor int
	identity Identity
}

func (rf *Raft) follower() *statePatch {
	var ret *statePatch
	select {
	case patch := <-rf.statePatchChan:
		ret = patch
	case <-time.After(rf.getElectionTimeout()):
		rf.mu.Lock() // would be unlocked in applyPatch
		ret = &statePatch{rf.currentTerm + 1, NULL, rf.me, CANDIDATE}
	}
	return ret
}

func (rf *Raft) candidate() *statePatch {
	var ret *statePatch
	// no need to add lock when reading currentTerm here
	args, ch := RequestVoteArgs{rf.me, rf.currentTerm}, make(chan *RequestVoteReply)
	voted, finished := 1, 1
	for i := range rf.peers {
		go rf.sendRequestVote(i, args, &ch)
	}
	for timeout := time.After(rf.getElectionTimeout()); ret == nil; {
		select {
		case ret = <-rf.statePatchChan:
		case res := <-ch:
			rf.mu.Lock()
			if res.Term > rf.currentTerm {
				ret = &statePatch{res.Term, NULL, NULL, FOLLOWER}
			} else {
				finished++
				if res.Voted {
					voted++
				}
				if voted > len(rf.peers)/2 {
					ret = &statePatch{rf.currentTerm, rf.me, NA, LEADER}
				} else if finished-voted > len(rf.peers)/2 {
					ret = &statePatch{rf.currentTerm, NULL, NA, FOLLOWER}
				} else {
					rf.mu.Unlock()
				}
			}
		case <-timeout:
			rf.mu.Lock()
			ret = &statePatch{rf.currentTerm + 1, NULL, rf.me, CANDIDATE}
		}
	}
	return ret
}

func (rf *Raft) leader() *statePatch {
	var ret *statePatch
	args, ch := AppendEntriesArgs{Term: rf.currentTerm, Leader: rf.currentLeader}, make(chan *AppendEntriesReply, len(rf.peers))
	for tick := time.NewTicker(100 * time.Millisecond); ret == nil; {
		select {
		case ret = <-rf.statePatchChan:
		case res := <-ch:
			rf.mu.Lock()
			if res.Term > rf.currentTerm {
				ret = &statePatch{res.Term, NULL, NULL, FOLLOWER}
			} else {
				rf.mu.Unlock()
			}
		case <-tick.C:
			for i := range rf.peers {
				go rf.sendAppendEntries(i, args, &ch)
			}
		}
	}
	return ret
}

func applyIfApplicable(p *int, v int) {
	if v != NA {
		*p = v
	}
}

func (rf *Raft) applyPatch(patch *statePatch) {
	applyIfApplicable(&rf.currentTerm, patch.term)
	applyIfApplicable(&rf.currentLeader, patch.leader)
	applyIfApplicable(&rf.votedFor, patch.votedFor)
	applyIfApplicable((*int)(&rf.identity), int(patch.identity))
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		var patch *statePatch
		switch rf.identity {
		case FOLLOWER:
			patch = rf.follower()
		case CANDIDATE:
			patch = rf.candidate()
		case LEADER:
			patch = rf.leader()
		}
		rf.applyPatch(patch)
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
	rf.currentLeader = NULL
	rf.identity = FOLLOWER
	rf.votedFor = NULL

	rf.statePatchChan = make(chan *statePatch)

	rand.Seed(time.Now().UnixNano())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
