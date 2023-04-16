package raft

//
// This is an outline of the API that raft must expose to the service (or tester).
// Please see comments below for each of these functions for more details.
//
// rf = MakeRaft(...)
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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/log"
)

const (
	NullCandidate               = -1
	HeartBeatIntervalMS         = 100
	RespWaitingTimeoutMS        = 125
	ElectionTimeoutLowerBound   = 300
	ElectionTimeoutUpBoundRange = 500
)

// Raft state
const (
	Follower = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// Raft implements a single Raft peer.
type Raft struct {
	mtx     sync.Mutex // Lock to protect shared access to this peer's state
	respMtx sync.Mutex

	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	myIdx       int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	myRand      *rand.Rand          // the Raft's own random number generator
	heartbeatCh chan struct{}       // a channel to receive heartbeat from the leader

	// TODO: 2B, 2C. Look at the paper's Figure 2 for a description of what state a Raft server must maintain.

	// ========= persistent state on all servers =========
	state       int           // current state of the server
	currentTerm int           // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int           // candidateId that received vote in current term (or null if none)
	logs        []interface{} // each entry contains command for state machine, and term when entry was received by leader

	// ========= volatile state on all servers =========
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine

	// ========= volatile state on all servers =========
	nextIndices  []int // for each server, index of the next log entry to send to that server
	matchIndices []int // for each server, index of the highest log entry known to be replicated on server
}

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs represents RequestVote RPC arguments structure
type RequestVoteArgs struct { // TODO: 2B
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply represents RequestVote RPC reply structure
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote is invoked by candidates to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	heartbeat := false
	rf.mtx.Lock()
	defer func() {
		rf.mtx.Unlock()
		if heartbeat {
			rf.heartbeatCh <- struct{}{}
		}
	}()

	// TODO: Your code here (2B).
	if args.Term == rf.currentTerm {
		return
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	if rf.votedFor == NullCandidate || rf.votedFor == args.CandidateId || rf.state == Leader {
		log.Debug("Raft.RequestVote", "raft server %d voted for %d previously, votes for the candidate %d", rf.myIdx, rf.votedFor, args.CandidateId)
		rf.state = Follower
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		heartbeat = true
	}
}

type AppendEntriesArgs struct {
	Term         int           // leader’s term
	LeaderId     int           // so follower can redirect clients
	PrevLogIndex int           // index of log entry immediately preceding new ones
	PrevLogTerm  int           // term of prevLogIndex entry
	LeaderCommit int           // leader’s commitIndex
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	heartbeat := false
	rf.mtx.Lock()
	defer func() {
		rf.mtx.Unlock()
		if heartbeat {
			log.Debug("Raft.AppendEntries", "raft server %d received heartbeat from the leader %d", rf.myIdx, args.LeaderId)
			rf.heartbeatCh <- struct{}{}
		}
	}()

	myTerm := rf.currentTerm
	leaderTerm := args.Term
	if myTerm > leaderTerm {
		reply.Term = myTerm
		return
	}

	rf.state = Follower
	rf.votedFor = args.LeaderId
	rf.currentTerm = leaderTerm
	heartbeat = true
}

// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return. Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've capitalized all field names
// in structs passed over RPC, and that the caller passes the address of the reply struct with &,
// not the struct itself.
//
// sendRequestVote sends a RequestVote RPC to a server.
// serverIdx is the index of the target server in rf.peers[].
func (rf *Raft) sendRequestVote(wg *sync.WaitGroup, serverIdx int, nVotes *int, nConnected *int) {
	defer wg.Done()
	reqVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.myIdx,
		//LastLogIndex: 0,
		//LastLogTerm: 0,
	}
	reqVoteReply := RequestVoteReply{}

	ok := false
	done := make(chan bool)
	go func() {
		ok = rf.peers[serverIdx].Call("Raft.RequestVote", &reqVoteArgs, &reqVoteReply)
		done <- true
	}()

	timeout := time.After(time.Duration(RespWaitingTimeoutMS) * time.Millisecond)
	select {
	case <-done:
		break
	case <-timeout:
		return
	}

	if !ok {
		return
	}

	rf.respMtx.Lock()
	*nConnected++
	rf.respMtx.Unlock()

	rf.mtx.Lock()
	if reqVoteReply.Term > rf.currentTerm {
		rf.currentTerm = reqVoteReply.Term
		rf.state = Follower
	}
	if reqVoteReply.VoteGranted && rf.state == Candidate {
		*nVotes++
	}
	rf.mtx.Unlock()
}

func (rf *Raft) sendAppendEntries(wg *sync.WaitGroup, serverIdx int, nReplies *int, nConnected *int) {
	defer wg.Done()
	appendEntriesArgs := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.myIdx,
	}
	appendEntriesReply := AppendEntriesReply{}

	ok := false
	done := make(chan bool)
	go func() {
		ok = rf.peers[serverIdx].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply)
		done <- true
	}()

	timeout := time.After(time.Duration(RespWaitingTimeoutMS) * time.Millisecond)
	select {
	case <-done:
		break
	case <-timeout:
		return
	}

	if !ok {
		return
	}

	rf.respMtx.Lock()
	*nConnected++
	rf.respMtx.Unlock()

	rf.mtx.Lock()
	if appendEntriesReply.Term > rf.currentTerm {
		rf.currentTerm = appendEntriesReply.Term
		rf.votedFor = NullCandidate
		rf.state = Follower
	} else {
		*nReplies++
	}
	rf.mtx.Unlock()
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		// check if a leader election should be started
		if rf.state == Leader { // I'm the leader, send heartbeat to maintain their authority
			rf.sendHeartBeat()
			time.Sleep(time.Duration(HeartBeatIntervalMS) * time.Millisecond)
			continue
		}

		// I ain't the leader, pause for a random amount of time between 50 and 350 milliseconds.
		ms := ElectionTimeoutLowerBound + (rf.myRand.Int63() % ElectionTimeoutUpBoundRange)
		electionTimeout := time.After(time.Duration(ms) * time.Millisecond)
		select {
		case <-electionTimeout:
			rf.startElection()
		case <-rf.heartbeatCh:
			break
		}
	}
}

func (rf *Raft) startElection() {
	log.Debug("Raft.startElection", "raft server %d starts the election!", rf.myIdx)
	var wg sync.WaitGroup
	rf.state = Candidate
	rf.votedFor = rf.myIdx
	rf.currentTerm++

	nVotes := 1     // number of received votes
	nConnected := 1 // number of connected raft servers
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}
		wg.Add(1)
		go rf.sendRequestVote(&wg, i, &nVotes, &nConnected)
	}

	wg.Wait()
	log.Debug("Raft.startElection", "raft server %d got %d votes", rf.myIdx, nVotes)
	if nVotes > nConnected/2 && nVotes > 1 && rf.state == Candidate {
		rf.state = Leader
	} else {
		rf.state = Follower
		rf.votedFor = NullCandidate
	}
}

func (rf *Raft) sendHeartBeat() {
	log.Debug("Raft.sendHeartBeat", "raft server %d begins to send heartbeat...", rf.myIdx)
	var wg sync.WaitGroup
	nReplies := 0
	nConnected := 0
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}
		wg.Add(1)
		go rf.sendAppendEntries(&wg, i, &nReplies, &nConnected)
	}

	wg.Wait()
	if rf.state == Leader && nReplies <= nConnected/2 {
		rf.state = Follower
		rf.votedFor = NullCandidate
		log.Debug("Raft.sendHeartBeat", "raft server %d only got %d replies and is demoted to be a follower", rf.myIdx, nReplies)
	}
}

// MakeRaft creates a Raft server. The ports of all the Raft servers (including this one) are in peers[].
// This server's port is peers[myIdx]. All the servers' peers[] arrays have the same order.
// persister is a place for this server to save its persistent state, and also initially holds the most recent saved
// state, if any. applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// MakeRaft must return quickly, so it should start goroutines for any long-running work.
func MakeRaft(peers []*labrpc.ClientEnd, myIdx int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.myIdx = myIdx

	// TODO: Your initialization code here (2B, 2C).
	rf.state = Follower
	rf.votedFor = NullCandidate // -1 means this server hasn't voted yet for the current term
	rf.heartbeatCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// set up the random generator
	seed := time.Now().UnixNano() + int64(myIdx)
	rf.myRand = rand.New(rand.NewSource(seed))

	// start heartbeat goroutine to start elections
	go rf.heartbeat()
	return rf
}
