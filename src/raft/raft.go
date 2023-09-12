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
	"6.5840/log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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
	mtx       sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	myIdx     int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// TODO: 2B, 2C. Look at the paper's Figure 2 for a description of what state a Raft server must maintain.
	rand          *rand.Rand // random number source
	electionTimer *time.Timer
	heartbeatChan chan struct{} // a channel to receive heartbeat from the leader

	// ========= persistent state on all servers =========
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	state       int
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
	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
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

// RequestVote is invoked by candidates to gather votes
func (rf *Raft) RequestVote(reqVoteArgs *RequestVoteArgs, reqVoteReply *RequestVoteReply) { // TODO: 2B
	log.Debug("Raft.RequestVote", "raft server %d receives the request of vote: %+v", rf.myIdx, *reqVoteArgs)
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	if rf.currentTerm > reqVoteArgs.Term {
		reqVoteReply.Term = rf.currentTerm
		return
	}

	reqVoteReply.Term = reqVoteArgs.Term
	if rf.currentTerm < reqVoteArgs.Term || rf.votedFor == NullCandidate || rf.votedFor == reqVoteArgs.CandidateId {
		log.Debug("Raft.RequestVote", "raft server %d voted for %d previously, votes for the candidate %d", rf.myIdx, rf.votedFor, reqVoteArgs.CandidateId)
		rf.currentTerm = reqVoteArgs.Term
		rf.votedFor = reqVoteArgs.CandidateId
		rf.state = Follower
		rf.sendHeartbeat()
		reqVoteReply.VoteGranted = true
	}
}

// AppendEntries is invoked by candidates to gather votes
func (rf *Raft) AppendEntries(appendEntriesArgs *AppendEntriesArgs, appendEntriesReply *AppendEntriesReply) { // TODO: 2B
	log.Debug("Raft.AppendEntries", "raft server %d receives heartbeat", rf.myIdx)
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	if rf.currentTerm > appendEntriesArgs.Term {
		appendEntriesReply.Term = rf.currentTerm
		return
	}

	rf.state = Follower
	rf.votedFor = appendEntriesArgs.LeaderId
	rf.currentTerm = appendEntriesArgs.Term
	rf.sendHeartbeat()
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
func (rf *Raft) sendRequestVote(serverIdx int, nVotes *int, reqVoteArgs *RequestVoteArgs) {
	reqVoteReply := &RequestVoteReply{}
	ok := rf.peers[serverIdx].Call("Raft.RequestVote", reqVoteArgs, reqVoteReply)
	if !ok {
		return
	}

	rf.mtx.Lock()
	if reqVoteReply.Term > rf.currentTerm {
		rf.currentTerm = reqVoteReply.Term
		rf.state = Follower
		rf.votedFor = NullCandidate
	}
	if reqVoteReply.VoteGranted && rf.state == Candidate {
		*nVotes++
		if *nVotes > len(rf.peers)/2 {
			log.Debug("sendRequestVote", "raft server %d becomes the leader", rf.myIdx)
			rf.state = Leader
		}
	}
	rf.mtx.Unlock()
}

func (rf *Raft) sendAppendEntries(serverIdx int, appendEntriesArgs *AppendEntriesArgs) {
	appendEntriesReply := &AppendEntriesReply{}
	ok := rf.peers[serverIdx].Call("Raft.AppendEntries", appendEntriesArgs, appendEntriesReply)
	if !ok {
		return
	}

	rf.mtx.Lock()
	if rf.currentTerm < appendEntriesReply.Term {
		log.Debug("sendAppendEntries", "the leader %d is demoted because appendEntriesReply.Term > rf.currentTerm", rf.myIdx)
		rf.currentTerm = appendEntriesReply.Term
		rf.votedFor = NullCandidate
		rf.state = Follower
	}
	rf.mtx.Unlock()
}

func (rf *Raft) sendHeartbeat() {
	select {
	case rf.heartbeatChan <- struct{}{}:
		// sent value to channel
	default:
		// heart channel is full
	}
}

func (rf *Raft) SendAppendEntries() {

}

// The service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) StartAgreement(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Lead does leader's work to lead the followers
func (rf *Raft) Lead() {
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}

		rf.mtx.Lock()
		appendEntriesArgs := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.currentTerm,
		}
		rf.mtx.Unlock()

		go rf.sendAppendEntries(i, &appendEntriesArgs)
	}
	time.Sleep(time.Duration(HeartBeatIntervalMS) * time.Millisecond)
}

// Elect requests for a new election by the candidate
func (rf *Raft) Elect() {
	log.Debug("Raft.startElection", "raft server %d starts the election!", rf.myIdx)
	rf.mtx.Lock()
	rf.currentTerm++
	reqVoteArgs := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.myIdx,
		//LastLogIndex: 0,
		//LastLogTerm: 0,
	}
	rf.mtx.Unlock()

	nVotes := 1
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}
		go rf.sendRequestVote(i, &nVotes, &reqVoteArgs)
	}
}

// Follow does follower's work to follow the leader / candidate
func (rf *Raft) Follow() {
	// check if a leader election should be started.
	select {
	case <-rf.electionTimer.C:
		rf.mtx.Lock()
		rf.votedFor = rf.myIdx
		rf.state = Candidate
		rf.electionTimer.Reset(time.Duration(rf.getRandomTimeoutMS()) * time.Millisecond)
		rf.mtx.Unlock()
	case <-rf.heartbeatChan:
		// log.Debug("Raft.startElection", "raft server %d received heartbeat from channel!", rf.myIdx)
		break
	}
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.state {
		case Leader:
			rf.Lead()
			continue
		case Candidate:
			rf.Elect()
		case Follower:
			rf.Follow()
		}

		// pause for a random amount of time between 1 and 100 milliseconds.
		randSleepMS := rand.Int63()%100 + 1
		time.Sleep(time.Duration(randSleepMS) * time.Millisecond)
	}
}

func (rf *Raft) getRandomTimeoutMS() int {
	return ElectionTimeoutLowerBound + rf.rand.Intn(ElectionTimeoutRange+1)
}

// MakeRaft creates a Raft server. The ports of all the Raft servers (including this one) are in peers[].
// This server's port is peers[myIdx]. All the servers' peers[] arrays have the same order.
// persister is a place for this server to save its persistent state, and also initially holds the most recent saved
// state, if any. applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
// Make must return quickly, so it should start goroutines for any long-running work.
func MakeRaft(peers []*labrpc.ClientEnd, myIdx int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	nServers := len(peers)
	nextIndices := make([]int, nServers)
	for i := range nextIndices {
		nextIndices[i] = 1
	}

	rf := &Raft{
		peers:         peers,
		persister:     persister,
		myIdx:         myIdx,
		state:         Follower,
		logs:          []interface{}{},
		nextIndices:   nextIndices,
		matchIndices:  make([]int, nServers),
		heartbeatChan: make(chan struct{}, 1),
	}

	seed := int64(myIdx) + time.Now().UnixNano()
	rf.rand = rand.New(rand.NewSource(seed))
	rf.electionTimer = time.NewTimer(time.Duration(rf.getRandomTimeoutMS()) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
