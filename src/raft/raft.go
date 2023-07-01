package raft

//
// This is an outline of the API that raft must expose to the service (or tester).
// Please see comments below for each of these functions for more details.
//
// rf = MakeRaft(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/common"
	"6.5840/labrpc"
	"6.5840/log"
)

// Raft state
const (
	Follower = iota
	Candidate
	Leader
)

// ApplyMsg will be sent by each Raft peer to the service (or tester) on the same server,
// when the peer becomes aware that successive log entries are committed, via the applyCh passed to Make().
// Set CommandValid to true to indicate that the ApplyMsg contains a newly committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g., snapshots) on the applyCh,
// but set CommandValid to false for these other uses.
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
	applyCh     chan ApplyMsg       // a channel to send ApplyMsg to the service (or tester)
	heartbeatCh chan struct{}       // a channel to receive heartbeat from the leader

	// TODO: 2C. Look at the paper's Figure 2 for a description of what state a Raft server must maintain.

	// ========= persistent state on all servers =========
	state       int        // current state of the server
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	logs        []LogEntry // each entry contains command for state machine, and term when entry was received by leader

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
	currentTerm := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mtx.Unlock()
	return currentTerm, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// TODO: Your code here (2C).
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
	// TODO: Your code here (2C).
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
	// TODO: Your code here (2D).

}

// RequestVoteArgs represents RequestVote RPC arguments structure
type RequestVoteArgs struct {
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
	defer func() {
		rf.mtx.Unlock()
		if heartbeat {
			rf.heartbeatCh <- struct{}{}
		}
	}()

	rf.mtx.Lock()
	log.Debug("Raft.RequestVote", "raft server %d received RequestVote from the candidate %d: args.Term=%d, rf.currentTerm=%d", rf.myIdx, args.CandidateId, args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term == rf.currentTerm && (rf.votedFor != NullCandidate && rf.votedFor != args.CandidateId) {
		log.Debug("Raft.RequestVote", "raft server %d has voted for %d in the term %d", rf.myIdx, rf.votedFor, rf.currentTerm)
		return
	}
	if rf.moreUpToDate(args.LastLogTerm, args.LastLogIndex) {
		log.Debug("Raft.RequestVote", "lastLog={%+v}, args.LastLogTerm=%d, args.LastLogIndex=%d", *rf.getLastLogEntry(), args.LastLogTerm, args.LastLogIndex)
		return
	}

	// grant the vote if:
	// 1. args.Term == rf.currentTerm && rf.votedFor == NullCandidate (not voted yet)
	// 2. args.Term > rf.currentTerm
	log.Debug("Raft.RequestVote", "raft server %d voted for %d previously, votes for the candidate %d", rf.myIdx, rf.votedFor, args.CandidateId)
	log.Debug("Raft.RequestVote", "raft server %d logs: %+v", rf.myIdx, rf.logs)
	rf.currentTerm = args.Term
	rf.state = Follower
	rf.votedFor = args.CandidateId
	reply.Term = args.Term
	reply.VoteGranted = true
	heartbeat = true
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	LeaderCommit int        // leader’s commitIndex
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	heartbeat := false
	rf.mtx.Lock()
	log.Debug("Raft.AppendEntries", "raft server %d received heartbeat=%+v", rf.myIdx, *args)
	defer func() {
		rf.mtx.Unlock()
		if heartbeat {
			rf.heartbeatCh <- struct{}{}
		}
	}()

	myTerm := rf.currentTerm
	leaderTerm := args.Term
	if myTerm > leaderTerm {
		reply.Term = myTerm
		return
	}

	// check prevLogIndex and prevLogTerm
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	if prevLogIndex >= rf.getLogLen() || rf.logs[prevLogIndex].Term != prevLogTerm { // command not equal
		//log.Debug("Raft.AppendEntries", "prevLogIndex >= rf.getLogLen() || rf.logs[prevLogIndex].Term != prevLogTerm for the raft server %d", rf.myIdx)
		return // reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
	}

	// if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if prevLogIndex+1 < rf.getLogLen() {
		// log.Debug("Raft.AppendEntries", "prevLogIndex+1 < rf.getLogLen() for the raft server %d", rf.myIdx)
		rf.logs = rf.logs[:prevLogIndex+1]
	}

	// append any new entries to the logs
	if len(args.Entries) > 0 {
		log.Debug("Raft.AppendEntries", "len(args.Entries) > 0 for the raft server %d, entries = %+v", rf.myIdx, args.Entries)
		rf.logs = append(rf.logs, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		log.Debug("Raft.RequestVote", "raft server %d received AppendEntries from the leader %d: args = {term=%d, PrevLogIndex=%d, PrevLogTerm=%d, LeaderCommit=%d}",
			rf.myIdx, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		log.Debug("Raft.AppendEntries", "args.LeaderCommit{%d} > rf.commitIndex{%d} for the raft server %d", args.LeaderCommit, rf.commitIndex, rf.myIdx)
		rf.commitIndex = common.Min(args.LeaderCommit, rf.getLogLen()-1)
	}
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		appliedEntry := rf.logs[rf.lastApplied]
		rf.sendApplyMsg(appliedEntry.Command, appliedEntry.Idx)
		//time.Sleep(ApplyMsgIntervalMS * time.Millisecond)
	}

	rf.state = Follower
	rf.votedFor = args.LeaderId
	rf.currentTerm = leaderTerm
	heartbeat = true
	reply.Success = true
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
func (rf *Raft) sendRequestVote(wg *sync.WaitGroup, serverIdx int, nVotes *int) {
	defer wg.Done()

	rf.mtx.Lock()
	if rf.state != Candidate {
		rf.mtx.Unlock()
		return
	}
	lastLog := rf.getLastLogEntry()
	reqVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.myIdx,
		LastLogIndex: lastLog.Idx,
		LastLogTerm:  lastLog.Term,
	}
	reqVoteReply := RequestVoteReply{}
	ok := false
	done := make(chan bool)

	go func() {
		// serverIdx is the index of the target server in rf.peers
		ok = rf.peers[serverIdx].Call("Raft.RequestVote", &reqVoteArgs, &reqVoteReply)
		done <- true
	}()
	rf.mtx.Unlock()

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

	rf.mtx.Lock()
	if reqVoteReply.Term > rf.currentTerm {
		rf.demote(reqVoteReply.Term)
	}
	if reqVoteReply.VoteGranted && rf.state == Candidate {
		*nVotes++
	}
	rf.mtx.Unlock()
}

func (rf *Raft) sendAppendEntriesAsync(wg *sync.WaitGroup, serverIdx int) {
	defer wg.Done()
	rf.mtx.Lock()
	prevLogIndex := rf.nextIndices[serverIdx] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	appendEntriesArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.myIdx,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.logs[rf.nextIndices[serverIdx]:], // include all log entries from (prevLogIndex + 1, ... len(logs)-1)
	}
	rf.mtx.Unlock()
	// log.Debug("Raft.sendAppendEntriesAsync", "appendEntriesArgs = {%+v}", appendEntriesArgs)

	var appendEntriesReply AppendEntriesReply
	for {
		appendEntriesReply = AppendEntriesReply{}
		ok := false
		done := make(chan bool)

		rf.mtx.Lock()
		if rf.state != Leader { // make sure this raft server is still the leader when sending AppendEntries
			rf.mtx.Unlock()
			return
		}
		go func() {
			ok = rf.peers[serverIdx].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply)
			done <- true
		}()
		rf.mtx.Unlock()

		timeout := time.After(time.Duration(RespWaitingTimeoutMS) * time.Millisecond)
		select {
		case <-done:
			break
		case <-timeout:
			// log.Debug("Raft.sendAppendEntriesAsync", "timed out to get the response from the raft server %d", serverIdx)
			return
		}
		if !ok { // no response received, this server is down
			return
		}

		rf.mtx.Lock()
		if appendEntriesReply.Term > rf.currentTerm {
			log.Debug("Raft.sendAppendEntriesAsync",
				"the leader %d is demoted because it's term is smaller than the term of the raft server %d",
				rf.myIdx, serverIdx)
			rf.demote(appendEntriesReply.Term)
			rf.mtx.Unlock()
			return
		}
		rf.mtx.Unlock()

		if appendEntriesReply.Success {
			break
		}

		// else AppendEntries fails because of log inconsistency, decrement nextIndex and retry
		rf.mtx.Lock()
		addFirst(&appendEntriesArgs.Entries, rf.logs[prevLogIndex])
		prevLogIndex--
		if prevLogIndex < 0 {
			// it should not be less than 0, if it is, then it means that the currentTerm is less than the reply.Term
			rf.mtx.Unlock()
			return
		}
		prevLogTerm = rf.logs[prevLogIndex].Term
		rf.mtx.Unlock()

		appendEntriesArgs.PrevLogIndex = prevLogIndex
		appendEntriesArgs.PrevLogTerm = prevLogTerm
	}

	// update nextIndex and matchIndex for this follower
	if len(appendEntriesArgs.Entries) > 0 {
		rf.mtx.Lock()
		rf.nextIndices[serverIdx] = rf.getLogLen()
		rf.matchIndices[serverIdx] = prevLogIndex + len(appendEntriesArgs.Entries)
		rf.mtx.Unlock()
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

// updateCommitIndex is used by the leader to update its own commitIndex
func (rf *Raft) updateCommitIndex() {
	log.Debug("Raft.updateCommitIndex", "raft server %d tries to update the commitIndex", rf.myIdx)
	// If there exists an N such that N > commitIndex,
	// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	if rf.state != Leader {
		return
	}

	for n := rf.getLogLen() - 1; n > rf.commitIndex; n-- {
		nMatch := 0
		for i := range rf.peers {
			if rf.matchIndices[i] >= n {
				nMatch++
			}
		}

		if nMatch > len(rf.peers)/2 && rf.logs[n].Term == rf.currentTerm {
			log.Debug("Raft.updateCommitIndex", "n = %d, nMatch = %d, rf.logs[n] = %+v", n, nMatch, rf.logs[n])
			log.Debug("Raft.updateCommitIndex", "leader updates its commitIndex to %d", n)
			rf.commitIndex = n
			break
		}
	}

	if rf.commitIndex > rf.lastApplied {
		log.Debug("Raft.updateCommitIndex", "logs of leader(%d): %+v", rf.myIdx, rf.logs)
		rf.lastApplied++
		appliedEntry := rf.logs[rf.lastApplied]
		go rf.sendApplyMsg(appliedEntry.Command, appliedEntry.Idx)
	}
}

func (rf *Raft) startElection() {
	var wg sync.WaitGroup
	rf.mtx.Lock()
	rf.state = Candidate
	rf.votedFor = rf.myIdx
	rf.currentTerm++
	rf.mtx.Unlock()
	log.Debug("Raft.startElection", "raft server %d starts the election at term %d!", rf.myIdx, rf.currentTerm)

	nVotes := 1 // number of received votes
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}
		wg.Add(1)
		go rf.sendRequestVote(&wg, i, &nVotes)
	}

	wg.Wait()
	log.Debug("Raft.startElection", "raft server %d got %d votes", rf.myIdx, nVotes)
	rf.mtx.Lock()
	if nVotes > len(rf.peers)/2 && rf.state == Candidate {
		rf.state = Leader
		// rf.logs = rf.logs[:rf.commitIndex+1] // TODO: need to think twice about removing uncommitted log entries
		for i := range rf.nextIndices {
			rf.nextIndices[i] = rf.getLogLen()
		}
	} else {
		rf.demote(rf.currentTerm)
	}
	rf.mtx.Unlock()
}

func (rf *Raft) sendAppendEntries() {
	log.Debug("Raft.sendAppendEntries", "raft server %d begins to send sendAppendEntries...", rf.myIdx)
	rf.mtx.Lock()
	if rf.getLogLen() > 100 {
		log.Debug("Raft.startElection", "leader(%d)'s logs: %+v", rf.myIdx, rf.logs)
	}
	rf.mtx.Unlock()
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}
		wg.Add(1)
		go rf.sendAppendEntriesAsync(&wg, i)
	}
	wg.Wait()
}

func (rf *Raft) sendApplyMsg(command interface{}, index int) {
	rf.applyCh <- ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: index,
	}
}

// demote demotes the leader itself to be a follower
func (rf *Raft) demote(newTerm int) {
	rf.state = Follower
	rf.votedFor = NullCandidate
	rf.currentTerm = newTerm
}

// StartAgreement is called by the service using Raft (e.g. a k/v server)
// which wants to start agreement on the next command to be appended to Raft's log.
//   - If this server isn't the leader, returns false.
//   - Otherwise, start the agreement and return immediately.
//
// There is no guarantee that this command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed, this function should return gracefully.
//
// Return values:
// * index:    the index that the command will appear at if it's ever committed
// * term:     the current term
// * isLeader: true if this server believes it is the leader
func (rf *Raft) StartAgreement(command interface{}) (int, int, bool) {
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	log.Debug("Raft.StartAgreement", "raft server %d begins to start agreement on the command {%v}...", rf.myIdx, command)

	// add the new command into logs
	index := rf.getLogLen()
	term := rf.currentTerm
	entry := &LogEntry{
		Idx:     index,
		Term:    term,
		Command: command,
	}
	rf.pushBack(entry)

	rf.nextIndices[rf.myIdx] = rf.getLogLen()
	rf.matchIndices[rf.myIdx] = rf.getLogLen() - 1

	// go rf.sendAppendEntries()
	return index, term, true
}

// mainLoop is the Raft main loop doing the main business in the background
func (rf *Raft) mainLoop() {
	for rf.killed() == false {
		// check if a leader election should be started
		_, isLeader := rf.GetState()
		if isLeader { // I'm the leader, send heartbeat to maintain their authority
			rf.sendAppendEntries()
			rf.updateCommitIndex()
			time.Sleep(time.Duration(HeartBeatIntervalMS) * time.Millisecond)
			continue
		}

		// I ain't the leader, pause for a random amount of time between 50 and 350 milliseconds.
		ms := ElectionTimeoutLowerBound + (rand.Int63() % ElectionTimeoutUpBoundRange)
		//log.Debug("Raft.heartbeat", "raft server %d has ms = %d", rf.myIdx, ms)
		electionTimeout := time.After(time.Duration(ms) * time.Millisecond)
		select {
		case <-electionTimeout:
			rf.startElection()
		case <-rf.heartbeatCh:
			break
		}
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

	// TODO: Your initialization code here (2C).
	rf.state = Follower
	rf.votedFor = NullCandidate
	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan struct{})
	rf.logs = []LogEntry{{}} // to make logs' first index 1
	rf.nextIndices = make([]int, len(peers))
	rf.matchIndices = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// set up the random generator
	seed := int64(myIdx)
	rf.myRand = rand.New(rand.NewSource(seed))

	// start heartbeat goroutine to start elections
	go rf.mainLoop()
	return rf
}
