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
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/common"
	"6.5840/labgob"
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
	mtx       sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	myIdx     int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// TODO: 2C. Look at the paper's Figure 2 for a description of what state a Raft server must maintain.
	rand          *rand.Rand // random number source
	electionTimer *time.Timer
	heartbeatChan chan struct{} // a channel to receive heartbeat from the leader
	applyChan     chan ApplyMsg // a channel to send ApplyMsg to the service (or tester)

	// ========= persistent state on all servers =========
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	state       int
	votedFor    int        // candidateId that received vote in current term (or null if none)
	logs        []LogEntry // each entry contains command for state machine, and term when entry was received by leader

	// ========= volatile state on all servers =========
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine

	// ========= volatile state on all servers =========
	nextIndices  []int // for each server, index of the next log entry to send to that server
	matchIndices []int // for each server, index of the highest log entry known to be replicated on server
}

// persist saves Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := rf.encode(e); err != nil {
		log.Error("rf.persist", "raft server %d %v", rf.myIdx, err)
		return
	}
	log.Debug("raft.persist", "raft server %d encoded persistent state: %s", rf.myIdx, rf.getPersistentState())
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// encode encodes the persistent state of this raft server
func (rf *Raft) encode(e *labgob.LabEncoder) error {
	if err := e.Encode(rf.state); err != nil {
		return errors.New("failed to encode state: " + err.Error())
	}
	if err := e.Encode(rf.currentTerm); err != nil {
		return errors.New("failed to encode currentTerm: " + err.Error())
	}
	if err := e.Encode(rf.votedFor); err != nil {
		return errors.New("failed to encode votedFor: " + err.Error())
	}
	if err := e.Encode(rf.logs); err != nil {
		return errors.New("failed to encode logs: " + err.Error())
	}
	return nil
}

// decode decodes the persistent state of this raft server
func (rf *Raft) decode(d *labgob.LabDecoder) error {
	if err := d.Decode(&rf.state); err != nil {
		return errors.New("failed to decode state: " + err.Error())
	}
	if err := d.Decode(&rf.currentTerm); err != nil {
		return errors.New("failed to decode currentTerm: " + err.Error())
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		return errors.New("failed to decode votedFor: " + err.Error())
	}
	if err := d.Decode(&rf.logs); err != nil {
		return errors.New("failed to decode logs: " + err.Error())
	}
	return nil
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	buf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buf)
	if err := rf.decode(d); err != nil {
		log.Error("[rf.readPersist]", "raft server %d %v", rf.myIdx, err)
	}
	log.Debug("raft.readPersist", "raft server %d reads persistent state from decoder: %s",
		rf.myIdx, rf.getPersistentState())
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// TODO: Your code here (2D).

}

// RequestVote is invoked by candidates to gather votes
func (rf *Raft) RequestVote(reqVoteArgs *RequestVoteArgs, reqVoteReply *RequestVoteReply) {
	log.Debug("Raft.RequestVote", "raft server %d receives the request of vote: %+v", rf.myIdx, *reqVoteArgs)
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	if rf.currentTerm > reqVoteArgs.Term {
		log.Debug("Raft.RequestVote", "rf.currentTerm(%d) > reqVoteArgs.Term(%d)", rf.currentTerm, reqVoteArgs.Term)
		reqVoteReply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < reqVoteArgs.Term || rf.votedFor == NullCandidate || rf.votedFor == reqVoteArgs.CandidateId {
		log.Debug("Raft.RequestVote", "raft server %d voted for %d previously, votes for the candidate %d", rf.myIdx, rf.votedFor, reqVoteArgs.CandidateId)
		rf.demote(reqVoteArgs.Term)

		if rf.moreUpToDate(reqVoteArgs.LastLogTerm, reqVoteArgs.LastLogIndex) {
			log.Debug("Raft.RequestVote", "moreUpToDate: lastLog={%+v}, args.LastLogTerm=%d, args.LastLogIndex=%d", *rf.getLastLogEntry(), reqVoteArgs.LastLogTerm, reqVoteArgs.LastLogIndex)
			return
		}

		// candidate’s log is at least as up-to-date as receiver’s log
		rf.passHeartbeat()
		rf.votedFor = reqVoteArgs.CandidateId
		rf.persist()
		reqVoteReply.VoteGranted = true
	}
}

// AppendEntries is invoked by candidates to gather votes
func (rf *Raft) AppendEntries(appendEntriesArgs *AppendEntriesArgs, appendEntriesReply *AppendEntriesReply) {
	log.Debug("Raft.AppendEntries", "raft server %d receives heartbeat: %+v", rf.myIdx, appendEntriesArgs)
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	prevLogIndex := appendEntriesArgs.PrevLogIndex
	prevLogTerm := appendEntriesArgs.PrevLogTerm
	if rf.currentTerm > appendEntriesArgs.Term {
		appendEntriesReply.Term = rf.currentTerm
		return
	}
	if rf.getLogLen() <= prevLogIndex || rf.logs[prevLogIndex].Term != prevLogTerm {
		return
	}

	// check if an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	if prevLogIndex+1 < rf.getLogLen() {
		log.Debug("Raft.AppendEntries", "prevLogIndex+1 < rf.getLogLen() for the raft server %d", rf.myIdx)
		rf.logs = rf.logs[:prevLogIndex+1]
	}
	// append any new entries to the logs
	if len(appendEntriesArgs.Entries) > 0 {
		log.Debug("Raft.AppendEntries", "len(args.Entries) > 0 for the raft server %d, entries = %+v", rf.myIdx, appendEntriesArgs.Entries)
		rf.pushBack(appendEntriesArgs.Entries...)
	}

	if appendEntriesArgs.LeaderCommit > rf.commitIndex {
		// log.Debug("Raft.AppendEntries", "args.LeaderCommit{%d} > rf.commitIndex{%d} for the raft server %d", appendEntriesArgs.LeaderCommit, rf.commitIndex, rf.myIdx)
		rf.commitIndex = common.Min(appendEntriesArgs.LeaderCommit, rf.getLogLen()-1)
	}
	if rf.commitIndex > rf.lastApplied {
		go rf.sendApplyMsg(rf.lastApplied, rf.commitIndex)
		rf.lastApplied = rf.commitIndex
	}

	rf.passHeartbeat()
	rf.state = Follower
	rf.currentTerm = appendEntriesArgs.Term
	appendEntriesReply.Success = true
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
	if rf.currentTerm < reqVoteReply.Term {
		rf.demote(reqVoteReply.Term)
	}
	if reqVoteReply.VoteGranted && rf.state == Candidate {
		*nVotes++
		if *nVotes > len(rf.peers)/2 {
			log.Debug("Raft.sendRequestVote", "raft server %d becomes the leader", rf.myIdx)
			rf.promote()
			rf.passHeartbeat()
		}
	}
	rf.mtx.Unlock()
}

func (rf *Raft) sendAppendEntries(serverIdx int, appendEntriesArgs *AppendEntriesArgs) {
	prevLogIndex := appendEntriesArgs.PrevLogIndex
	var appendEntriesReply *AppendEntriesReply
	for {
		appendEntriesReply = &AppendEntriesReply{}
		ok := rf.peers[serverIdx].Call("Raft.AppendEntries", appendEntriesArgs, appendEntriesReply)
		if !ok {
			return
		}
		if appendEntriesReply.Success {
			break
		}

		rf.mtx.Lock()
		if rf.currentTerm < appendEntriesReply.Term {
			log.Debug("Raft.sendAppendEntries", "the leader %d is demoted because appendEntriesReply.Term > rf.currentTerm", rf.myIdx)
			rf.demote(appendEntriesReply.Term)
			rf.mtx.Unlock()
			return
		}

		// add the missing log entry first
		addFirst(&appendEntriesArgs.Entries, &rf.logs[prevLogIndex])
		prevLogIndex--
		if prevLogIndex < 0 {
			rf.mtx.Unlock()
			return
		}
		appendEntriesArgs.PrevLogIndex = prevLogIndex
		appendEntriesArgs.PrevLogTerm = rf.logs[prevLogIndex].Term
		rf.mtx.Unlock()
	}

	// update nextIndex and matchIndex for this server
	if len(appendEntriesArgs.Entries) > 0 {
		rf.mtx.Lock()
		rf.nextIndices[serverIdx] = rf.getLogLen()
		rf.matchIndices[serverIdx] = prevLogIndex + len(appendEntriesArgs.Entries)
		rf.mtx.Unlock()
	}
}

func (rf *Raft) sendApplyMsg(start, end int) {
	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	for i := start; i <= end; i++ {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: rf.logs[i].Idx,
		}
	}
}

func (rf *Raft) passHeartbeat() {
	select {
	case rf.heartbeatChan <- struct{}{}:
		// sent value to channel
	default:
		// heart channel is full
	}
}

// promote promotes the candidate itself to be the leader
func (rf *Raft) promote() {
	rf.state = Leader
	rf.persist()
	for i := range rf.peers {
		rf.nextIndices[i] = rf.getLogLen()
		rf.matchIndices[i] = 0
	}
}

// demote demotes the leader itself to be a follower
func (rf *Raft) demote(newTerm int) {
	if rf.state != Follower {
		rf.electionTimer.Reset(time.Duration(rf.getRandomTimeoutMS()) * time.Millisecond)
	}
	rf.state = Follower
	rf.votedFor = NullCandidate
	rf.currentTerm = newTerm
	rf.persist()
}

func (rf *Raft) getRandomTimeoutMS() int {
	return ElectionTimeoutLowerBound + rf.rand.Intn(ElectionTimeoutRange+1)
}

// updateCommitIndex is used by the leader to update its own commitIndex
func (rf *Raft) updateCommitIndex() {
	log.Debug("Raft.updateCommitIndex", "leader %d tries to update the commitIndex", rf.myIdx)
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
			log.Debug("Raft.updateCommitIndex", "leader updates its commitIndex to %d", n)
			rf.commitIndex = n
			break
		}
	}

	if rf.commitIndex > rf.lastApplied {
		// log.Debug("Raft.updateCommitIndex", "logs of leader(%d): %+v", rf.myIdx, rf.logs)
		rf.lastApplied++
		go rf.sendApplyMsg(rf.lastApplied, rf.lastApplied)
	}
}

// lead replicates logs by sending AppendEntries RPC in parallel
func (rf *Raft) lead() {
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}

		rf.mtx.Lock()
		nextIndex := rf.nextIndices[i]
		appendEntriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.myIdx,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.logs[nextIndex-1].Term,
			LeaderCommit: rf.commitIndex,
		}
		if rf.getLogLen() > nextIndex {
			appendEntriesArgs.Entries = rf.logs[nextIndex:]
		}
		rf.mtx.Unlock()

		go rf.sendAppendEntries(i, &appendEntriesArgs)
	}

	time.Sleep(time.Duration(HeartBeatIntervalMS) * time.Millisecond)
	rf.updateCommitIndex()
}

// elect starts a new election by sending requests of vote in parallel
func (rf *Raft) elect() {
	log.Debug("Raft.elect", "raft server %d starts the election!", rf.myIdx)
	rf.mtx.Lock()
	rf.currentTerm++
	rf.persist()
	lastLog := rf.getLastLogEntry()
	reqVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.myIdx,
		LastLogIndex: lastLog.Idx,
		LastLogTerm:  lastLog.Term,
	}
	rf.mtx.Unlock()

	nVotes := 1
	for i := range rf.peers {
		if i == rf.myIdx {
			continue
		}
		go rf.sendRequestVote(i, &nVotes, &reqVoteArgs)
	}

	select {
	case <-rf.electionTimer.C: // pause for a random amount of time
		rf.electionTimer.Reset(time.Duration(rf.getRandomTimeoutMS()) * time.Millisecond)
	case <-rf.heartbeatChan:
		break
	}
}

// follow waits for either leader's heartbeat or the election timeout
func (rf *Raft) follow() {
	// check if a leader election should be started.
	select {
	case <-rf.electionTimer.C:
		rf.mtx.Lock()
		rf.votedFor = rf.myIdx
		rf.state = Candidate
		rf.mtx.Unlock()
	case <-rf.heartbeatChan:
		// log.Debug("Raft.startElection", "raft server %d received heartbeat from channel!", rf.myIdx)
		break
	}
	rf.electionTimer.Reset(time.Duration(rf.getRandomTimeoutMS()) * time.Millisecond)
}

func (rf *Raft) tick() {
	for rf.killed() == false {
		rf.mtx.Lock()
		state := rf.state
		rf.mtx.Unlock()

		switch state {
		case Leader:
			rf.lead()
		case Candidate:
			rf.elect()
		case Follower:
			rf.follow()
		}
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// =====================================================================================================================
// ====================================== ****** PUBLIC FUNCTIONS ****** ===============================================
// =====================================================================================================================

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
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
		return -1, -1, false
	}

	index := rf.getLogLen()
	term := rf.currentTerm
	entry := LogEntry{
		Idx:     index,
		Term:    term,
		Command: command,
	}
	rf.pushBack(entry)
	log.Debug("rf.StartAgreement", "leader %d starts to do agreement on %+v", rf.myIdx, entry)

	rf.nextIndices[rf.myIdx] = rf.getLogLen()
	rf.matchIndices[rf.myIdx] = rf.getLogLen() - 1
	return index, term, true
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
		logs:          []LogEntry{{}}, // to make logs' first index 1
		nextIndices:   nextIndices,
		matchIndices:  make([]int, nServers),
		heartbeatChan: make(chan struct{}, 1),
		applyChan:     applyCh,
	}

	seed := int64(myIdx) + time.Now().UnixNano()
	rf.rand = rand.New(rand.NewSource(seed))
	rf.electionTimer = time.NewTimer(time.Duration(rf.getRandomTimeoutMS()) * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.tick()
	return rf
}
