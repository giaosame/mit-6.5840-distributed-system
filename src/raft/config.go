package raft

//
// support for Raft tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"bytes"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	mylog "6.5840/log"
)

func randString(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	n         int
	rpcs0     int // rpcTotal() at start of test
	cmds0     int // number of agreements
	bytes0    int64
	maxIndex  int
	maxIndex0 int
	finished  int32

	t     *testing.T
	net   *labrpc.Network
	mu    sync.Mutex
	start time.Time // time at which makeConfig() was called
	t0    time.Time // time at which test_test.go called cfg.begin()
	// begin()/end() statistics

	rafts       []*Raft
	applyErr    []string // from apply channel readers
	connected   []bool   // whether each server is on the net
	saved       []*Persister
	endNames    [][]string            // the port file names each sends to
	logs        []map[int]interface{} // copy of each server's committed entries
	lastApplied []int
}

var nCPUOnce sync.Once

func makeConfig(t *testing.T, n int, unreliable bool, snapshot bool) *config {
	nCPUOnce.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endNames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]interface{}, cfg.n)
	cfg.lastApplied = make([]int, cfg.n)
	cfg.start = time.Now()

	cfg.setUnreliable(unreliable)
	cfg.net.LongDelays(true)

	applier := cfg.applier
	if snapshot {
		applier = cfg.applierSnap
	}

	// create a full set of Rafts
	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]interface{}{}
		cfg.start1(i, applier)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// crash1 shuts down a Raft server but saves its persistent state
func (cfg *config) crash1(i int) { // TODO: rename to crashRaft
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh persister, in case old instance continues to update the Persister.
	// but copy old persister's content so that we always pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.rafts[i]
	if rf != nil {
		cfg.mu.Unlock()
		rf.Kill()
		cfg.mu.Lock()
		cfg.rafts[i] = nil
	}

	if cfg.saved[i] != nil {
		raftLog := cfg.saved[i].ReadRaftState()
		snapshot := cfg.saved[i].ReadSnapshot()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].Save(raftLog, snapshot)
	}
}

func (cfg *config) checkLogs(i int, m ApplyMsg) (string, bool) {
	errMsg := ""
	v := m.Command
	for j := 0; j < len(cfg.logs); j++ {
		if old, oldOk := cfg.logs[j][m.CommandIndex]; oldOk && old != v {
			log.Printf("%v: log %v; server %v\n", i, cfg.logs[i], cfg.logs[j])
			// some server has already committed a different value for this entry!
			errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
				m.CommandIndex, i, m.Command, j, old)
		}
	}
	_, prevok := cfg.logs[i][m.CommandIndex-1]
	cfg.logs[i][m.CommandIndex] = v
	if m.CommandIndex > cfg.maxIndex {
		cfg.maxIndex = m.CommandIndex
	}
	return errMsg, prevok
}

// applier reads message from apply ch and checks that they match the log contents
func (cfg *config) applier(i int, applyCh chan ApplyMsg) {
	for m := range applyCh {
		if m.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			cfg.mu.Lock()
			errMsg, prevok := cfg.checkLogs(i, m)
			cfg.mu.Unlock()
			if m.CommandIndex > 1 && prevok == false {
				errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
			}
			if errMsg != "" {
				log.Fatalf("apply error: %v", errMsg)
				cfg.applyErr[i] = errMsg
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
		}
	}
}

// returns "" or error string
func (cfg *config) ingestSnap(i int, snapshot []byte, index int) string {
	if snapshot == nil {
		log.Fatalf("nil snapshot")
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []interface{}
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		log.Fatalf("snapshot decode error")
		return "snapshot Decode() error"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", i)
		return err
	}
	cfg.logs[i] = map[int]interface{}{}
	for j := 0; j < len(xlog); j++ {
		cfg.logs[i][j] = xlog[j]
	}
	cfg.lastApplied[i] = lastIncludedIndex
	return ""
}

const SnapShotInterval = 10

// applierSnap takes snapshot raft state periodically
func (cfg *config) applierSnap(i int, applyCh chan ApplyMsg) {
	cfg.mu.Lock()
	rf := cfg.rafts[i]
	cfg.mu.Unlock()
	if rf == nil {
		return // ???
	}

	for m := range applyCh {
		errMsg := ""
		if m.SnapshotValid {
			cfg.mu.Lock()
			errMsg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
			cfg.mu.Unlock()
		} else if m.CommandValid {
			if m.CommandIndex != cfg.lastApplied[i]+1 {
				errMsg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", i, cfg.lastApplied[i]+1, m.CommandIndex)
			}

			if errMsg == "" {
				cfg.mu.Lock()
				var prevok bool
				errMsg, prevok = cfg.checkLogs(i, m)
				cfg.mu.Unlock()
				if m.CommandIndex > 1 && prevok == false {
					errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			}

			cfg.mu.Lock()
			cfg.lastApplied[i] = m.CommandIndex
			cfg.mu.Unlock()

			if (m.CommandIndex+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xLog []interface{}
				for j := 0; j <= m.CommandIndex; j++ {
					xLog = append(xLog, cfg.logs[i][j])
				}
				e.Encode(xLog)
				rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		} else {
			// Ignore other types of ApplyMsg.
		}
		if errMsg != "" {
			log.Fatalf("apply error: %v", errMsg)
			cfg.applyErr[i] = errMsg
			// keep reading after error so that Raft doesn't block
			// holding locks...
		}
	}
}

// start1 starts or re-start a Raft.
// If one already exists, "kill" it first.
// Then, allocate new outgoing port file names, and a new state persister,
// to isolate previous instance of this server. since we cannot really kill it.
func (cfg *config) start1(i int, applier func(int, chan ApplyMsg)) { // TODO: rename to startRaft
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endNames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endNames[i][j] = randString(20)
	}

	// a fresh set of ClientEnds
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endNames[i][j])
		cfg.net.Connect(cfg.endNames[i][j], j)
	}

	cfg.mu.Lock()
	cfg.lastApplied[i] = 0

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()

		snapshot := cfg.saved[i].ReadSnapshot()
		if snapshot != nil && len(snapshot) > 0 {
			// mimic KV server and process snapshot now.
			// ideally Raft should send it up on applyCh...
			err := cfg.ingestSnap(i, snapshot, -1)
			if err != "" {
				cfg.t.Fatal(err)
			}
		}
	} else {
		cfg.saved[i] = MakePersister()
	}
	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	rf := MakeRaft(ends, i, cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	go applier(i, applyCh)

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) checkTimeout() {
	// enforce a two-minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) checkFinished() bool {
	z := atomic.LoadInt32(&cfg.finished)
	return z != 0
}

// cleanup kills all raft servers and clean up the network
func (cfg *config) cleanup() {
	atomic.StoreInt32(&cfg.finished, 1)
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// connect attaches server i to the net.
func (cfg *config) connect(i int) {
	mylog.Debug("config.connect", "connect the raft server %d", i)
	cfg.connected[i] = true

	// TODO: merge the outgoing loop and incoming loop into one
	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endName := cfg.endNames[i][j]
			cfg.net.Enable(endName, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endName := cfg.endNames[j][i]
			cfg.net.Enable(endName, true)
		}
	}
}

// disconnect detaches server i from the net.
func (cfg *config) disconnect(i int) {
	mylog.Debug("config.disconnect", "disconnect the raft server %d", i)
	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endNames[i] != nil {
			endName := cfg.endNames[i][j]
			cfg.net.Enable(endName, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endNames[j] != nil {
			endName := cfg.endNames[j][i]
			cfg.net.Enable(endName, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setUnreliable(unreliable bool) {
	cfg.net.Reliable(!unreliable)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setLongReordering(longRel bool) {
	cfg.net.LongReordering(longRel)
}

// checkOneLeader checks that one of the connected servers thinks it is the leader,
// and that no other connected server thinks otherwise.
// Try a few times in case re-elections are needed.
func (cfg *config) checkOneLeader() int {
	for iter := 0; iter < 10; iter++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				if term, isLeader := cfg.rafts[i].GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, termLeaders := range leaders {
			nTermLeaders := len(termLeaders)
			if nTermLeaders > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", term, nTermLeaders)
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// checkTerms checks that everyone agrees on the term.
func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = xterm
			} else if term != xterm {
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

// checkNoLeader checks that none of the connected servers thinks it is the leader.
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, isLeader := cfg.rafts[i].GetState()
			if isLeader {
				cfg.t.Fatalf("expected no leader among connected servers, but %v claims to be leader", i)
			}
		}
	}
}

// nCommitted returns how many servers think a log entry is committed
func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	var cmd interface{} = nil
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && cmd != cmd1 {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// wait waits for at least n servers to commit, but it doesn't wait forever.
func (cfg *config) wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range cfg.rafts {
				if t, _ := r.GetState(); t > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := cfg.nCommitted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d",
			nd, index, n)
	}
	return cmd
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, may submit the command multiple
// times, in case a leader fails just after Start().
// if retry==false, calls Start() only once, in order
// to simplify the early Lab 2B tests.
func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 && cfg.checkFinished() == false {
		// try all the servers, maybe one is the leader.
		index := -1
		for si := 0; si < cfg.n; si++ {
			starts = (starts + 1) % cfg.n
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil {
				index1, _, ok := rf.Start(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd1 == cmd {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if cfg.checkFinished() == false {
		cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	}
	return -1
}

// begin starts a Test and prints the Test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.cmds0 = 0
	cfg.maxIndex0 = cfg.maxIndex
}

// end ends a Test -- the fact that we got here means there was no failure.
// Print the Passed message, and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {
		cfg.mu.Lock()
		t := time.Since(cfg.t0).Seconds()       // real time
		nPeers := cfg.n                         // number of Raft peers
		nRPC := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nBytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		nCmds := cfg.maxIndex - cfg.maxIndex0   // number of Raft agreements reported
		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, nPeers, nRPC, nBytes, nCmds)
	}
}

// LogSize returns the maximum log size across all servers
func (cfg *config) LogSize() int {
	logSize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logSize {
			logSize = n
		}
	}
	return logSize
}
