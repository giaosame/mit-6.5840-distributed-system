package raft

import "fmt"

type LogEntry struct {
	Idx     int // index to identify its position in the log
	Term    int
	Command interface{}
}

// addFirst adds the given LogEntry e to the first position of LogEntries es
func addFirst(es *[]LogEntry, e *LogEntry) {
	*es = append([]LogEntry{*e}, *es...)
}

type LogEntries []LogEntry

func (e *LogEntry) equals(entry *LogEntry) bool {
	return e.Idx == entry.Idx && e.Term == entry.Term
}

// moreUpToDate returns true if the last log of the current raft server rf is more up-to-date
func (rf *Raft) moreUpToDate(lastLogTerm, lastLogIndex int) bool {
	lastLogEntry := rf.getLastLogEntry()
	if lastLogEntry.Term != lastLogTerm {
		return lastLogEntry.Term > lastLogTerm
	}
	return lastLogEntry.Idx > lastLogIndex
}

// getLastLogEntry returns the last entry of logs
func (rf *Raft) getLastLogEntry() *LogEntry {
	return &rf.logs[len(rf.logs)-1]
}

// getLogLen returns the length of logs
func (rf *Raft) getLogLen() int {
	return len(rf.logs)
}

// pushBack pushes the given LogEntries to the end of log
func (rf *Raft) pushBack(es ...LogEntry) {
	rf.logs = append(rf.logs, es...)
	rf.persist()
}

func (rf *Raft) getPersistentState() string {
	return fmt.Sprintf("state=%d, term=%d, votedFor=%d, logs=%+v", rf.state, rf.currentTerm, rf.votedFor, rf.logs)
}
