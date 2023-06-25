package raft

type LogEntry struct {
	Idx     int // index to identify its position in the log
	Term    int
	Command interface{}
}

type LogEntries []LogEntry

func (e *LogEntry) equals(entry *LogEntry) bool {
	return e.Idx == entry.Idx && e.Term == entry.Term
}

func addFirst(es *[]LogEntry, e LogEntry) {
	*es = append([]LogEntry{e}, *es...)
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

func (rf *Raft) pushBack(e *LogEntry) {
	rf.logs = append(rf.logs, *e)
}
