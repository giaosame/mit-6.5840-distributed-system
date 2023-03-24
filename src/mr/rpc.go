package mr

import (
	"fmt"
	"os"
)

const (
	TaskTypeVoid = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeEnd
)

const (
	TaskStatusDoing = iota
	TaskStatusDone
	TaskStatusFailed
)

type Task struct {
	Idx       int
	Type      uint8
	Status    uint8
	Filenames []string
}

type DummyArgs struct{}

type ReportReply int

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	sock := fmt.Sprintf("/var/tmp/5840-mr-%d.sock", os.Getuid())
	return sock
}
