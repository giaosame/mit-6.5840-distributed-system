package mr

import (
	"fmt"
	"os"
)

const (
	TaskTypeVoid = iota
	TaskTypeMap
	TaskTypeReduce
)

const (
	TaskStatusReady = iota
	TaskStatusDoing
	TaskStatusDone
)

type DummyArgs struct{}

type DummyReply struct{}

type Task struct {
	Idx    int
	Type   uint8
	Status uint8
}

type TaskReply struct {
	Task      *Task
	Filenames []string
}

type ReportArgs struct {
	Task           *Task
	InterFilenames []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	sock := fmt.Sprintf("/var/tmp/5840-mr-%d.sock", os.Getuid())
	return sock
}
