package mr

import (
	"fmt"
	"os"
)

type DummyArgs struct{}

type ReduceNumReply struct {
	Num int
}

type TaskReply struct {
	Filename string
}

type TestReply struct {
	Val int
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
