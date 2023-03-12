package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	files []string
	mtx   sync.Mutex

	nMap      int
	mapIdx    int
	nReduce   int
	reduceIdx int
}

// RPC handlers for the worker to call.

// GetNReduce returns nReduce
func (c *Coordinator) GetNReduce(args *DummyArgs, reply *ReduceNumReply) error {
	reply.Num = c.nReduce
	return nil
}

// GetMapTask returns a map task including the filename
func (c *Coordinator) GetMapTask(args *DummyArgs, reply *TaskReply) error {
	c.mtx.Lock()
	idx := c.mapIdx
	c.mapIdx++
	c.mtx.Unlock()

	if idx < c.nMap {
		reply.Filename = c.files[idx]
	}
	return nil
}

// serve starts a thread that listens for RPCs from worker.go
func (c *Coordinator) serve() {
	if err := rpc.Register(c); err != nil {
		fmt.Printf("[Coordinator.serve] failed to register coordinator for rpc: %v\n", err)
		return
	}
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sock := coordinatorSock()
	os.Remove(sock)
	l, e := net.Listen("unix", sock)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Printf("[Coordinator.serve] begin to listen %s\n", sock)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
	}

	//TODO

	c.serve()
	return &c
}
