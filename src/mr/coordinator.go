package mr

import (
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

	mapIdx    int
	nReduce   int
	reduceIdx int
}

// RPC handlers for the worker to call.

// GetData returns nReduce and the number of files to be processed
func (c *Coordinator) GetData(args *DummyArgs, reply *CoordinatorDataReply) error {
	reply.NReduce = c.nReduce
	reply.NumFiles = len(c.files)
	return nil
}

// GetMapTask returns a map task including the filename
func (c *Coordinator) GetMapTask(args *DummyArgs, reply *TaskReply) error {
	c.mtx.Lock()
	idx := c.mapIdx
	c.mapIdx++
	c.mtx.Unlock()

	if idx < len(c.files) {
		reply.Filename = c.files[idx]
	}
	return nil
}

// serve starts a thread that listens for RPCs from worker.go
func (c *Coordinator) serve() {
	if err := rpc.Register(c); err != nil {
		log.Fatal("[Coordinator.serve] failed to register coordinator for rpc:", err)
		return
	}
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sock := coordinatorSock()
	if err := os.Remove(sock); err != nil {
		log.Println("[Coordinator.serve] failed to remove:", err)
	}
	listener, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatalf("[Coordinator.serve] failed to listen the socket %s: %v", sock, err)
	}

	log.Printf("[Coordinator.serve] begin to listen %s\n", sock)
	go http.Serve(listener, nil)
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
		nReduce: nReduce,
	}

	c.serve()
	return &c
}
