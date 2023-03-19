package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

const MappersPerFile = 1

const (
	StageMapping = iota
	StageReducing
	StageDone
)

type Coordinator struct {
	files    []string
	stageMtx sync.Mutex // mutex for the variable stage
	indexMtx sync.Mutex // mutex for the variables mapIdx and reduceIdx
	nDoneMtx sync.Mutex // mutex for the variables nMapDone and nReduceDone
	interMtx sync.Mutex // mutex for the variable interMap

	stage          int
	mapIdx         int
	nMap           int
	nMapDone       int
	reduceIdx      int
	nReduce        int
	nReduceDone    int
	mappersPerFile int

	interMap     map[int][]string
	redoTaskChan chan int
}

// RPC handlers for the worker to call.

// GetNReduce returns nReduce
func (c *Coordinator) GetNReduce(args *DummyArgs, reply *int) error {
	*reply = c.nReduce
	return nil
}

// AssignTask returns a specific assigned task to the worker
func (c *Coordinator) AssignTask(args *DummyArgs, reply *TaskReply) error {
	var stage int
	c.stageMtx.Lock()
	switch c.stage {
	case StageMapping:
		stage = StageMapping
	case StageReducing:
		stage = StageReducing
	case StageDone:
		stage = StageDone
	}
	c.stageMtx.Unlock()

	c.assignTask(stage, reply)
	return nil
}

// assignTask assigns a task to the worker according to the current stage
func (c *Coordinator) assignTask(stage int, reply *TaskReply) {
	reply.Task = &Task{Type: TaskTypeVoid}
	switch stage {
	case StageMapping:
		c.assignMapTask(reply)
	case StageReducing:
		c.assignReduceTask(reply)
	default:
		break
	}
}

// assignMapTask assigns a map task included in reply
func (c *Coordinator) assignMapTask(reply *TaskReply) {
	var idx int
	select {
	case idx = <-c.redoTaskChan: // a failed task found, redo it
	default:
		c.indexMtx.Lock()
		idx = c.mapIdx
		c.mapIdx++
		c.indexMtx.Unlock()
	}
	if idx >= c.nMap {
		return
	}

	reply.Task.Idx = idx
	reply.Task.Type = TaskTypeMap
	reply.Task.Status = TaskStatusReady
	reply.Filenames = append(reply.Filenames, c.files[idx])
}

// assignReduceTask assigns a reduce task included in reply
func (c *Coordinator) assignReduceTask(reply *TaskReply) {
	c.indexMtx.Lock()
	idx := c.reduceIdx
	c.reduceIdx++
	c.indexMtx.Unlock()
	if idx >= c.nReduce {
		return
	}
}

// ReportTask reports the task completion sent from worker
func (c *Coordinator) ReportTask(args *ReportArgs, reply *DummyReply) error {
	task := args.Task
	switch task.Type {
	case TaskTypeMap:
		return c.reportMapTask(task, args.Filenames)
	case TaskTypeReduce:
		return c.reportReduceTask()
	}
	return nil
}

func (c *Coordinator) reportMapTask(task *Task, interFilenames []string) error {
	switch task.Status {
	case TaskStatusDone:
		c.nDoneMtx.Lock()
		c.nMapDone++
		curNumMapDone := c.nMapDone
		c.nDoneMtx.Unlock()
		if curNumMapDone == c.nMap {
			c.stageMtx.Lock()
			c.stage = StageReducing // begin to do reducing
			c.stageMtx.Unlock()
		}

		for _, filename := range interFilenames {
			strs := strings.Split(filename, "-")
			reduceIdx, err := strconv.Atoi(strs[len(strs)-1])
			if err != nil {
				log.Printf("[Coordinator.reportMapTask] failed to convert string to integer for the intermediate file %s", filename)
				return err
			}
			c.interMtx.Lock()
			c.interMap[reduceIdx] = append(c.interMap[reduceIdx], filename)
			c.interMtx.Unlock()
		}
	case TaskStatusFailed:
		c.redoTaskChan <- task.Idx
	default:
		return errors.New("received an invalid reported task")
	}
	return nil
}

func (c *Coordinator) reportReduceTask() error {
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

// Done is called by main/mrcoordinator.go periodically to find out if the entire job has finished
func (c *Coordinator) Done() bool {
	return c.stage == StageDone
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          StageMapping,
		mappersPerFile: MappersPerFile,
		files:          files,
		nReduce:        nReduce,
		interMap:       make(map[int][]string),
		redoTaskChan:   make(chan int),
	}
	c.nMap = c.mappersPerFile * len(c.files)

	c.serve()
	return &c
}
