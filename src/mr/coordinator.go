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
	"time"
)

const CheckTaskSleepSec = 10

const (
	StageMapping = iota
	StageReducing
	StageWaiting
	StageDone
)

type Coordinator struct {
	files    []string
	stageMtx sync.Mutex // mutex for the variable stage
	indexMtx sync.Mutex // mutex for the variables fileIdx and reduceIdx
	nDoneMtx sync.Mutex // mutex for the variables nFileDone and nReduceDone
	interMtx sync.Mutex // mutex for the variable interMap

	stage       int
	fileIdx     int // the index to the currently processed file
	nFile       int // the number of files
	nFileDone   int // the number of successfully processed files
	reduceIdx   int // the index to the currently processed reduce task
	nReduce     int // the number of reduce tasks
	nReduceDone int // the number of successfully completed reduce tasks

	mapTasks     []*Task          // the list of generated map tasks
	reduceTasks  []*Task          // the list of generated reduce tasks
	interMap     map[int][]string // a map to map reduce index to its corresponding intermediate files
	redoTaskChan chan int         // the channel to pass tasks need to be redone
}

// RPC handlers for the worker to call.

// GetNReduce returns nReduce
func (c *Coordinator) GetNReduce(args *DummyArgs, reply *int) error {
	*reply = c.nReduce
	return nil
}

// AssignTask returns a specific assigned task to the worker according to the current stage
func (c *Coordinator) AssignTask(args *DummyArgs, task *Task) error {
	c.stageMtx.Lock()
	stage := c.stage
	c.stageMtx.Unlock()

	switch stage {
	case StageMapping:
		c.assignMapTask(task)
	case StageReducing:
		c.assignReduceTask(task)
	case StageDone:
		c.assignEndTask(task)
	default:
		break
	}
	return nil
}

// assignMapTask assigns a map task included in reply
func (c *Coordinator) assignMapTask(task *Task) {
	var idx int
	select {
	case idx = <-c.redoTaskChan: // a failed task found, redo it
		task = c.mapTasks[idx]
		return
	default:
		c.indexMtx.Lock()
		idx = c.fileIdx
		c.fileIdx++
		c.indexMtx.Unlock()
	}
	if idx >= c.nFile {
		return
	}

	task.Idx = idx
	task.Type = TaskTypeMap
	task.Status = TaskStatusDoing
	task.Filenames = []string{c.files[idx]}
	c.mapTasks[idx] = task
	go c.checkTaskAsync(idx, c.mapTasks)
}

// assignReduceTask assigns a reduce task included in reply
func (c *Coordinator) assignReduceTask(task *Task) {
	var idx int
	select {
	case idx = <-c.redoTaskChan: // a failed task found, redo it
		task = c.reduceTasks[idx]
		return
	default:
		c.indexMtx.Lock()
		idx = c.reduceIdx
		c.reduceIdx++
		c.indexMtx.Unlock()
	}
	if idx >= c.nReduce {
		return
	}

	task.Idx = idx
	task.Type = TaskTypeReduce
	task.Status = TaskStatusDoing
	task.Filenames = c.interMap[idx]
	c.reduceTasks[idx] = task
	go c.checkTaskAsync(idx, c.reduceTasks)
}

// assignEndTask assigns a task as an end flag
func (c *Coordinator) assignEndTask(task *Task) {
	task.Type = TaskTypeEnd
}

// ReportTask reports the task completion sent from worker
func (c *Coordinator) ReportTask(task *Task, reply *ReportReply) error {
	switch task.Type {
	case TaskTypeMap:
		return c.reportMapTask(task)
	case TaskTypeReduce:
		return c.reportReduceTask(task, reply)
	}
	return nil
}

func (c *Coordinator) reportMapTask(task *Task) error {
	switch task.Status {
	case TaskStatusDone:
		c.mapTasks[task.Idx].Status = TaskStatusDone
		c.nDoneMtx.Lock()
		c.nFileDone++
		curNumFileDone := c.nFileDone
		c.nDoneMtx.Unlock()
		if curNumFileDone == c.nFile {
			c.stageMtx.Lock()
			c.stage = StageReducing // begin to do reducing
			c.stageMtx.Unlock()
		}

		interFilenames := task.Filenames
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
		return errors.New("received an invalid reported map task")
	}
	return nil
}

func (c *Coordinator) reportReduceTask(task *Task, reply *ReportReply) error {
	switch task.Status {
	case TaskStatusDone:
		c.reduceTasks[task.Idx].Status = TaskStatusDone
		c.nDoneMtx.Lock()
		c.nReduceDone++
		curNumReduceDone := c.nReduceDone
		c.nDoneMtx.Unlock()
		if curNumReduceDone == c.nReduce {
			c.stageMtx.Lock()
			c.stage = StageDone // all tasks completed!
			c.stageMtx.Unlock()
			*reply = StageDone
		}
	case TaskStatusFailed:
		c.redoTaskChan <- task.Idx
	default:
		return errors.New("received an invalid reported reduce task")
	}
	return nil
}

// checkTaskAsync checks the task status async
func (c *Coordinator) checkTaskAsync(idx int, tasks []*Task) {
	time.Sleep(CheckTaskSleepSec * time.Second)
	if tasks[idx].Status == TaskStatusDoing {
		c.redoTaskChan <- idx
	}
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
		stage:        StageMapping,
		files:        files,
		nFile:        len(files),
		nReduce:      nReduce,
		interMap:     make(map[int][]string),
		redoTaskChan: make(chan int),
		mapTasks:     make([]*Task, len(files)),
		reduceTasks:  make([]*Task, nReduce),
	}

	c.serve()
	return &c
}
