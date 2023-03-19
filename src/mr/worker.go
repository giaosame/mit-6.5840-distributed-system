package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"6.5840/common"
)

const MapReduceSleepFactor = 30

type Worker struct {
	nReduce    int
	mapFunc    func(string, string) []common.KeyValue
	reduceFunc func(string, []string) string
}

// use ihash(key) % NReduce to choose the number of reduce task for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// MakeWorker is called by main/mrworker.go
func MakeWorker(mapFunc func(string, string) []common.KeyValue, reduceFunc func(string, []string) string) *Worker {
	return &Worker{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}
}

func (w *Worker) MapReduce() {
	stage := StageMapping
	allTasksAssigned := make(chan bool, 1)
	wg := sync.WaitGroup{}

	w.nReduce = getNReduce()
	if w.nReduce < 0 {
		return
	}

	for {
		select {
		case <-allTasksAssigned:
			wg.Wait()
			stage++
		default:
			go w.work(allTasksAssigned, &wg)
		}

		if stage >= StageReducing {
			break
		}
		time.Sleep(MapReduceSleepFactor * time.Millisecond)
	}
	log.Println("[Worker.MapReduce] completed successfully!")
	time.Sleep(time.Second * 5)
}

func (w *Worker) work(waitChan chan bool, wg *sync.WaitGroup) {
	taskReply := getTask()
	if taskReply == nil { // ignores tasks which failed to call rpc
		return
	}

	switch taskReply.Task.Type {
	case TaskTypeVoid:
		waitChan <- true
	case TaskTypeMap:
		w.doMapWork(taskReply.Task, taskReply.Filenames[0], wg)
	case TaskTypeReduce:
		w.doReduceWork()
	}
}

func (w *Worker) doMapWork(task *Task, filename string, wg *sync.WaitGroup) {
	wg.Add(1)
	task.Status = TaskStatusDoing
	log.Println("[Worker.doMapWork] begin to map the file", filename)

	var err error
	interFilenames := make([]string, w.nReduce)
	files := make([]*os.File, w.nReduce)
	encoders := make([]*json.Encoder, w.nReduce)
	defer func() {
		for _, file := range files {
			file.Close()
		}
		reportTask(task, interFilenames, err)
		wg.Done()
	}()

	for y := 0; y < w.nReduce; y++ {
		var file *os.File
		interFilename := filepath.Join(common.IntermediateDir, fmt.Sprintf("mr-%d-%d", task.Idx, y))
		file, err = os.Create(interFilename)
		if err != nil {
			log.Printf("[Worker.doMapWork] failed to create the intermediate file %s: %v", interFilename, err)
			return
		}

		interFilenames[y] = interFilename
		files[y] = file
		encoders[y] = json.NewEncoder(file)
	}

	kva := w.convertFileToKVArray(filename)
	for _, kv := range kva {
		hashIdx := ihash(kv.Key) % w.nReduce
		encoder := encoders[hashIdx]
		if err = encoder.Encode(kv); err != nil {
			log.Printf("[Worker.doMapWork] failed to encode the KV pair (key = %s, val = %s): %v", kv.Key, kv.Value, err)
			return
		}
	}
}

func (w *Worker) doReduceWork() {

}

func (w *Worker) convertFileToKVArray(filename string) []common.KeyValue {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return w.mapFunc(filename, string(content))
}

func getTask() *TaskReply {
	args := &DummyArgs{}
	reply := &TaskReply{}

	ok := call("Coordinator.AssignTask", args, reply)
	if !ok {
		log.Printf("[getTask] call failed!")
		return nil
	}
	return reply
}

func reportTask(task *Task, filenames []string, err error) {
	if err != nil {
		task.Status = TaskStatusFailed
		log.Printf("[reportTask] map task #%d failed", task.Idx)
	} else {
		task.Status = TaskStatusDone
		log.Printf("[reportTask] map task #%d completed", task.Idx)
	}

	args := &ReportArgs{
		Task:      task,
		Filenames: filenames,
	}
	reply := &DummyReply{}
	ok := call("Coordinator.ReportTask", args, reply)
	if !ok {
		log.Printf("[reportTask] call failed!")
	}
}

func getNReduce() int {
	args := DummyArgs{}
	reply := 0

	ok := call("Coordinator.GetNReduce", &args, &reply)
	if !ok {
		log.Printf("[getCoordinatorData] call failed!")
		return -1
	}
	return reply
}

// call sends an RPC request to the coordinator, wait for the response.
// It usually returns true, and returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sock := coordinatorSock()
	cli, err := rpc.DialHTTP("unix", sock)
	if err != nil {
		log.Println("[call] failed to call DialHTTP:", err)
		return false
	}
	defer cli.Close()

	err = cli.Call(rpcName, args, reply)
	if err != nil {
		log.Printf("[call] failed to call rpc %s: %v", rpcName, err)
		return false
	}

	return true
}
