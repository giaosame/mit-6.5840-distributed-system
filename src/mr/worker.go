package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"

	"6.5840/common"
)

const MapReduceSleepMilliSec = 20

type Worker struct {
	nReduce    int
	mapFunc    func(string, string) []common.KeyValue
	reduceFunc func(string, []string) string
}

// MakeWorker is called by main/mrworker.go
func MakeWorker(mapFunc func(string, string) []common.KeyValue, reduceFunc func(string, []string) string) *Worker {
	return &Worker{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}
}

func (w *Worker) MapReduce() {
	w.nReduce = getNReduce()
	if w.nReduce < 0 {
		return
	}

	var stage int
	for {
		w.work(&stage)
		if stage == StageDone {
			break
		}
		time.Sleep(MapReduceSleepMilliSec * time.Millisecond)
	}
	log.Println("[Worker.MapReduce] completed successfully!")
	time.Sleep(time.Second * 3)
}

func (w *Worker) work(stage *int) {
	task := getTask()
	if task == nil {
		return
	}

	switch task.Type {
	case TaskTypeMap:
		w.doMapWork(task)
	case TaskTypeReduce:
		w.doReduceWork(task, stage)
	case TaskTypeEnd:
		*stage = StageDone
	}
}

func (w *Worker) doMapWork(task *Task) {
	filename := task.Filenames[0]
	log.Println("[Worker.doMapWork] begin to map the file", filename)

	var err error
	interFilenames := make([]string, w.nReduce)
	files := make([]*os.File, w.nReduce)
	encoders := make([]*json.Encoder, w.nReduce)
	defer func() {
		for _, file := range files {
			file.Close()
		}
		task.Filenames = interFilenames
		reportTask(task, err)
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

	kva := common.ConvertFileToKVArray(filename, w.mapFunc)
	for _, kv := range kva {
		hashIdx := common.Hash(kv.Key) % w.nReduce
		encoder := encoders[hashIdx]
		if err = encoder.Encode(kv); err != nil {
			log.Printf("[Worker.doMapWork] failed to encode the KV pair (key = %s, val = %s): %v", kv.Key, kv.Value, err)
			return
		}
	}
}

func (w *Worker) doReduceWork(task *Task, stage *int) {
	log.Printf("[Worker.doReduceWork] begin to reduce the #%d list of files", task.Idx)

	var err error
	var kva []common.KeyValue
	defer func() {
		*stage = reportTask(task, err)
	}()

	interFilenames := task.Filenames
	for _, interFilename := range interFilenames {
		var interFile *os.File
		interFile, err = os.Open(interFilename)
		if err != nil {
			log.Printf("[Worker.doReduceWork] failed to open the intermediate file %s: %v", interFilename, err)
			return
		}
		decoder := json.NewDecoder(interFile)
		for { // read json file back
			var kv common.KeyValue
			if tmpErr := decoder.Decode(&kv); tmpErr != nil {
				if tmpErr != io.EOF {
					err = tmpErr
					log.Printf("[Worker.doReduceWork] failed to decode the intermediate file %s: %v", interFilename, err)
				}
				break
			}
			kva = append(kva, kv)
		}
		interFile.Close()
	}

	// sort and write the reduce output to the file, according to the reduce part of main/mrsequential.go
	sort.Sort(common.ByKey(kva))
	outFile, _ := os.Create(fmt.Sprintf("mr-out-%d", task.Idx))
	defer outFile.Close()

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reduceFunc(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
}

func getTask() *Task {
	task := &Task{}
	if ok := call("Coordinator.AssignTask", &DummyArgs{}, task); !ok {
		log.Printf("[getTask] call failed!")
		return nil
	}
	return task
}

func reportTask(task *Task, err error) int {
	var taskType string
	switch task.Type {
	case TaskTypeMap:
		taskType = "map"
	case TaskTypeReduce:
		taskType = "reduce"
	}
	if err != nil {
		task.Status = TaskStatusFailed
		log.Printf("[reportTask] %s task #%d failed", taskType, task.Idx)
	} else {
		task.Status = TaskStatusDone
		log.Printf("[reportTask] %s task #%d completed", taskType, task.Idx)
	}

	reply := ReportReply(-1)
	if ok := call("Coordinator.ReportTask", task, &reply); !ok {
		log.Printf("[reportTask] call failed!")
	}
	return int(reply)
}

func getNReduce() int {
	args := DummyArgs{}
	reply := 0

	if ok := call("Coordinator.GetNReduce", &args, &reply); !ok {
		log.Printf("[getNReduce] call failed!")
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
