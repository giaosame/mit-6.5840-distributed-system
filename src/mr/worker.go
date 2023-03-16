package mr

import (
	"6.5840/common"
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
)

type Worker struct {
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
	wg := sync.WaitGroup{}
	//reduceDone := false
	nReduce, numFiles := getCoordinatorData()
	if nReduce < 0 {
		return
	}
	log.Printf("[Worker.MapReduce] begin to do MapReduce... [%d, %d]", nReduce, numFiles)

	// mapping stage
	for i := 0; i < numFiles; i++ {
		go w.Map(i, nReduce, &wg)
	}
	wg.Wait()

	time.Sleep(time.Second * 3)
}

func (w *Worker) Map(idx, nReduce int, wg *sync.WaitGroup) {
	filename := getMapTask()
	wg.Add(1)
	log.Println("[Worker.Map] begin to map the file", filename)

	var files []*os.File
	defer func() {
		if files == nil {
			return
		}
		for _, file := range files {
			file.Close()
		}
		log.Println("[Worker.Map] completed to map the file", filename)
		wg.Done()
	}()

	var encoders []*json.Encoder
	for y := 0; y < nReduce; y++ {
		interFilename := filepath.Join(common.IntermediateDir, fmt.Sprintf("mr-%d-%d", idx, y))
		file, err := os.Create(interFilename)
		if err != nil {
			log.Printf("[Worker.Map] failed to create the intermediate file %s: %v", interFilename, err)
			return
		}
		files = append(files, file)
		encoders = append(encoders, json.NewEncoder(file))
	}

	kva := w.convertFileToKVArray(filename)
	for _, kv := range kva {
		hashIdx := ihash(kv.Key) % nReduce
		encoder := encoders[hashIdx]
		if err := encoder.Encode(kv); err != nil {
			log.Printf("[Worker.Map] failed to encode the KV pair (key = %s, val = %s): %v", kv.Key, kv.Value, err)
		}
	}
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

func getMapTask() string {
	args := DummyArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if !ok {
		log.Printf("[getMapTask] call failed!")
		return ""
	}
	return reply.Filename
}

func getCoordinatorData() (int, int) {
	args := DummyArgs{}
	reply := CoordinatorDataReply{}

	ok := call("Coordinator.GetData", &args, &reply)
	if !ok {
		log.Printf("[getCoordinatorData] call failed!")
		return -1, 0
	}
	return reply.NReduce, reply.NumFiles
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
