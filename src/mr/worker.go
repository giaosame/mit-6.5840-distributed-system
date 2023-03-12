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

	"6.5840/common"
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
	var wg sync.WaitGroup
	mapDone := false
	//reduceDone := false
	doneChan := make(chan bool, 1)
	nReduce := getReduceNumber()
	if nReduce < 0 {
		return
	}
	log.Println("nReduce =", nReduce)

	// mapping stage
	for i := 0; ; i++ {
		select {
		case <-doneChan:
			mapDone = true
		default:
			go w.Map(i, nReduce, doneChan, &wg)
		}

		if mapDone {
			log.Println("[Worker.work] all map tasks have been assigned and running")
			break
		}
	}
	wg.Wait()
}

func (w *Worker) Map(idx, nReduce int, done chan bool, wg *sync.WaitGroup) {
	filename := getMapTask()
	if len(filename) == 0 {
		done <- true
	}
	wg.Add(1)

	var files []*os.File
	defer func() {
		if files != nil {
			return
		}
		for _, file := range files {
			file.Close()
		}
	}()

	var encoders []*json.Encoder
	for y := 0; y < nReduce; y++ {
		interFilename := filepath.Join(common.IntermediateDir, fmt.Sprintf("mr-%d-%d", idx, y))
		file, err := os.Create(interFilename)
		if err != nil {
			log.Printf("failed to create the intermediate file %s: %v", interFilename, err)
			return
		}
		files = append(files, file)
		encoders = append(encoders, json.NewEncoder(file))
	}

	kva := w.convertFileToKVArray(filename)
	for _, kv := range kva {
		hashIdx := ihash(kv.Key) & nReduce
		encoder := encoders[hashIdx]
		if err := encoder.Encode(kv); err != nil {
			log.Printf("failed to encode the KV pair (key = %s, val = %s): %v", kv.Key, kv.Value, err)
		}
	}
	wg.Done()
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

	ok := call("Coordinator.GetMapTask", args, reply)
	if !ok {
		log.Printf("[getMapTask] call failed!")
		return ""
	}
	return reply.Filename
}

func getReduceNumber() int {
	args := DummyArgs{}
	reply := ReduceNumReply{}

	ok := call("Coordinator.GetNReduce", &args, &reply)
	if !ok {
		log.Printf("[getReduceNumber] call failed!")
		return -1
	}
	return reply.Num
}

func CallTest() {

	// declare a reply structure.
	args := DummyArgs{}
	reply := TestReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Test", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Val = %v\n", reply.Val)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// call sends an RPC request to the coordinator, wait for the response.
// It usually returns true, and returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sock := coordinatorSock()
	cli, err := rpc.DialHTTP("unix", sock)
	if err != nil {
		log.Fatal("[call] failed to call DialHTTP: ", err)
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
