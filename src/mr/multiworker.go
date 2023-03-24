package mr

import (
	"encoding/json"
	"fmt"

	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"6.5840/common"
)

const MultiMapReduceSleepFactor = 10

// MultiWorker generates multiple worker goroutines
type MultiWorker struct {
	nReduce    int
	mapFunc    func(string, string) []common.KeyValue
	reduceFunc func(string, []string) string
}

// MakeMultiWorker is called by main/mrworker.go
func MakeMultiWorker(mapFunc func(string, string) []common.KeyValue, reduceFunc func(string, []string) string) *MultiWorker {
	return &MultiWorker{
		mapFunc:    mapFunc,
		reduceFunc: reduceFunc,
	}
}

func (w *MultiWorker) MapReduce() {
	var stage int
	stageChan := make(chan int)
	wg := sync.WaitGroup{}

	w.nReduce = getNReduce()
	if w.nReduce < 0 {
		return
	}
	for {
		select {
		case stage = <-stageChan:
		default:
			go w.work(stageChan, &wg)
		}

		if stage == StageDone {
			break
		} else {
			wg.Wait()
		}
		time.Sleep(MultiMapReduceSleepFactor * time.Millisecond)
	}
	log.Println("[MultiWorker.MapReduce] completed successfully!")
	time.Sleep(time.Second)
}

func (w *MultiWorker) work(stageChan chan int, wg *sync.WaitGroup) {
	task := getTask()
	if task == nil { // ignores tasks which failed to call rpc
		return
	}

	switch task.Type {
	case TaskTypeVoid:
		stageChan <- StageWaiting
	case TaskTypeMap:
		w.doMapWork(task, wg)
	case TaskTypeReduce:
		w.doReduceWork(task, stageChan, wg)
	case TaskTypeEnd:
		stageChan <- StageDone
	}
}

func (w *MultiWorker) doMapWork(task *Task, wg *sync.WaitGroup) {
	wg.Add(1)
	filename := task.Filenames[0]
	log.Println("[MultiWorker.doMapWork] begin to map the file", filename)

	var err error
	interFilenames := make([]string, w.nReduce)
	files := make([]*os.File, w.nReduce)
	encoders := make([]*json.Encoder, w.nReduce)
	defer func() {
		for _, file := range files {
			file.Close()
		}
		wg.Done()
		task.Filenames = interFilenames
		reportTask(task, err)
	}()

	for y := 0; y < w.nReduce; y++ {
		var file *os.File
		interFilename := filepath.Join(common.IntermediateDir, fmt.Sprintf("mr-%d-%d", task.Idx, y))
		file, err = os.Create(interFilename)
		if err != nil {
			log.Printf("[MultiWorker.doMapWork] failed to create the intermediate file %s: %v", interFilename, err)
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
			log.Printf("[MultiWorker.doMapWork] failed to encode the KV pair (key = %s, val = %s): %v", kv.Key, kv.Value, err)
			return
		}
	}
}

func (w *MultiWorker) doReduceWork(task *Task, stageChan chan int, wg *sync.WaitGroup) {
	wg.Add(1)
	task.Status = TaskStatusDoing
	log.Printf("[MultiWorker.doReduceWork] begin to reduce the #%d list of files", task.Idx)

	var err error
	var kva []common.KeyValue
	defer func() {
		wg.Done()
		stage := reportTask(task, err)
		if stage == StageDone {
			stageChan <- StageDone
		}
	}()

	interFilenames := task.Filenames
	for _, interFilename := range interFilenames {
		var interFile *os.File
		interFile, err = os.Open(interFilename)
		if err != nil {
			log.Printf("[MultiWorker.doReduceWork] failed to open the intermediate file %s: %v", interFilename, err)
			return
		}
		decoder := json.NewDecoder(interFile)
		for { // read json file back
			var kv common.KeyValue
			if tmpErr := decoder.Decode(&kv); tmpErr != nil {
				if tmpErr != io.EOF {
					err = tmpErr
					log.Printf("[MultiWorker.doReduceWork] failed to decode the intermediate file %s: %v", interFilename, err)
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
