//
// start a worker process, which is implemented in ../mr/worker.go.
// Typically there will be multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//

package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"6.5840/common"
	"6.5840/mr"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	if _, err := os.Stat(common.IntermediateDir); errors.Is(err, os.ErrNotExist) {
		err = os.Mkdir(common.IntermediateDir, os.ModePerm)
		if err != nil {
			log.Printf("failed to create the directory which stores intermediate files: %v", err)
		}
	}

	mapFunc, reduceFunc := common.LoadPlugin(os.Args[1])
	w := mr.MakeWorker(mapFunc, reduceFunc)
	w.MapReduce()
}
