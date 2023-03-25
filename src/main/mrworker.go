//
// start a worker process, which is implemented in ../mr/worker.go.
// Typically there will be multiple worker processes, talking to one coordinator.
//
// * single-worker mode:
//   go run mrworker.go ../mrapps/wc.so
// * multi-worker mode:
//   go run mrworker.go ../mrapps/wc.so multi-mode

package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"6.5840/common"
	"6.5840/mr"
)

const MultiModeArg = "multi-mode"

func main() {
	nArgs := len(os.Args)
	if nArgs < 2 {
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
	if nArgs == 2 { // multi-worker mode is one main worker process which generates multiple worker goroutines
		w := mr.MakeWorker(mapFunc, reduceFunc)
		w.MapReduce()
	} else if os.Args[2] == MultiModeArg {
		mw := mr.MakeMultiWorker(mapFunc, reduceFunc)
		mw.MapReduce()
	}
}
