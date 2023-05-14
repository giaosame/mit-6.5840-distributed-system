package log

/*
 * Refer to https://blog.josejg.com/debugging-pretty/
 * e.g. to disable logging: VERBOSE=0 go test -run 2A
 */

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

var verbose int

func init() {
	verbose = 1

	verboseEnvStr := os.Getenv("VERBOSE")
	if verboseEnvStr != "" {
		var err error
		verbose, err = strconv.Atoi(verboseEnvStr)
		if err != nil {
			log.Fatalf("invalid verbose value: %s", verboseEnvStr)
		}
	}

	if verbose != 0 {
		log.Printf("[init] verbose = %d", verbose)
	}
}

func Debug(funcName string, format string, args ...interface{}) {
	if verbose == 1 {
		log.Printf(fmt.Sprintf("[%s] ", funcName)+format, args...)
	}
}
