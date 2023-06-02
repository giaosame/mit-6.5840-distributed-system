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
	"time"
)

var verbose int

func init() {
	verbose = 1
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

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

func myPrint(prefix string, format string, args ...interface{}) {
	if verbose == 1 {
		t := time.Now().UnixMilli()
		prefixWithTime := fmt.Sprintf("@%d %s", t, prefix)
		log.Printf(fmt.Sprintf(prefixWithTime+format, args...))
	}
}

func Debug(funcName string, format string, args ...interface{}) {
	prefix := fmt.Sprintf("DEBUG [%s] ", funcName)
	myPrint(prefix, format, args)
}

func Error(funcName string, format string, args ...interface{}) {
	prefix := fmt.Sprintf("ERROR [%s] ", funcName)
	myPrint(prefix, format, args)
}
