package main

//
// same as crash.go but doesn't actually crash.
//
// go build -buildmode=plugin nocrash.go
//

import (
	crand "crypto/rand"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"

	"6.5840/common"
)

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if false && rr.Int64() < 500 {
		// crash!
		os.Exit(1)
	}
}

func Map(filename string, contents string) []common.KeyValue {
	maybeCrash()

	var kva []common.KeyValue
	kva = append(kva, common.KeyValue{Key: "a", Value: filename})
	kva = append(kva, common.KeyValue{Key: "b", Value: strconv.Itoa(len(filename))})
	kva = append(kva, common.KeyValue{Key: "c", Value: strconv.Itoa(len(contents))})
	kva = append(kva, common.KeyValue{Key: "d", Value: "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
