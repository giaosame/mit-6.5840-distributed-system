package common

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"plugin"
)

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Hash % NReduce to choose the number of reduce task for each KeyValue emitted by Map.
func Hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// LoadPlugin loads the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func LoadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("[LoadPlugin] cannot load the plugin %v: %v", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("[LoadPlugin] cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("[LoadPlugin] cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func ConvertFileToKVArray(filename string, mapFunc func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("[ConvertFileToKVArray] cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[ConvertFileToKVArray] cannot read %v", filename)
	}
	return mapFunc(filename, string(content))
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
