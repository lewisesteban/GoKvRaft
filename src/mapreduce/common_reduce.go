package mapreduce

import (
	"os"
	"bufio"
	"encoding/json"
	"sort"
)

// sorting a slice of KeyValue by key
type ByKey []KeyValue

func (kvSlice ByKey) Len() int           { return len(kvSlice) }
func (kvSlice ByKey) Swap(i, j int)      { kvSlice[i], kvSlice[j] = kvSlice[j], kvSlice[i] }
func (kvSlice ByKey) Less(i, j int) bool { return kvSlice[i].Key < kvSlice[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	// all key-value pairs
	var kvSlice []KeyValue

	// loop through each file (one per map task)
	for mapTask := 0; mapTask < nMap; mapTask++ {
		inFileName := reduceName(jobName, mapTask, reduceTask)

		// read the file and gather its key/value pairs
		inFile, _ := os.Open(inFileName)
		reader := bufio.NewReader(inFile)
		dec := json.NewDecoder(reader)
		for {
			var kv KeyValue
			if dec.Decode(&kv) == nil {
				kvSlice = append(kvSlice, kv)
			} else {
				break
			}
		}
	}

	// prepare json file
	file, _ := os.Create(outFile)
	writer := bufio.NewWriter(file)
	enc := json.NewEncoder(writer)

	// group keys together, reduce and write to json file
	sort.Sort(ByKey(kvSlice))
	i := 0
	for i < len(kvSlice) {
		values := make([]string, 1)
		values[0] = kvSlice[i].Value
		for i+1 < len(kvSlice) && kvSlice[i].Key == kvSlice[i+1].Key {
			i++
			values = append(values, kvSlice[i].Value)
		}
		enc.Encode(KeyValue{kvSlice[i].Key, reduceF(kvSlice[i].Key, values)})
		i++
	}

	writer.Flush()
	file.Close()


	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
