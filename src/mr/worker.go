package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Ask master process for work (RPC)
		funcType, inputs, outputs := CallGetTask()
		switch funcType {
		case "map":
			// read each input file, call mapf on contents
			intermediate := []KeyValue{}
			for _, filename := range inputs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}

			// create output files
			output_files := make([]*json.Encoder, len(outputs))
			for i, filename := range outputs {
				file, err := os.Create(filename)
				output_files[i] = json.NewEncoder(file)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
			}

			// write intermediate key-value pairs to output files
			// use ihash(key) % len(output_files) to choose which output file to use
			for _, kv := range intermediate {
				output_files[ihash(kv.Key)%len(output_files)].Encode(&kv)
			}
			// TODO call Master.DoneTask
		case "reduce":
			// TODO
		case "quit":
			os.Exit(0)
		default:
			panic(fmt.Sprintf("%v not a recognized task type", funcType))
		}
	}
}

func CallGetTask() (string, []string, []string) {
	reply := GetTaskReply{}
	if didCall := call("Master.GetTask", &GetTaskArgs{}, &reply); didCall {
		fmt.Printf("task assigned (%v: %v -> %v)\n", reply.FuncType, reply.InputFiles, reply.OutputFiles)
		return reply.FuncType, reply.InputFiles, reply.OutputFiles
	} else {
		fmt.Println("could not connect to master, exiting...")
		return "quit", []string{}, []string{}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
