package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Keep track of output/input file and associated JSON encoder
//
type JSONFile struct {
	File      *os.File
	Enc       *json.Encoder
	FinalName string
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
		funcType, inputs, outputs, taskId := CallGetTask()
		intermediate := []KeyValue{}
		switch funcType {
		case MapTask:
			// read each input file, call mapf on contents
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
				log.Printf("Task %v got %v intermediate values from mapf(%v)", taskId, len(kva), filename)
				intermediate = append(intermediate, kva...)
			}

			// create temporary output files
			output_files := make([]JSONFile, len(outputs))
			for i, filename := range outputs {
				file, err := os.CreateTemp("/tmp", filename)
				output_files[i] = JSONFile{file, json.NewEncoder(file), filename}
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
			}

			// write intermediate key-value pairs to output files
			// use ihash(key) % len(output_files) to choose which output file to use
			for _, kv := range intermediate {
				output_files[ihash(kv.Key)%len(output_files)].Enc.Encode(&kv)
			}

			// rename and close temporary files
			for _, f := range output_files {
				os.Rename(f.File.Name(), f.FinalName)
				f.File.Close()
			}

			// notify master that task is done
			call("Master.DoneTask", &DoneTaskArgs{taskId}, &DoneTaskReply{})
		case ReduceTask:
			// read and collect input data
			for _, filename := range inputs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				// TODO determine if there was a problem reading the input file
				//
				// approach below doesn't work for tasks with few intermediate values where some
				// input files will be empty
				//
				// if len(intermediate) == 0 {
				// 	log.Fatalf("cannot read %v", filename)
				// }
				file.Close()
			}

			// sort input data
			sort.Sort(ByKey(intermediate))

			// create temporary output file
			if len(outputs) > 1 {
				log.Fatalf("reduce task expects single output file, got %v", outputs)
			}
			filename := outputs[0]
			ofile, err := os.CreateTemp("/tmp", filename)
			if err != nil {
				log.Fatalf("could not create /tmp/%v-*", filename)
			}

			// call Reduce on each distinct key in intermediate[],
			// and print the result to ofile.
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			// rename temporary outfile file and close it
			os.Rename(ofile.Name(), filename)
			ofile.Close()

			// notify master that task is done
			call("Master.DoneTask", &DoneTaskArgs{taskId}, &DoneTaskReply{})
		case QuitTask:
			os.Exit(0)
		default:
			log.Panicf("%v not a recognized task type", funcType)
		}
	}
}

//
// make an RPC call to the master to ask for a task
// if master cannot be reached, return a QuitTask
//
func CallGetTask() (string, []string, []string, int) {
	reply := GetTaskReply{}
	if didCall := call("Master.GetTask", &GetTaskArgs{}, &reply); didCall {
		log.Printf("Task %v assigned (%v: %v -> %v)\n", reply.TaskId, reply.TaskType, reply.InputFiles, reply.OutputFiles)
		return reply.TaskType, reply.InputFiles, reply.OutputFiles, reply.TaskId
	} else {
		log.Println("could not connect to master, exiting...")
		return QuitTask, []string{}, []string{}, 0
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
