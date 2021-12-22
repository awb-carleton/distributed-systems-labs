package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
)

type Task struct {
	InputFiles  []string
	OutputFiles []string
	Type        string
	id          int
}

type Master struct {
	// work to be assigned
	TasksToAssign chan Task
	// work in progress
	TasksInProgress map[int]Task
	NReduce         int
	MasterLock      sync.Mutex
	// work done (debugging only?)
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if len(m.TasksToAssign) == 0 {
		// TODO wait on condition variable if there is no task to assign
		panic("no map tasks to assign")
	}
	task := <-m.TasksToAssign
	reply.InputFiles = task.InputFiles
	reply.OutputFiles = task.OutputFiles
	reply.FuncType = task.Type
	// TODO start thread to monitor task
	return nil
}

// TODO DoneTask (incl. check for starting reduce tasks when NReduce > 0)

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.MasterLock.Lock()
	defer m.MasterLock.Unlock()
	return len(m.TasksInProgress) == 0 && len(m.TasksToAssign) == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	tasks := make(chan Task, len(files))
	taskId := 0
	for _, f := range files {
		outputs := make([]string, 0, nReduce)
		for i := 0; i < nReduce; i++ {
			outputs = append(outputs, fmt.Sprintf("mr-%d-%d", taskId, i))
		}
		fmt.Printf("Adding task %d on %s\n", taskId, f)
		tasks <- Task{[]string{f}, outputs, MapTask, taskId}
		taskId++
	}
	m := Master{TasksToAssign: tasks, TasksInProgress: make(map[int]Task, len(files)),
		NReduce: nReduce}

	m.server()
	return &m
}
