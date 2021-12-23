package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	QuitTask   = "quit" // should never appear in TasksToAssign, only ever sent directly to idle worker
)

type Task struct {
	InputFiles  []string
	OutputFiles []string
	Type        string
	id          int
}

type Master struct {
	NReduce int
	NMap    int
	// work to be assigned
	TasksToAssign chan Task
	// work in progress
	TasksInProgress map[int]Task
	// Synchronization
	MasterLock *sync.Mutex
	NeedWork   *sync.Cond
	// work done (debugging only?)
}

func (m *Master) monitorTask(taskId int, startTime int64) {
	m.MasterLock.Lock()
	for time.Now().Unix()-startTime < 10 {
		if _, ok := m.TasksInProgress[taskId]; !ok {
			// if taskId is no longer in progress, assume task completed and doesn't need monitoring
			m.MasterLock.Unlock()
			return
		}
		m.MasterLock.Unlock()
		time.Sleep(time.Second)
		m.MasterLock.Lock()
	}
	// we've waited 10 seconds and the task is still in progress
	// assume worker has died, move from in progress back to TasksToAssign
	// and signal any waiting workers
	task, ok := m.TasksInProgress[taskId]
	// it's possible the task was completed between last check of map and now
	// in which case, there's nothing to do
	if ok {
		m.TasksToAssign <- task
		delete(m.TasksInProgress, taskId)
		log.Printf("Assuming worker with %v task %v is dead, reassigning...", task.Type, task.id)
		m.NeedWork.Signal()
	}
	m.MasterLock.Unlock()
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.MasterLock.Lock()
	defer m.MasterLock.Unlock()

	for len(m.TasksToAssign) == 0 {
		if len(m.TasksInProgress) == 0 {
			// job is done, tell worker to quit
			log.Println("job is done, telling worker to quit")
			reply.TaskType = QuitTask
			reply.TaskId = -1
			return nil
		} else {
			// job is not done, wait for task to become available
			m.NeedWork.Wait()
		}
	}
	task := <-m.TasksToAssign
	m.TasksInProgress[task.id] = task
	reply.InputFiles = task.InputFiles
	reply.OutputFiles = task.OutputFiles
	reply.TaskType = task.Type
	reply.TaskId = task.id

	// launch a thread to monitor task
	go m.monitorTask(task.id, time.Now().Unix())
	return nil
}

func (m *Master) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	m.MasterLock.Lock()
	defer m.MasterLock.Unlock()

	log.Printf("Task %v reports done", args.TaskId)
	task, ok := m.TasksInProgress[args.TaskId]
	if !ok {
		log.Println("\tTask not shown as in progress")
	} else {
		delete(m.TasksInProgress, args.TaskId)
	}

	if task.Type == MapTask && len(m.TasksInProgress) == 0 && len(m.TasksToAssign) == 0 {
		// create reduce tasks
		for reduceId := 0; reduceId < m.NReduce; reduceId++ {
			inputs := make([]string, 0, m.NMap)
			for mapId := 0; mapId < m.NMap; mapId++ {
				inputs = append(inputs, fmt.Sprintf("mr-%d-%d", mapId, reduceId))
			}
			log.Printf("Adding reduce task %d on %v\n", reduceId, inputs)
			m.TasksToAssign <- Task{inputs, []string{fmt.Sprintf("mr-out-%d", reduceId)}, ReduceTask, reduceId}
		}
		// wake up all waiting workers
		m.NeedWork.Broadcast()
	} else if task.Type == ReduceTask && len(m.TasksInProgress) == 0 && len(m.TasksToAssign) == 0 {
		// wake up all waiting workers, so they can be told to quit
		m.NeedWork.Broadcast()
	}

	return nil
}

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
	tasks := make(chan Task, max(len(files), nReduce))
	taskId := 0
	for _, f := range files {
		outputs := make([]string, 0, nReduce)
		for i := 0; i < nReduce; i++ {
			outputs = append(outputs, fmt.Sprintf("mr-%d-%d", taskId, i))
		}
		log.Printf("Adding task %d on %s\n", taskId, f)
		tasks <- Task{[]string{f}, outputs, MapTask, taskId}
		taskId++
	}
	lock := sync.Mutex{}
	m := Master{TasksToAssign: tasks, TasksInProgress: make(map[int]Task, len(files)),
		NReduce: nReduce, NMap: len(files), MasterLock: &lock, NeedWork: sync.NewCond(&lock)}
	m.server()
	return &m
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
