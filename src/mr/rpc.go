package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// GetTask definitions
type GetTaskArgs struct{}

type GetTaskReply struct {
	InputFiles  []string
	OutputFiles []string
	TaskType    string
	TaskId      int
}

// DoneTask definitions
type DoneTaskArgs struct {
	TaskId int
}

type DoneTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
