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

const (
	None int = iota
	MapTask
	ReduceTask
)

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskType       int
	TaskId         int32
	InputFileNames []string
	NReduce        int
	OutputFile     string // for reduce task
}

type ReportTaskArgs struct {
	TaskType int
	TaskId   int32
	Files    []string
	Status   int
}

type ReportTaskReply struct {
	TaskId int32
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
