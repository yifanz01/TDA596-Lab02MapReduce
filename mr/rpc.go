package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ReplyTaskArgs struct {
	TaskId   int    // task id
	Type     string // map or reduce
	FileName string
	NReduce  int
	NMap     int
}

type ApplyforTaskArgs struct {
	WorkerId     int
	LastTaskId   int
	LastTaskType string
}

type Task struct {
	Id       int    // task id
	Type     string // map or reduce
	FileName string
	WorkerId int
	DeadLine time.Time
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
