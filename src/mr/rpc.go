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
type TaskType int

const (
	Map TaskType = iota
	Reduce
	TurnOff
)

type TaskRpcArgs struct {
	Uuid string
}

type TaskRpcReply struct {
	TaskType          TaskType
	PieceFileName     string
	ReduceIdx         int
	NReduce           int
	MapTaskNum        int
	IntermediateFiles []string
}

type IntermediateFileArgs struct {
	IntermediateFileName string
	ReduceIdx            int
}

type IntermediateFileReply struct {
	Msg string
}

type RpcTaskDoneArgs struct {
	TaskType     TaskType
	TaskDoneInfo string
}

type RpcTaskDoneReply struct {
	Msg string
}

type RpcWorkerStatusArgs struct {
	Uuid string
}

type RpcWorkerStatusReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
