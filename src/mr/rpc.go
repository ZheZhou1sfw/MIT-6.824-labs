package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"strings"
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

// Define RPC structures used for RPC
type RPCrequest struct {
	dummyData string
}

type RPCresponse struct {
	curJob MrJob
}

/*
The mapJob used for Map phase
jobType: 1: "map", 2: "reduce", 3: "pleaseExit", 4: "hold"
file
*/
type MrJob struct {
	jobType  string
	fileName string
	fileLoc  string
	ID       int // for map, it's the mapID, for reduce, it's the nReduce'th
}

// type MapJob struct {
// 	MrJob
// 	mapID int
// }

// type ReduceJob struct {
// 	MrJob
// 	reduceID int
// }

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// RemoveDotTxt Return the string name without txt for a file
func removeDotTxt(file string) string {
	return strings.Split(file, ".")[0]
}
