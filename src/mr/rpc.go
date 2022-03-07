package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// Add your RPC definitions here.
type CheckinArgs struct {
	Timestamp time.Time
}

type CheckinReply struct {
	Id int
}

type ReportArgs struct {
	Timestamp time.Time
	WorkerId  int
	IsIdle    bool
}

type ReportReply struct {
	TaskIdentifier TaskIdentifier
	FileName       string
	Num            int // number of partitions for map task and number of map tasks for reduce task
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
