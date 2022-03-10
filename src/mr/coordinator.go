package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	WorkerList WorkerList
	Schedule   Schedule
}

type WorkerList struct {
	WorkerRecords []WorkerRecord
	Mu            sync.Mutex
}

type WorkerRecord struct {
	LastReportedTime time.Time
	TaskAssigned     TaskIdentifier
}

type TaskIdentifier struct {
	JobId  int
	TaskId int
}

type Schedule struct {
	Jobs         []*Job
	CurrentJobId int
	TaskQueue    chan TaskIdentifier
	Mu           sync.Mutex
}

type Job struct {
	Tasks map[int]string // input file names for map task and output file names for reduce task
	Num   int            // number of input files for reduce task and number of output files for map task
	Mu    sync.Mutex
}

func NO_TASK() TaskIdentifier {
	return TaskIdentifier{-1, -1}
}

func (workerList *WorkerList) AddWorker(timestamp time.Time) int {
	workerList.Mu.Lock()
	defer workerList.Mu.Unlock()

	ret := len(workerList.WorkerRecords)
	workerList.WorkerRecords = append(workerList.WorkerRecords, WorkerRecord{timestamp, NO_TASK()})

	return ret
}

func (workerList *WorkerList) Supervise(taskQueue *chan TaskIdentifier) {
	for {
		workerList.Mu.Lock()
		for _, record := range workerList.WorkerRecords {
			if time.Since(record.LastReportedTime) > 10*time.Second && record.TaskAssigned != NO_TASK() {
				*taskQueue <- record.TaskAssigned
				record.TaskAssigned = NO_TASK()
			}
		}
		workerList.Mu.Unlock()
		time.Sleep(time.Second) // worker are not added as frequently
	}
}

func MakeSchedule(inputFiles []string, outputFiles []string) *Schedule {
	ret := Schedule{Jobs: make([]*Job, 0, 2), CurrentJobId: 0, TaskQueue: make(chan TaskIdentifier, 1000)}
	ret.Jobs = append(ret.Jobs, MakeJob(inputFiles, len(outputFiles)))
	ret.Jobs = append(ret.Jobs, MakeJob(outputFiles, len(inputFiles)))
	for i := range ret.Jobs[0].Tasks {
		ret.TaskQueue <- TaskIdentifier{0, i}
	}
	return &ret
}

func (schedule *Schedule) GetTaskInfo(task TaskIdentifier) (string, int, bool) {
	schedule.Mu.Lock()
	defer schedule.Mu.Unlock()
	if schedule.CurrentJobId != task.JobId {
		return "", -1, false
	}

	job := schedule.Jobs[schedule.CurrentJobId]
	job.Mu.Lock()
	defer job.Mu.Unlock()

	fileName, ok := job.Tasks[task.TaskId]
	return fileName, job.Num, ok
}

func (schedule *Schedule) Update(taskIdentifier TaskIdentifier) bool {
	ret := false

	schedule.Mu.Lock()
	defer schedule.Mu.Unlock()

	if taskIdentifier != NO_TASK() && taskIdentifier.JobId == schedule.CurrentJobId {
		// situation where the work has done a duplicate job is handled
		job := schedule.Jobs[schedule.CurrentJobId]
		job.Mu.Lock()
		defer job.Mu.Unlock()

		_, ok := job.Tasks[taskIdentifier.TaskId]
		if ok {
			// task not done by others
			delete(job.Tasks, taskIdentifier.TaskId)
			if len(job.Tasks) == 0 {
				schedule.CurrentJobId++
				if schedule.CurrentJobId < len(schedule.Jobs) {
					for i := range schedule.Jobs[schedule.CurrentJobId].Tasks {
						schedule.TaskQueue <- TaskIdentifier{schedule.CurrentJobId, i}
					}
				}
				ret = true
			}
		}
	}
	return ret
}

func MakeJob(fileNames []string, num int) *Job {
	ret := Job{Tasks: make(map[int]string), Num: num}
	for i, fileName := range fileNames {
		ret.Tasks[i] = fileName
	}
	return &ret
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Checkin(args *CheckinArgs, reply *CheckinReply) error {
	reply.Id = c.WorkerList.AddWorker(args.Timestamp)
	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	reply.TaskIdentifier = NO_TASK()

	c.WorkerList.Mu.Lock()
	defer c.WorkerList.Mu.Unlock()

	record := &c.WorkerList.WorkerRecords[args.WorkerId]

	if args.Timestamp.After(record.LastReportedTime) {
		record.LastReportedTime = args.Timestamp
		if args.IsIdle {
			updated := c.Schedule.Update(record.TaskAssigned)
			if updated {
				for i := range c.WorkerList.WorkerRecords {
					c.WorkerList.WorkerRecords[i].TaskAssigned = NO_TASK()
				}
			}
			record.TaskAssigned = NO_TASK()
			select {
			case task := <-c.Schedule.TaskQueue:
				// task might be stale
				fileName, num, ok := c.Schedule.GetTaskInfo(task)
				if ok {
					record.TaskAssigned = task
					reply.TaskIdentifier, reply.FileName, reply.Num = task, fileName, num
				}
			default:
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Schedule.Mu.Lock()
	defer c.Schedule.Mu.Unlock()
	return c.Schedule.CurrentJobId == len(c.Schedule.Jobs)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	outputFiles := make([]string, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		outputFiles = append(outputFiles, "mr-out-"+strconv.Itoa(i))
	}
	c.Schedule = *MakeSchedule(files, outputFiles)

	c.WorkerList.WorkerRecords = make([]WorkerRecord, 0)
	go c.WorkerList.Supervise(&c.Schedule.TaskQueue)

	c.server()
	return &c
}
