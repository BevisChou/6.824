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
	"strconv"
	"sync"
	"time"
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

type TaskInfo ReportReply

type Status struct {
	IsIdle bool
	Mu     sync.Mutex
}

func (status *Status) GetStatus() bool {
	status.Mu.Lock()
	defer status.Mu.Unlock()
	return status.IsIdle
}

func (status *Status) SetStatus(isIdle bool) {
	status.Mu.Lock()
	defer status.Mu.Unlock()
	status.IsIdle = isIdle
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

	// Your worker implementation here.
	status := Status{IsIdle: true}

	id := DoCheckin()
	taskChan := make(chan TaskInfo)

	go DoReport(id, &taskChan, &status)

	prefix := "intermediate-"

	for {
		taskInfo := <-taskChan
		status.SetStatus(false)
		if taskInfo.TaskIdentifier.JobId%2 == 0 {
			DoMap(mapf, taskInfo.FileName, prefix, taskInfo.TaskIdentifier.TaskId, taskInfo.Num)
		} else {
			DoReduce(reducef, prefix, taskInfo.Num, taskInfo.TaskIdentifier.TaskId, taskInfo.FileName)
		}
		status.SetStatus(true)
	}
}

func DoMap(mapf func(string, string) []KeyValue, fileName string, prefix string, taskId int, nReduce int) {
	inputFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	defer inputFile.Close()
	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile.Name())
	}

	pairs := mapf(fileName, string(content))

	buckets := make([][]KeyValue, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		buckets = append(buckets, make([]KeyValue, 0))
	}
	for _, pair := range pairs {
		i := ihash(pair.Key) % nReduce
		buckets[i] = append(buckets[i], pair)
	}

	prefix = prefix + strconv.Itoa(taskId) + "-"
	for i := 0; i < nReduce; i++ {
		outputFile, _ := ioutil.TempFile("", "")
		enc := json.NewEncoder(outputFile)
		for _, kv := range buckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot write %v", outputFile.Name())
			}
		}
		outputFile.Close()
		if err := os.Rename(outputFile.Name(), prefix+strconv.Itoa(i)); err != nil {
			log.Fatalf("cannot rename %v", outputFile.Name())
		}
	}
}

func DoReduce(reducef func(string, []string) string, prefix string, nMap int, taskId int, fileName string) {
	pairs := make([]KeyValue, 0)

	for i := 0; i < nMap; i++ {
		infile := prefix + strconv.Itoa(i) + "-" + strconv.Itoa(taskId)
		inputFile, err := os.Open(infile)
		if err != nil {
			log.Fatalf("cannot open %v", infile)
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			pairs = append(pairs, kv)
		}
		inputFile.Close()
	}

	sort.Sort(ByKey(pairs))

	outputFile, _ := ioutil.TempFile("", "")

	i := 0
	for i < len(pairs) {
		j := i + 1
		for j < len(pairs) && pairs[j].Key == pairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, pairs[k].Value)
		}
		res := reducef(pairs[i].Key, values)

		fmt.Fprintf(outputFile, "%v %v\n", pairs[i].Key, res)

		i = j
	}

	outputFile.Close()
	if err := os.Rename(outputFile.Name(), fileName); err != nil {
		log.Fatalf("cannot rename %v", outputFile.Name())
	}
}

//
// the RPC argument and reply types are defined in rpc.go.
//

func DoCheckin() int {
	args := CheckinArgs{}
	args.Timestamp = time.Now()
	reply := CheckinReply{}

	ok := call("Coordinator.Checkin", &args, &reply)

	id := -1
	if ok {
		id = reply.Id
	}
	return id
}

func DoReport(id int, taskChan *chan TaskInfo, status *Status) {
	for {
		args := ReportArgs{time.Now(), id, status.GetStatus()}
		reply := ReportReply{}

		ok := call("Coordinator.Report", &args, &reply)
		if ok && reply.TaskIdentifier != NO_TASK() {
			*taskChan <- TaskInfo(reply) // would not block
		}

		time.Sleep(time.Second) // report every second
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
