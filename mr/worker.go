package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	pid := os.Getpid()
	log.Printf("Worker %d is working...\n", pid)

	lastTaskId := -1
	lastTaskType := ""
	for {
		args := ApplyforTaskArgs{
			WorkerId:     pid,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
		}
		reply := ReplyTaskArgs{}
		call("Coordinator.ApplyforTasks", &args, &reply)
		if reply.Type == "map" {
			HandleMapTask(pid, reply.TaskId, reply.FileName, reply.NReduce, mapf)
		} else if reply.Type == "reduce" {
			HandleReduceTask(pid, reply.TaskId, reply.NMap, reducef)
		} else if reply.Type == "ok" {
			break
		}
		lastTaskId = reply.TaskId
		lastTaskType = reply.Type
	}
	log.Printf("Worker %d is done\n", pid)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func HandleMapTask(id int, taskId int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("File open error: %v", err)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("File read error: %v", err)
	}
	kva := mapf(fileName, string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hash := ihash(kv.Key) % nReduce
		hashedKva[hash] = append(hashedKva[hash], kv)
	}
	// i is reduce task number
	for i := 0; i < nReduce; i++ {
		//oname := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
		oname := tmpMapOutFile(id, taskId, i)
		ofile, _ := os.Create(oname)
		for _, kv := range hashedKva[i] {
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}

}

func HandleReduceTask(id int, taskId int, nMap int, reducef func(string, []string) string) {
	var lines []string
	var file *os.File
	var err error
	for i := 0; i < nMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskId)
		file, err = os.Open(iname)
		if err != nil {
			log.Fatalf("The file %s cannot be opened!\n", err)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("The file %s cannot be read!\n", err)
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	file.Close()

	var kv []KeyValue

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		pair := strings.Split(line, "\t")
		kv = append(kv, KeyValue{
			Key:   pair[0],
			Value: pair[1],
		})
	}

	sort.Sort(ByKey(kv))

	// oname := "mr-out-" + strconv.Itoa(taskId)
	oname := tmpReduceOutFile(id, taskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output := reducef(kv[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kv[i].Key, output)

		i = j
	}

	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
