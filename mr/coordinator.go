package mr

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	stage     string //map or reduce or done
	lock      sync.Locker
	nMap      int
	nReduce   int
	TaskList  map[string]Task
	TodoTasks chan Task //Todo tasks
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ApplyforTasks(args *ApplyforTaskArgs, reply *ReplyTaskArgs) error {
	// if worker have done a task
	if args.LastTaskId != -1 {
		c.lock.Lock()
		taskId := CreateTaskId(args.LastTaskType, args.LastTaskId)
		// if the worker is the one we specified, then it is ok
		if task, ok := c.TaskList[taskId]; ok && task.WorkerId == args.WorkerId {
			log.Printf("Worker: %d  has done task: %s %d ", args.WorkerId, args.LastTaskType, args.LastTaskId)
			if args.LastTaskType == "map" {
				for i := 0; i < c.nReduce; i++ {
					err := os.Rename(tmpMapOutFile(args.WorkerId, args.LastTaskId, i), finalMapOutFile(args.LastTaskId, i))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(args.WorkerId, args.LastTaskId, i), err)
					}
				}
			} else if args.LastTaskType == "reduce" {
				err := os.Rename(tmpReduceOutFile(args.WorkerId, args.LastTaskId), finalReduceOutFile(args.LastTaskId))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.WorkerId, args.LastTaskId), err)
				}
			}
			delete(c.TaskList, taskId)
			if len(c.TaskList) == 0 {
				c.SwitchStage()
			}
		}

		c.lock.Unlock()
	}

	// allocate a new task
	task, ok := <-c.TodoTasks
	if !ok {
		log.Printf("All tasks have been done!")
		reply.Type = "ok"
		return nil
	}
	// update task
	c.lock.Lock()
	task.WorkerId = args.WorkerId
	task.DeadLine = time.Now().Add(time.Second * 10)
	c.TaskList[CreateTaskId(task.Type, task.Id)] = task

	reply.TaskId = task.Id
	reply.Type = task.Type
	reply.FileName = task.FileName
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	c.lock.Unlock()

	return nil
}

func (c *Coordinator) SwitchStage() {
	if c.stage == "map" {
		// create reduce tasks
		log.Printf("All map tasks have been done! Switching to Reduce mod")
		c.stage = "reduce"
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Id:       i,
				Type:     "reduce",
				WorkerId: -1,
			}
			c.TaskList[CreateTaskId(task.Type, task.Id)] = task
			c.TodoTasks <- task
		}
	} else if c.stage == "reduce" {
		log.Printf("All reduce tasks have been done! Ending..")
		c.stage = "done"
		close(c.TodoTasks)
	}

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.lock.Lock()
	// Your code here.
	if c.stage == "done" {
		ret = true
	}
	c.lock.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// The job for Coordinator:
// 1.Create map tasks, waiting for workers to apply these tasks
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.stage = "map"
	c.nMap = len(files)
	c.nReduce = nReduce
	c.TodoTasks = make(chan Task, int(math.Max(float64(len(files)), float64(nReduce))))
	c.TaskList = make(map[string]Task)
	c.lock = &sync.Mutex{}

	for i, file := range files {
		task := Task{
			Id:       i,
			Type:     "map",
			FileName: file,
			WorkerId: -1,
		}
		c.TaskList[CreateTaskId(task.Type, task.Id)] = task
		c.TodoTasks <- task
	}
	log.Printf("Coordinator starts!\n")
	c.server()

	// recycle the time-out tasks
	go func() {
		for {
			c.lock.Lock()
			for _, task := range c.TaskList {
				if task.WorkerId != -1 && time.Now().After(task.DeadLine) {
					log.Printf("Worker: %d, executing task: %s %d error...", task.WorkerId, task.Type, task.Id)
					task.WorkerId = -1
					c.TodoTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func CreateTaskId(taskType string, taskId int) string {
	return taskType + strconv.Itoa(taskId)
}

func tmpMapOutFile(workerId int, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerId, mapId, reduceId)
}

func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
