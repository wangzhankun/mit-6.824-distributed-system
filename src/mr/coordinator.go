package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	firstRequest int32
	waitingTasks map[int32]RequestTaskReply

	taskId int32
	// Your definitions here.
	files   []string
	nReduce int

	requestTaskReplyChannel chan RequestTaskReply
	reportTaskArgsChannel   chan ReportTaskArgs

	// mapDoneChannel chan bool

	intermediateFilesMutex sync.Mutex
	intermediateFiles      [][]string

	reduceFilesMutex sync.Mutex
	reduceFiles      []string
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	atomic.StoreInt32(&c.firstRequest, 1)
	// if channel is empty, continue without blocking
	if len(c.requestTaskReplyChannel) == 0 {
		reply.TaskType = None
		reply.TaskId = -1
	} else {
		*reply = <-c.requestTaskReplyChannel
	}
	return nil
}

func (c *Coordinator) ReportTask(report *ReportTaskArgs, reply *ReportTaskReply) error {
	c.reportTaskArgsChannel <- *report
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

func (c *Coordinator) reply2RequestTask() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for len(c.waitingTasks) != 0 {
		select {
		case <-ticker.C:
			newWaitingTasks := make(map[int32]RequestTaskReply)
			for _, reply := range c.waitingTasks {
				reply.TaskId = c.getTaskId()
				newWaitingTasks[reply.TaskId] = reply
				c.requestTaskReplyChannel <- reply
			}
			c.waitingTasks = newWaitingTasks

		case report := <-c.reportTaskArgsChannel:

			reply, ok := c.waitingTasks[report.TaskId]
			if ok && reply.TaskId == report.TaskId {
				delete(c.waitingTasks, report.TaskId)

				switch report.TaskType {
				case MapTask:
					// log.Println("map task", report.TaskId, "status", report.Status)
					if report.Status == 0 {
						c.intermediateFilesMutex.Lock()
						for i, f := range report.Files {
							c.intermediateFiles[i] = append(c.intermediateFiles[i], f)
						}
						c.intermediateFilesMutex.Unlock()
					}
				case ReduceTask:
					// log.Println("reduce task", args.TaskId, "status", args.Status)
					if report.Status == 0 {
						c.reduceFilesMutex.Lock()
						c.reduceFiles = append(c.reduceFiles, report.Files...)
						c.reduceFilesMutex.Unlock()
					}
				}
			}
			if len(c.waitingTasks) == 0 {
				// log.Println("all tasks done")
				return
			}
		}
	}
}

func (c *Coordinator) coordinate() {
	for atomic.LoadInt32(&c.firstRequest) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for f := range c.files {
		reply := RequestTaskReply{
			TaskType:       MapTask,
			TaskId:         c.getTaskId(),
			InputFileNames: []string{c.files[f]},
			NReduce:        c.nReduce,
		}
		c.requestTaskReplyChannel <- reply
		c.waitingTasks[reply.TaskId] = reply
	}
	c.reply2RequestTask()

	// log.Println(c.intermediateFiles)

	for i := 0; i < c.nReduce; i++ {
		reply := RequestTaskReply{
			TaskType:       ReduceTask,
			TaskId:         c.getTaskId(),
			InputFileNames: c.intermediateFiles[i],
			NReduce:        c.nReduce,
			OutputFile:     fmt.Sprintf("mr-out-%v", i),
		}
		c.requestTaskReplyChannel <- reply
		c.waitingTasks[reply.TaskId] = reply
	}
	c.reply2RequestTask()

	// log.Println("MapReduce done")

	// log.Println(c.reduceFiles)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.reduceFilesMutex.Lock()
	ret := len(c.reduceFiles) == c.nReduce
	c.reduceFilesMutex.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	c.firstRequest = 0
	c.waitingTasks = make(map[int32]RequestTaskReply)
	c.taskId = 0
	c.files = files
	c.nReduce = nReduce
	c.reportTaskArgsChannel = make(chan ReportTaskArgs)
	c.requestTaskReplyChannel = make(chan RequestTaskReply, 1000)
	c.intermediateFiles = make([][]string, c.nReduce)

	go c.coordinate()

	c.server()
	return &c
}

func (c *Coordinator) getTaskId() int32 {
	ret := atomic.AddInt32(&c.taskId, 1)
	// 没有处理溢出的情况
	return ret
}
