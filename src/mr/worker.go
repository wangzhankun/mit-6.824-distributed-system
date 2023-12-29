package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

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

	// log 打印时打印行号
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	for {
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &RequestTaskArgs{}, &reply)

		if ok {
			reportTask := ReportTaskArgs{
				TaskType: None,
				TaskId:   -1,
			}
			switch reply.TaskType {
			case None:
				// log.Println("no task, sleep 1s")
				time.Sleep(1 * time.Second)
			case MapTask:
				reportTask.Files = doMap(mapf, reply.InputFileNames, reply.NReduce)
			case ReduceTask:
				reportTask.Files = doReduce(reducef, reply.InputFileNames, reply.OutputFile)
			default:
				log.Fatal("unknown task type")
			}

			if reply.TaskType != None {
				reportTask.TaskType = reply.TaskType
				reportTask.TaskId = reply.TaskId
				reportTask.Status = 0
				call("Coordinator.ReportTask", &reportTask, &ReportTaskReply{})
			}
		} else {
			fmt.Printf("call failed!\n")
			return
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, filenames []string, nReduce int) []string {
	// log.Println("doMap", filenames, nReduce)

	intermediates := make([][]KeyValue, nReduce)

	for _, filename := range filenames {
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		kva := mapf(filename, string(content))
		for _, kv := range kva {
			reduceTaskId := ihash(kv.Key) % nReduce
			intermediates[reduceTaskId] = append(intermediates[reduceTaskId], kv)
		}
	}

	tmpFiles := make([]string, nReduce)
	for i, kvs := range intermediates {
		tmpFiles[i] = fmt.Sprintf("/var/tmp/mr-intermediate-%v-%v.txt", i, time.Now().UnixNano())

		tmpFile, err := os.Create(tmpFiles[i])
		if err != nil {
			log.Fatal("create tmp file failed", err)
		}
		defer tmpFile.Close()

		for _, kv := range kvs {
			tmpFile.Write([]byte(fmt.Sprintf("%v %v\n", kv.Key, kv.Value)))
		}
	}

	// log.Println("doMap done", filenames, nReduce, tmpFiles)
	return tmpFiles
}

func doReduce(reducef func(string, []string) string, intermediateFiles []string, oname string) []string {
	kvsChannel := make(chan []KeyValue)

	for _, filename := range intermediateFiles {
		go func(filename string) {
			file, err := os.Open(filename)

			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			defer file.Close()

			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}

			contentStr := strings.Split(string(content), "\n")

			kvs_tmp := []KeyValue{}

			for _, line := range contentStr {
				kv := KeyValue{}
				if line == "" {
					continue
				}
				// log.Println("doReduce line", line)
				fmt.Sscanf(line, "%v %v", &kv.Key, &kv.Value)
				kvs_tmp = append(kvs_tmp, kv)
			}

			sort.Sort(ByKey(kvs_tmp))

			// log.Println("doReduce done ", filename)
			kvsChannel <- kvs_tmp
		}(filename)
	}

	intermediate := []KeyValue{}
	for i := 0; i < len(intermediateFiles); i++ {
		kvs := <-kvsChannel
		intermediate = append(intermediate, kvs...)
	}
	defer close(kvsChannel)

	sort.Sort(ByKey(intermediate))

	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	defer ofile.Close()

	return []string{oname}
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
