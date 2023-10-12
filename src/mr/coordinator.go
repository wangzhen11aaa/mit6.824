package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type void struct{}

var member void

type Coordinator struct {
	// When process files need to synchronization.
	// The task is non-state for the coordinator, the coordinator only cares the files empty or not.
	// Mutex for map stage and reduce stage.
	mu      sync.Mutex
	NReduce int
	// Your definitions here.
	// All files/splits need to be processed.
	files_process_map map[string]int
	// filename: [map_id, timestamp]
	files_in_process_map map[string][2]int64

	// Store the intermediate files in the Master
	id_intermediate_files_map map[int][]string

	files_done_process_map map[string]int

	map_done_chan chan string
	// reduce_id: timestamp
	reduce_done_chan chan int

	reduce_id_to_mapfiles_map map[int][]string
	// Mutex for reduce stage. Read the file name like mr-map_id-reduce_id.
	reduce_process_id_files_map map[int]struct{}
	reduce_in_process_ids_map   map[int]int64
	reduce_done_process_id_map  map[int]struct{}
	// stage = 0:means map, =1 means reduce
	stage int
}

// Your code here -- RPC handlers for the worker to call.

/// RPC handler
func (c *Coordinator) Task(request *RequestArgs, reply *ReplyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// map stage
	time := time.Now().Unix()
	if c.stage == 0 {
		if len(c.files_process_map) != 0 {
			reply.Task_Type = "map"
			reply.NReduce = c.NReduce
			for k, v := range c.files_process_map {
				reply.FileName = k
				reply.Task_Id = int(v)
				break
			}
			delete(c.files_process_map, reply.FileName)
			c.files_in_process_map[reply.FileName] = [2]int64{int64(reply.Task_Id), time}
			return nil
		} else {
			if len(c.files_in_process_map) != 0 {
				reply.Task_Type = "wait_last_map"
				return nil
			} else {
				// Enter into reduce stage.
				c.stage = 1
			}
		}
	}
	// Here assign reduce task to the worker
	if len(c.reduce_process_id_files_map) == 0 {
		if len(c.reduce_in_process_ids_map) != 0 {
			reply.Task_Type = "wait_last_reduce"
		} else {
			reply.Task_Type = "done"
		}
	} else {
		for id, _ := range c.reduce_process_id_files_map {
			reply.Task_Id = id
			break
		}
		reply.Intermediate_FileNames = append(reply.Intermediate_FileNames, c.id_intermediate_files_map[reply.Task_Id]...)
		delete(c.reduce_process_id_files_map, reply.Task_Id)
		c.reduce_in_process_ids_map[reply.Task_Id] = time
		reply.Task_Type = "reduce"
	}
	return nil
}

func (c *Coordinator) DoneTask(args *RequestArgs, reply *ReplyArgs) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.Task_Type = args.Task_Type
	reply.Task_Id = args.Task_Id

	if args.Task_Status != "success" {
		log.Printf(" %v task %v error \n", args.Task_Type, args.Task_Id)
		return nil
	}
	if args.Task_Type == "map" {
		filename := args.FileName
		reply.FileName = filename
		_, ok := c.files_in_process_map[filename]
		if ok {
			c.map_done_chan <- filename
			for _, filename := range args.Intermediate_FileNames {
				idx, _ := strconv.Atoi((strings.Split(filename, "-")[3]))
				c.id_intermediate_files_map[idx] = append(c.id_intermediate_files_map[idx], filename)
			}
			reply.Response = "cool"
		} else {
			reply.Response = fmt.Sprintf("timeout process %s", filename)
		}
	} else {
		reply.Task_Id = args.Task_Id
		_, ok := c.reduce_in_process_ids_map[reply.Task_Id]
		if ok {
			reply.Response = "cool"
			c.reduce_done_chan <- reply.Task_Id
		} else {
			reply.Response = fmt.Sprintf("timeout process reduce_id: %v \n", reply.Task_Id)
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// This is a synchronous RPC call.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
		log.Println("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	if len(c.files_in_process_map) == 0 && len(c.files_process_map) == 0 && len(c.reduce_in_process_ids_map) == 0 && len(c.reduce_process_id_files_map) == 0 {
		ret = true
	}
	return ret
}

func (c *Coordinator) MarkMapTaskSuccess(filename string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := c.files_in_process_map[filename]
	c.files_done_process_map[filename] = int(values[0])
	delete(c.files_in_process_map, filename)
}

func (c *Coordinator) MarkReduceTaskSuccess(reduce_id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.reduce_in_process_ids_map, reduce_id)
	c.reduce_done_process_id_map[reduce_id] = struct{}{}
}

func (c *Coordinator) TryReDoTask(now int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.files_in_process_map) != 0 {
		for filename, vals := range c.files_in_process_map {
			// timeout
			if (now - vals[1]) >= 10 {
				c.files_process_map[filename] = int(vals[0])
				fmt.Printf("map_task %s added into files_process_map via timeout", filename)
				delete(c.files_in_process_map, filename)
			}
		}
	}
	if len(c.reduce_done_process_id_map) != 0 {
		for k, v := range c.reduce_in_process_ids_map {
			if now-v >= 10 {
				c.reduce_process_id_files_map[k] = struct{}{}
				fmt.Printf("%d added into reduce_process_id_files_map via timeout", k)
				delete(c.reduce_in_process_ids_map, k)
			}
		}
	}
	return nil
}

func (c *Coordinator) checkTask() error {
	for true {
		select {
		case map_task_hd := <-c.map_done_chan:
			c.MarkMapTaskSuccess(map_task_hd)

		case reduce_task_hd := <-c.reduce_done_chan:
			c.MarkReduceTaskSuccess(reduce_task_hd)

		case <-time.After(time.Second):
			//fmt.Printf("Need Schedule to check")
			c.TryReDoTask(time.Now().Unix())
		}
	}
	return nil
}

func InitCoordinator(c *Coordinator, nReduce int) {
	c.files_process_map = make(map[string]int)
	c.files_done_process_map = make(map[string]int)
	c.files_in_process_map = make(map[string][2]int64)

	c.NReduce = nReduce

	c.id_intermediate_files_map = make(map[int][]string)

	c.reduce_process_id_files_map = make(map[int]struct{})
	c.reduce_in_process_ids_map = make(map[int]int64)
	c.reduce_done_process_id_map = make(map[int]struct{})

	c.map_done_chan = make(chan string, nReduce)
	c.reduce_done_chan = make(chan int, nReduce)

	c.stage = 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	InitCoordinator(&c, nReduce)

	for i, file := range files {
		c.files_process_map[file] = i
	}

	for i := 0; i < nReduce; i++ {
		c.reduce_process_id_files_map[i] = struct{}{}
	}

	// Your code here.
	// timers
	go c.checkTask()
	c.server()
	return &c
}
