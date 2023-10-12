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

	for {
		// Your worker implementation here.
		args := RequestArgs{}
		reply := ReplyArgs{}
		reply.Response = ""
		// Get the task to execute.
		CallTask(&args, &reply)
		//fmt.Println("Called for task \n")
		switch task_type := reply.Task_Type; task_type {
		case "map":
			args.Task_Type = reply.Task_Type
			args.Task_Id = reply.Task_Id
			args.FileName = reply.FileName

			//log.Printf("Get map task, task id: %v, filename: %v", reply.Task_Id, reply.FileName)
			ret, err := MapTask(&reply, mapf)
			if err == nil {
				args.Task_Status = "success"
				args.Intermediate_FileNames = ret
			}
			CallDoneTask(&args, &reply)
			// if len(reply.Response) != 0 {
			// 	log.Printf("Response :%s \n", reply.Response)
			// }
		case "reduce":
			args.Task_Type = reply.Task_Type
			//log.Printf("Get reduce task, task id: %v \n", reply.Task_Id)
			ReduceTask(&reply, reducef)
			args.Task_Status = "success"
			args.Task_Id = reply.Task_Id
			CallDoneTask(&args, &reply)

		case "wait_last_map":
			//log.Printf("Wait for some other map task to end \n")
			time.Sleep(time.Second)
		case "wait_last_reduce":
			//log.Printf("Wait for some other reduce task to end \n")
			time.Sleep(time.Second)
		case "done":
			log.Printf("Job is done \n")
			// Worker can be exit
			os.Exit(0)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func MapTask(reply *ReplyArgs, mapf func(string, string) []KeyValue) ([]string, error) {
	intermediate := []KeyValue{}
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Printf("cannot open %v", reply.FileName)
		return []string{}, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", reply.FileName)
		return []string{}, err
	}
	kva := mapf(reply.FileName, string(content))
	intermediate = append(intermediate, kva...)

	// Sort all the contents.
	sort.Sort(ByKey(intermediate))

	key_content_map := map[int][]KeyValue{}

	hashk := -1
	var i = 0
	for j, item := range intermediate {
		new_hashk := ihash(item.Key) % reply.NReduce
		if hashk == -1 {
			hashk = new_hashk
			continue
		} else {
			if hashk == new_hashk {
				if j+1 < len(intermediate) {
					continue
				}
				key_content_map[hashk] = append(key_content_map[hashk], intermediate[i:j+1]...)
				break
			} else {
				if j+1 < len(intermediate) {
					key_content_map[hashk] = append(key_content_map[hashk], intermediate[i:j]...)
					i = j
					hashk = new_hashk
				} else {
					key_content_map[hashk] = append(key_content_map[hashk], intermediate[i:j]...)
					key_content_map[new_hashk] = append(key_content_map[new_hashk], intermediate[j:]...)
					break
				}
			}
		}
	}
	intermediate_map_files := []string{}

	for hashk, contents := range key_content_map {
		intermediate_f_name := fmt.Sprintf("mr-out-%v-%v", reply.Task_Id, hashk)

		intermediate_map_files = append(intermediate_map_files, intermediate_f_name)
		// Make sure that only one intermediate file exists.
		os.Remove(intermediate_f_name)
		ofile, _ := os.Create(intermediate_f_name)
		enc := json.NewEncoder(ofile)
		for _, kv := range contents {
			enc.Encode(kv)
		}
		defer ofile.Close()
	}
	return intermediate_map_files, nil
}

func ReduceTask(reply *ReplyArgs, reducef func(string, []string) string) {
	filenames := reply.Intermediate_FileNames

	// indexes := make([]int, len(filenames))
	// contents := make([][]string, len(filenames))
	intermediate := []KeyValue{}
	var kv KeyValue
	// Initialize these things.
	for _, filename := range filenames {
		file, err := os.Open(filename)

		if err != nil {
			log.Printf("can not read %v \n", filename)
		}
		dec := json.NewDecoder(file)
		for {
			if err := dec.Decode(&kv); err != nil {
				fmt.Printf("%v", err)
				break
			}
			intermediate = append(intermediate, kv)
		}
		//content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Printf("can not read %v \n", filename)
		}
		file.Close()
		defer os.Remove(filename)
	}

	sort.Sort(ByKey(intermediate))

	tmpfile, err := os.CreateTemp("./", "tmp")
	//defer tmpfile.Close()
	if err != nil {
		log.Printf("Create tmp file failed")
	}

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
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	finalName := fmt.Sprintf("mr-out-%v", reply.Task_Id)
	err = os.Rename(tmpfile.Name(), finalName)
	if err != nil {
		log.Printf("os.Rename %s to %s failed", tmpfile.Name(), finalName)
	}
}

// Call for the task from the coordinator.
func CallTask(request *RequestArgs, reply *ReplyArgs) {
	call("Coordinator.Task", request, reply)
	//fmt.Printf("reply.Task_Type %v \n", reply.Task_Type)
}

func CallDoneTask(requestArgs *RequestArgs, reply *ReplyArgs) {
	call("Coordinator.DoneTask", requestArgs, reply)
	//fmt.Printf("reply.Response: %v \n", reply.Response)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
