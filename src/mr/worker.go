package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	_ = os.Mkdir("6.824tmp", 0777)
	for {
		args := Args{}
		args.WorkerId = rand.Int()
		id := args.WorkerId
		reply := Reply{}
		ok := call("Coordinator.Handler", &args, &reply) //申请任务
		if ok {
			if reply.State == 1 {
				MapTask(mapf, reply, id)
			} else if reply.State == 2 {
				ReduceTask(reducef, reply, id)
			} else if reply.State == 0 {
				fmt.Printf("全部任务已结束\n")
				os.Exit(0)
			} else { //if reply.State == -1
				//fmt.Printf("暂无任务可做\n")
				time.Sleep(10 * time.Millisecond)
			}

		} else {
			fmt.Printf("call failed!\n")
		}
	}
}

func MapTask(mapf func(string, string) []KeyValue, reply Reply, id int) {
	kva := mapf(reply.Files[reply.TaskId], reply.FileContent)

	intermediate := make([][]KeyValue, reply.NReduce)
	tmpfile := make([]*os.File, reply.NReduce)

	//sort.Sort(ByKey(kva))
	for _, kv := range kva {
		intermediate[ihash(kv.Key)%reply.NReduce] = append(intermediate[ihash(kv.Key)%reply.NReduce], kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		tmpfile[i], _ = os.CreateTemp("6.824tmp", "")

		tmpcontent, _ := json.Marshal(intermediate[i])
		_, err := fmt.Fprintf(tmpfile[i], string(tmpcontent))
		if err != nil {
			log.Fatalf("cannot write %v", tmpfile[i].Name())
		}

		err = tmpfile[i].Close()
		if err != nil {
			log.Fatalf("cannot close %v", tmpfile[i].Name())
		}
	}
	for i := 0; i < reply.NReduce; i++ {
		newpath := "mapresult_" + strconv.Itoa(reply.TaskId) + "_" + strconv.Itoa(i+1) + ".txt"

		err := os.Rename(tmpfile[i].Name(), newpath)
		if err != nil {
			fmt.Println(err)
			log.Fatalf("cannot rename %v", tmpfile[i].Name())
		}
	}
	for !call("Coordinator.MapDone", &DoneArgs{WorkerId: id}, &reply) {
	} //向coordinator汇报完成

}

func ReduceTask(reduecf func(string, []string) string, reply Reply, id int) {
	ofile, _ := os.CreateTemp("6.824tmp", "")

	intermediate := []KeyValue{}

	for i := range reply.Files {
		tmp := []KeyValue{}
		file, err := os.Open("mapresult_" + strconv.Itoa(i) + "_" + strconv.Itoa(reply.TaskId) + ".txt")
		if err != nil {
			log.Fatalf("cannot open %v", file.Name())
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", file.Name())
		}
		err = json.Unmarshal(content, &tmp)
		if err != nil {
			log.Fatalf("cannot unmarshal %v", file.Name())
		}

		intermediate = append(intermediate, tmp...)
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", file.Name())
		}
	}
	sort.Sort(ByKey(intermediate))
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
		output := reduecf(intermediate[i].Key, values)
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write %v", ofile.Name())
		}
		i = j
	}
	err := os.Rename(ofile.Name(), "mr-out-"+strconv.Itoa(reply.TaskId))
	if err != nil {
		fmt.Println(err)
		return
	}
	err = ofile.Close()
	if err != nil {
		log.Fatalf("cannot close %v", ofile.Name())
	}
	for !call("Coordinator.ReduceDone", &DoneArgs{WorkerId: id}, &reply) {
	}

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
