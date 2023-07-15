package mr

import (
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	state          atomic.Int32 //1 for map, 2 for reduce,0 for done
	mapWorkChan    chan int
	reduceWorkChan chan int
	//isDone     bool
	//mu         sync.Mutex
	workingMap sync.Map //map[int]int
	nReduce    int
	files      []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Handler(args *Args, reply *Reply) error {
	//fmt.Print("get request\n")

	reply.State = int(c.state.Load())

	if reply.State == 1 {
		if len(c.mapWorkChan) == 0 {
			reply.State = -1
			return nil
		}
		c.MapHandler(args, reply)
	} else if reply.State == 2 {
		if len(c.reduceWorkChan) == 0 {
			reply.State = -1
			return nil
		}
		c.ReduceHandler(args, reply)
	}

	//if c.state == 1 {
	//	c.MapHandler(args, reply)
	//} else if c.state == 2 {
	//	c.ReduceHandler(args, reply)
	//}

	return nil
}

func (c *Coordinator) ReduceHandler(args *Args, reply *Reply) {
	reply.TaskId = <-c.reduceWorkChan
	reply.Files = c.files
	id := args.WorkerId
	c.workingMap.Store(id, reply.TaskId)
	go func() { //超时重传
		time.Sleep(10 * time.Second)
		if taskId, ok := c.workingMap.Load(id); ok {
			c.reduceWorkChan <- taskId.(int)
			c.workingMap.Delete(id)
		}
	}()
}

func (c *Coordinator) MapHandler(args *Args, reply *Reply) {
	reply.TaskId = <-c.mapWorkChan
	c.workingMap.Store(args.WorkerId, reply.TaskId)
	reply.NReduce = c.nReduce
	reply.Files = c.files

	file, err := os.Open(c.files[reply.TaskId])
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskId)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskId)
	}
	reply.FileContent = string(content)
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", c.files[reply.TaskId])
		return
	}

	id := args.WorkerId
	//c.workingMap.Store(id, reply.TaskId)
	go func() { //超时重传
		time.Sleep(10 * time.Second)
		if taskId, ok := c.workingMap.Load(id); ok {
			c.mapWorkChan <- taskId.(int)
			c.workingMap.Delete(id)
		}
	}()
}

func (c *Coordinator) MapDone(args *DoneArgs, reply *DoneReply) error {
	c.workingMap.Delete(args.WorkerId)
	return nil
}

func (c *Coordinator) ReduceDone(args *DoneArgs, reply *DoneReply) error {
	c.workingMap.Delete(args.WorkerId)
	return nil
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
	//ret := false

	// Your code here.

	return c.state.Load() == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.state.Store(1)
	//c.isDone = false
	c.mapWorkChan = make(chan int, 10)
	c.reduceWorkChan = make(chan int, 10)
	c.nReduce = nReduce
	c.files = files
	c.server()
	go func() {
		for i := 0; i < len(c.files); i++ {
			c.mapWorkChan <- i
		}
		for {
			time.Sleep(50 * time.Millisecond)
			if len(c.mapWorkChan) == 0 {
				size := 0
				c.workingMap.Range(func(key, value interface{}) bool {
					size++
					return true
				})
				if size == 0 {

					c.state.Store(2)

					for i := 1; i <= nReduce; i++ {
						c.reduceWorkChan <- i
					}
					break
				}
			}
		}
		for {
			time.Sleep(50 * time.Millisecond)
			if len(c.reduceWorkChan) == 0 {
				size := 0
				c.workingMap.Range(func(key, value interface{}) bool {
					size++
					return true
				})
				if size == 0 {

					c.state.Store(0)

					break
				}
			}
		}
	}()
	return &c
}
