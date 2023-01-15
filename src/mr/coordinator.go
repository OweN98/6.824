package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	IDLE       = 0
	PROCESSING = 1
	DONE       = 2
)

const (
	MAP    = 1
	REDUCE = 2
)

type Coordinator struct {
	// Your definitions here.
	mutex             sync.Mutex
	nMap              int
	nReduce           int
	MapDone           int
	ReduceDone        int
	MapStatus         []int
	ReduceStatus      []int
	MapChan           []chan bool
	ReduceChan        []chan bool
	MapFiles          []string
	IntermediateFiles [][]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Ask(args *AskTask, reply *AssignTask) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.MapDone < c.nMap {
		for i, v := range c.MapStatus {
			if v == IDLE {
				c.MapStatus[i] = PROCESSING
				ch := c.MapChan[i]
				go func(i int, ch chan bool) {
					timer := time.NewTimer(10 * time.Second)
					select {
					case <-ch:
						//log.Println("Map task " + strconv.Itoa(i) + " done.")
						c.mutex.Lock()
						defer c.mutex.Unlock()
						c.MapStatus[i] = DONE
						c.MapDone += 1
					case <-timer.C:
						//log.Println("Map task " + strconv.Itoa(i) + " TIMEOUT!")
						c.mutex.Lock()
						defer c.mutex.Unlock()
						c.MapStatus[i] = IDLE
					}
				}(i, ch)
				reply.TaskID = i
				reply.TaskType = MAP
				reply.ReduceNum = c.nReduce
				reply.MapFiles = c.MapFiles[i]
				//reply.TaskName = []string{}
				//reply.TaskName = append(reply.TaskName, c.MapFiles[i])
				break
			}
		}
		return nil
	}
	if c.ReduceDone < c.nReduce {
		for i, v := range c.ReduceStatus {
			if v == IDLE {
				c.ReduceStatus[i] = PROCESSING
				ch := c.ReduceChan[i]
				go func(i int, ch chan bool) {
					timer := time.NewTimer(10 * time.Second)
					select {
					case <-ch:
						//log.Println("Reduce task " + strconv.Itoa(i) + " done.")
						c.mutex.Lock()
						defer c.mutex.Unlock()
						c.ReduceStatus[i] = DONE
						c.ReduceDone += 1
					case <-timer.C:
						//log.Println("Reduce task " + strconv.Itoa(i) + " TIMEOUT!")
						c.mutex.Lock()
						defer c.mutex.Unlock()
						c.ReduceStatus[i] = IDLE
					}
				}(i, ch)

				reply.TaskID = i
				reply.TaskType = REDUCE
				//reply.TaskName = []string{}
				for j := 0; j < c.nMap; j++ {
					reply.IntermediateFiles = append(reply.IntermediateFiles, "mr-"+strconv.Itoa(j)+"-"+strconv.Itoa(i))
				}
				//reply.IntermediateFiles = c.IntermediateFiles[i]
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(args *Report, reply *Receive) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskType == MAP {
		c.MapChan[args.TaskID] <- true
	} else if args.TaskType == REDUCE {
		c.ReduceChan[args.TaskID] <- true
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.nMap == c.MapDone && c.nReduce == c.ReduceDone {
		//log.Println("**********MR OVER**********")
		time.Sleep(time.Second * 8)
		return true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:              len(files),
		nReduce:           nReduce,
		MapStatus:         make([]int, len(files)),
		ReduceStatus:      make([]int, nReduce),
		MapDone:           0,
		ReduceDone:        0,
		MapFiles:          files,
		MapChan:           make([]chan bool, len(files)),
		ReduceChan:        make([]chan bool, nReduce),
		IntermediateFiles: [][]string{},
	}

	// Your code here.
	for i := 0; i < len(files); i++ {
		c.MapStatus[i] = IDLE
		c.MapChan[i] = make(chan bool, 1)
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceStatus[i] = IDLE
		c.ReduceChan[i] = make(chan bool, 1)
	}

	c.server()
	return &c
}
