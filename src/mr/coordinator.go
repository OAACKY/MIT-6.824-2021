package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.
	fileName       []string
	nReduce        int
	mapState       []int
	reduceState    []int
	mapFinish      int
	reduceFinish   int
	mapIsfinish    bool
	reduceIsfinish bool
	timerset       bool
	beginTime      time.Time
	mux            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MrHandler(args *MrArgs, reply *MrReply) error {
	c.mux.Lock()
	if args.IsFinish == false { // worker ask for task
		if c.mapIsfinish {
			if c.reduceIsfinish { // task is done
				reply.TaskNum = -1
				defer c.mux.Unlock()
				return nil
			} else { // do reduce task
				if c.timerset == false {
					c.beginTime = time.Now()
					c.timerset = true
				}
				reply.TaskNum = 0
				reply.Type = 1
				reply.MapNum = len(c.fileName)
				i, k := 0, 0
				for i, k = range c.reduceState {
					if k == 0 {
						break
					}
				}
				if k != 0 {
					defer c.mux.Unlock()
					cost := time.Since(c.beginTime)
					if cost < 20*time.Second {
						reply.TaskNum = -1 //wait if some reduce task are being done
					} else {
						//fmt.Println("got it 2")
						reply.TaskNum = 0
					}
					return nil
				}
				reply.TaskNum = i
				c.reduceState[i] = 1 //doing
				c.mux.Unlock()

				//check if the worker has died
				go func(stateid int) {
					time.Sleep(10 * time.Second)
					c.mux.Lock()
					if c.reduceState[stateid] == 1 {
						c.reduceState[stateid] = 0
					}
					c.mux.Unlock()
				}(i)
				return nil
			}
		} else { // check if some map task are being done
			if c.timerset == false {
				c.beginTime = time.Now()
				c.timerset = true
			}
			reply.TaskNum = 0
			reply.Type = 0
			reply.ReduceNum = c.nReduce
			i, k := 0, 0
			for i, k = range c.mapState {
				if k == 0 {
					break
				}
			}
			if k != 0 {
				defer c.mux.Unlock()
				cost := time.Since(c.beginTime)
				if cost < 20*time.Second {
					reply.TaskNum = -1 //wait if some map task are being done
				} else {
					//fmt.Println("got it!")
					reply.TaskNum = 0
					reply.FileName = c.fileName[0]
				}
				return nil
			}
			reply.TaskNum = i
			reply.FileName = c.fileName[i]
			c.mapState[i] = 1 // doing
			c.mux.Unlock()

			//check if the worker has died
			go func(stateid int) {
				time.Sleep(10 * time.Second)
				c.mux.Lock()
				if c.mapState[stateid] == 1 {
					c.mapState[stateid] = 0
				}
				c.mux.Unlock()
			}(i)
			return nil
		}
	} else {
		if args.Type == 0 {
			if c.mapState[args.TaskNum] == 2 {
				defer c.mux.Unlock()
				return nil
			}
			c.mapState[args.TaskNum] = 2
			c.mapFinish++
			if c.mapFinish == len(c.fileName) {
				c.mapIsfinish = true
				c.timerset = false
			}
		} else {
			if c.reduceState[args.TaskNum] == 2 {
				defer c.mux.Unlock()
				return nil
			}
			c.reduceState[args.TaskNum] = 2
			c.reduceFinish++
			if c.reduceFinish == c.nReduce {
				c.reduceIsfinish = true
			}
		}
		defer c.mux.Unlock()
		return nil
	}
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
	c.mux.Lock()
	if c.reduceIsfinish {
		ret = true
	}
	defer c.mux.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 初始化元数据，包括文件名，初始文件的状态，中间文件的名称，reduce task的数量
	c.fileName = []string{}
	c.fileName = append(c.fileName, files...)
	c.nReduce = nReduce

	c.mapState = make([]int, len(files))
	c.reduceState = make([]int, nReduce)

	c.server()
	return &c
}
