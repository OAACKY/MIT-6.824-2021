package mr

import (
	"fmt"
	"io/ioutil"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "strconv"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := MrArgs{}
		args.IsFinish = false

		reply := MrReply{}
		// 调用Call函数通过RPC请求coordinator work的类型，以及文件的位置
		for {
			reply.TaskNum = 0
			err := call("Coordinator.MrHandler", &args, &reply)
			if err == false {
				return
			} else { // work循环等待直到最后一个map任务结束，或者task已完成等待coord退出，或者等待某些map&reduce task 正在做
				if reply.TaskNum == -1 {
					time.Sleep(time.Second)
				} else {
					break
				}
			}
			//fmt.Println("task num:",reply.TaskNum)
		}
		if reply.Type == 0 { // map task
			filename := reply.FileName

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			intermediate := mapf(filename, string(content))
			// hash key to the different reduce task
			tempName := "mr-" + strconv.Itoa(reply.TaskNum) + "-"
			interFile := make([]*os.File, reply.ReduceNum)
			enc := make([]*json.Encoder, reply.ReduceNum)

			for i := 0; i < reply.ReduceNum; i++ {
				ifile, _ := ioutil.TempFile("./", "tempInterFile"+strconv.Itoa(i))
				interFile[i] = ifile
				enc[i] = json.NewEncoder(ifile)
			}

			for _, kv := range intermediate {
				index := ihash(kv.Key) % reply.ReduceNum
				err := enc[index].Encode(&kv)
				if err != nil {
					log.Fatalf("encode error #{reply.TaskNum}")
				}
			}

			for i := 0; i < reply.ReduceNum; i++ {
				interName := tempName + strconv.Itoa(i)
				os.Rename(interFile[i].Name(), interName)
				interFile[i].Close()
			}

		} else { // reduce task
			intermediate := []KeyValue{}

			for i := 0; i < reply.MapNum; i++ { // read the intermediate file
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNum)
				file, err := os.Open(filename)

				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				dec := json.NewDecoder(file)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}

				file.Close()
			}

			// sort the intermediate file
			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
			ofile, _ := ioutil.TempFile("./", "tempReduceFile"+strconv.Itoa(reply.TaskNum))

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

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			os.Rename(ofile.Name(), oname)
			ofile.Close()

		}

		args.Type = reply.Type
		args.IsFinish = true
		args.TaskNum = reply.TaskNum
		call("Coordinator.MrHandler", &args, &reply)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
