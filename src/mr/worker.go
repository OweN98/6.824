package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
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
		// Your worker implementation here.z
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(200) + 200
		time.Sleep(time.Duration(r) * time.Millisecond)
		ask := AskTask{}
		task := AssignTask{}
		res := call("Coordinator.Ask", &ask, &task)
		if !res {
			log.Print("Job done.")
			return
		}

		if task.TaskType == MAP {
			id := task.TaskID
			filename := task.MapFiles
			nReduce := task.ReduceNum
			var intermediate_files []string
			intermediate := []KeyValue{}

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)

			split := make([][]KeyValue, nReduce)
			for i, _ := range split {
				split[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				rNum := ihash(kv.Key) % nReduce
				split[rNum] = append(split[rNum], kv)
			}
			//log.Println("Mapping task " + strconv.Itoa(id))
			outfiles := make([]*os.File, nReduce)
			for i, kva := range split {
				temp, _ := ioutil.TempFile("", "mr-tep-*")
				encode := json.NewEncoder(temp)
				for _, kv := range kva {
					err := encode.Encode(kv)
					if err != nil {
						log.Printf("can not json file %v \n", temp)
					}
				}
				outfiles[i] = temp
			}
			for i, f := range outfiles {
				filename := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
				old := filepath.Join(f.Name())
				os.Rename(old, filename)
				intermediate_files = append(intermediate_files, filename)
				f.Close()
			}

			report := Report{TaskType: MAP, TaskID: id, FilesLoc: intermediate_files}
			receive := Receive{}
			res := call("Coordinator.FinishTask", &report, &receive)
			if !res {
				//log.Println("Job done")
				return
			}
			continue
		}
		if task.TaskType == REDUCE {
			id := task.TaskID
			files := task.IntermediateFiles
			//nReduce := task.nReduceNum
			/*
				var fileTrans [][]string
				for i := 0; i < nReduce; i++ {
					var ReduceI []string
					for j := 0; j < len(files); j++ {
						ReduceI = append(ReduceI, files[j][i])
					}
					fileTrans = append(fileTrans, ReduceI)
				}*/
			// read and decode
			intermediate := []KeyValue{}

			for _, filename := range files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("can not open file %v", filename)
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
			sort.Sort(ByKey(intermediate))
			//log.Println("Reducing task " + strconv.Itoa(id))

			ofile, err := ioutil.TempFile("", "mr-tmp-*")
			if err != nil {
				log.Println(err)
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(filepath.Join(ofile.Name()), "mr-out-"+strconv.Itoa(id))
			ofile.Close()
			report := Report{TaskType: REDUCE, TaskID: id, FilesLoc: nil}
			receive := Receive{}
			res := call("Coordinator.FinishTask", &report, &receive)
			if !res {
				//log.Println("Coordinator exited, job done")
				return
			}
			continue

		}
	}

}

//
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
