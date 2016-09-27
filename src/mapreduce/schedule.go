package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		fmt.Println("Entering reduce phase...")
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	var wg sync.WaitGroup
	tasks := make(chan int, ntasks)
	// put all tasks to tasks channel
	for taskIndex := 0; taskIndex < ntasks; taskIndex++ {
		tasks <- taskIndex
	}
	wg.Add(ntasks)
	go func() {
		for {
			select {
			case taskIndex := <-tasks:
				go func(taskNumber int) {
					worker := <-mr.registerChannel

					var f string
					switch phase {
					case mapPhase:
						f = mr.files[taskNumber]
					case reducePhase:
						f = ""
					}
					args := &DoTaskArgs{
						JobName:       mr.jobName,
						File:          f,
						Phase:         phase,
						TaskNumber:    taskNumber,
						NumOtherPhase: nios,
					}
					ok := call(worker, "Worker.DoTask", args, nil)
					if !ok {
						// if RPC failed, put the task back to task list
						// and return without notify wait group and re-register
						tasks <- taskNumber
						return
					}
					wg.Done()
					mr.registerChannel <- worker
				}(taskIndex)
			}
		}
	}()
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
