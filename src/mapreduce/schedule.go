package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	switch phase {
	case mapPhase:
		var wg sync.WaitGroup
		for taskNumber, file := range mapFiles {
			wg.Add(1)
			go func(file string, n_other int, taskNumber int) {
				defer wg.Done()
				workerUrl := <-registerChan

				doTaskArgs := DoTaskArgs{
					File:          file,
					JobName:       jobName,
					Phase:         mapPhase,
					NumOtherPhase: n_other,
					TaskNumber:    taskNumber,
				}

				call(workerUrl, "Worker.DoTask", doTaskArgs, nil)

				/* worker 完成工作后，将 workerUrl 放回 channel 中，可以给下一个 */
				go func() {
					registerChan <- workerUrl
				}()
			}(file, n_other, taskNumber)
		}
		wg.Wait()

	case reducePhase:
		var wg sync.WaitGroup
		for i := 0; i < nReduce; i++ {
			wg.Add(1)
			go func(taskNumber int) {
				defer wg.Done()
				workerUrl := <-registerChan
				doTaskArgs := DoTaskArgs{
					File:          "",
					JobName:       jobName,
					Phase:         reducePhase,
					NumOtherPhase: n_other,
					TaskNumber:    taskNumber,
				}

				call(workerUrl, "Worker.DoTask", doTaskArgs, nil)

				go func() {
					registerChan <- workerUrl
				}()
			}(i)
		}
		wg.Wait()
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
