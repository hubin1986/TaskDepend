#Summary
A task execution framework based on Directed Acyclic Graph (DAG), which automatically handles complex 
task relationships through a simple API and provides comprehensive error handling, timeout handling, 
and coroutine pool processing mechanisms


#Quick Start
/**
* 单任务超时
* taskE.SetTimeout(500)
 */
func Example1() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	taskE.SetTimeout(50)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecute()
	fmt.Println("isDone TaskD:", taskD.IsDone())
	fmt.Println("isDone TaskE:", taskE.IsDone())
	//fmt.Println(taskA)
	//fmt.Println(taskB)
}
