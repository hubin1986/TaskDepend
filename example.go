package task

import (
	"fmt"
	"os"
	"time"
)

type TaskA struct {
}

func (p *TaskA) Execute(node *TaskNode) bool {
	time.Sleep(100 * time.Millisecond)
	return true
}

func (p *TaskA) GetResult() interface{} {
	return "TaskA"
}

type TaskB struct {
	result string
}

// 方法
func (p *TaskB) Execute(node *TaskNode) bool {
	time.Sleep(500 * time.Millisecond)
	p.result = "B"
	//return true
	return true
}

// 接口
func (p *TaskB) GetResult() interface{} {
	return p.result
}

type TaskFail struct {
	result string
}

// 方法
func (p *TaskFail) Execute(node *TaskNode) bool {
	time.Sleep(500 * time.Millisecond)
	p.result = "B"
	//return true
	return false
}

// 接口
func (p *TaskFail) GetResult() interface{} {
	return p.result
}

type TaskC struct {
}

// 方法
func (p *TaskC) Execute(node *TaskNode) bool {
	time.Sleep(100 * time.Millisecond)
	return true
}

// 接口
func (p *TaskC) GetResult() interface{} {
	return "TaskC"
}

type TaskD struct {
}

// 方法
func (p *TaskD) Execute(node *TaskNode) bool {
	time.Sleep(100 * time.Millisecond)
	result := node.GetResutlt("TaskB")
	fmt.Fprintln(os.Stdout, []any{"resutB:%v", result}...)
	return true
}

// 接口
func (p *TaskD) GetResult() interface{} {
	return "TaskD"
}

type TaskE struct {
}

// 方法
func (p *TaskE) Execute(node *TaskNode) bool {
	time.Sleep(100 * time.Millisecond)
	result := node.GetResutlt("taskD")
	taskD := node.GetWaitNode("taskD")
	fmt.Fprintln(os.Stdout, []any{"resultD:%v:", result}...)
	var a []any = []any{"taskD time:%d:", taskD.GetExecuteTime()}
	fmt.Fprintln(os.Stdout, a...)
	return true
}

// 接口
func (p *TaskE) GetResult() interface{} {
	return "TaskE"
}

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

/**
* 整体超时
* taskExecute.SetTimeout(600)
 */
func Example2() {
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
	taskExecute.SetTimeout(1200)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecute()
	//fmt.Println(taskA)
	//fmt.Println(taskB)
}

/**
* 异常检测
 */
func Example3() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	err := taskA.TaskDepend(taskA)
	if nil != err {
		fmt.Println(err)
	}
	err = taskA.TaskDepend(nil)
	if nil != err {
		fmt.Println(err)
	}
	err = taskA.TaskDependName("not exist")
	if nil != err {
		fmt.Println(err)
	}
}

/**
* panic test
 */
func Example4() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	//taskB.TaskDependName("TaskA")
	//taskB.TaskDependName("TaskB")
	//taskD.TaskDependName("taskC")
	//taskE.TaskDependName("taskE")
	/*	taskB.TaskDepend(taskA)
		taskB.TaskDepend(taskC)
		taskD.TaskDepend(taskB)
		taskE.TaskDepend(taskD)*/
	taskExecute.SetTimeout(2000)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecute()
	//fmt.Println(taskA)
	//fmt.Println(taskB)
}

/**
* TaskDependName
 */
func Example5() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("taskA", &TaskA{})
	taskB := taskExecute.CreatTask("taskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDependName("taskA")
	taskD.TaskDependName("taskB")
	taskD.TaskDependName("taskC")
	taskE.TaskDependName("taskD")
	taskExecute.SetTimeout(2000)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecute()
	//fmt.Println(taskA)
	//fmt.Println(taskB)
}

/**
* 网络执行器
* DoExecuteGraph
 */
func Example6() {
	taskExecute := CreateExecute()
	taskExecute.Debug()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	taskExecute.SetTimeout(1200)
	//fmt.Println("hello word")
	//fmt.Println(taskA)
	//fmt.Println(taskB)
	//fmt.Println(taskC)
	//fmt.Println(taskD)
	//fmt.Println(taskE)
	taskExecute.DoExecuteGraph()
	//fmt.Println(taskA)
	//fmt.Println(taskB)
}

/**
* 整体超时
* taskExecute.SetTimeout(600)
 */
func Example7() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	taskB.SetTimeout(20)
	taskB.taskTypeFailNotifyChildCancel() //当任务失败或超时时通知子节点不要执行
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	//taskExecute.SetTimeout(1200)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecuteGraph()
	//fmt.Println(taskA)
	//fmt.Println(taskB)
}

/**
* 整体超时,取消未执行任务
* taskExecute.SetTimeout(600)
 */
func Example8() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	//taskB.SetTimeout(20)
	taskB.taskTypeFailNotifyChildCancel() //当任务失败或超时时通知子节点不要执行
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	taskExecute.SetTimeout(590)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecuteGraph()
	//fmt.Println(taskA)
	//fmt.Println(taskB)

	time.Sleep(time.Duration(3) * time.Second)
}

/**
* 核心作业失败
 */
func Example9() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskFail{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	//taskB.SetTimeout(20)
	taskB.taskTypeFailCoreFail()
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	//taskExecute.SetTimeout(590)
	//fmt.Println("hello word")
	fmt.Println(taskA)
	fmt.Println(taskB)
	fmt.Println(taskC)
	fmt.Println(taskD)
	fmt.Println(taskE)
	taskExecute.DoExecuteGraph()
	//fmt.Println(taskA)
	//fmt.Println(taskB)

	time.Sleep(time.Duration(3) * time.Second)
}

/**
* 打印执行过程,deadlock检测
 */
func Example10() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskFail{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	//deadlock
	taskA.TaskDepend(taskD)
	//taskExecute.SetTimeout(590)
	//fmt.Println("hello word")

	taskExecute.printExecureGraph()
	//fmt.Println(taskA)
	//fmt.Println(taskB)

	time.Sleep(time.Duration(3) * time.Second)
}

/**
* TaskDependGroup
 */
func Example11() {
	taskExecute := CreateExecute()
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskFail{})
	//taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	//taskB.TaskDepend(taskA)
	//taskB.TaskDepend(taskC)
	taskB.TaskDependGroup([]string{"TaskA", "taskC"}...)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	//deadlock
	taskA.TaskDepend(taskD)
	//taskExecute.SetTimeout(590)
	//fmt.Println("hello word")
	//taskExecute.NewPool(1)
	taskExecute.printExecureGraph()
	//fmt.Println(taskA)
	//fmt.Println(taskB)

	time.Sleep(time.Duration(3) * time.Second)
}

/**
* NewPool 支持协程池
 */
func Example12() {
	taskExecute := CreateExecute()
	taskExecute.Debug()
	taskExecute.NewPool(1)
	taskA := taskExecute.CreatTask("TaskA", &TaskA{})
	taskB := taskExecute.CreatTask("TaskB", &TaskB{})
	taskC := taskExecute.CreatTask("taskC", &TaskC{})
	taskD := taskExecute.CreatTask("taskD", &TaskD{})
	taskE := taskExecute.CreatTask("taskE", &TaskE{})
	taskB.TaskDepend(taskA)
	taskB.TaskDepend(taskC)
	taskD.TaskDepend(taskB)
	taskE.TaskDepend(taskD)
	taskExecute.SetTimeout(1200)
	//fmt.Println("hello word")
	//fmt.Println(taskA)
	//fmt.Println(taskB)
	//fmt.Println(taskC)
	//fmt.Println(taskD)
	//fmt.Println(taskE)
	taskExecute.DoExecuteGraph()
}
