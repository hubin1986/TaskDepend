package task_test

import (
	task "TaskDepend"
	"fmt"
	"os"
	"testing"
	"time"
)

type TaskA struct {
}

func (p *TaskA) Execute(node *task.TaskNode) bool {
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
func (p *TaskB) Execute(node *task.TaskNode) bool {
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
func (p *TaskFail) Execute(node *task.TaskNode) bool {
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
func (p *TaskC) Execute(node *task.TaskNode) bool {
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
func (p *TaskD) Execute(node *task.TaskNode) bool {
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
func (p *TaskE) Execute(node *task.TaskNode) bool {
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

func TestCreateExecute(t *testing.T) {
	taskExecute := task.CreateExecute()
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
	taskExecute.DoExecute()

}
