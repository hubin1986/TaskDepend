package task

import (
	"testing"
)

func TestCreateExecute(t *testing.T) {
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
	taskExecute.DoExecute()

}
