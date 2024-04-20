# task depend
A task execution framework based on Directed Acyclic Graph (DAG), which automatically handles complex 
task relationships through a simple API and provides comprehensive error handling, timeout handling, 
and coroutine pool processing mechanisms

## usage

Import the package:

```go
import (
	"github.com/hubin1986/TaskDepend/v2"
)

```

```bash
go get "github.com/hubin1986/TaskDepend/v2"
```

The package is now imported under the "task" namespace.


# example   
 B depend A  
 B depend C  
 D depend B  
 E depend D   
    
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
fmt.Println("isDone TaskD:", taskD.IsDone())  
fmt.Println("isDone TaskE:", taskE.IsDone())  

