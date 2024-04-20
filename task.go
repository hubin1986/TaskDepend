package task

import (
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/panjf2000/ants"
)

const DEFALUT_TASK_TIMEOUT = 1500
const DEFALUT_EXECUTE_TIMEOUT = 1500
const ROOT_NODE = "root"
const FINAL_NODE = "final_node"

// signal
const SIGNAL_TASK_DONE = 1
const SIGNAL_TASK_PANIC = 2
const SIGNAL_TASK_FAIL = 3
const SIGNAL_TASK_FAIL_WAIT_NODE = 4

// ERROR_CODE
const ERROR_NO_NULL_DEPEND = 1
const ERROR_NO_SELF_DEPEND = 2
const ERROR_NO_TASK_ERROR = 3
const ERROR_NO_TASK_PANIC = 4
const ERROR_NO_TASK_TIMEOUT = 5
const ERROR_NO_EXECUTE_TIMEOUT = 6
const ERROR_NO_TASK_FAIL_WAIT_ERROR = 7

// ERROR_MSG
const ERROR_MSG_NULL_DEPEND = "can not depend nil"
const ERROR_MSG_SELF_DEPEND = "can not depend itself"
const ERROR_MSG_TASK_ERROR = "task fail"
const ERROR_MSG_TASK_PANIC = "task panic"
const ERROR_MSG_TASK_TIMEOUT = "task timeout"
const ERROR_MSG_EXECUTE_TIMEOUT = "execute timeout"
const ERROR_MSG_TASK_FAIL_WAIT_ERROR = "task wait node error"

// 任务状态
const TASK_STATUS_INTI = 0
const TASK_STATUS_RUNNING = 1
const TASK_STATUS_SUCCESS = 2
const TASK_STATUS_TIMEOUT = 3
const TASK_STATUS_ERROR = 4
const TASK_STATUS_WAIT_NODE_FAIL = 5 //父节点失败

// task 任务类型
const TASK_TYPE_DEFAULT = 0                  //默认任务类型
const TASK_TYPE_FAIL_NOTIFY_CHILD_CANCEL = 1 //当前节点失败后通知子节点不用做处理了
const TASK_TYPE_FAIL_CORE_FAIL = 2           //核心任务，如果失败认为整体失败

// 最小携程池并行度,理论上是2
const MINI_POOL_SIZE = 2

type Error struct {
	ErrCode int
	ErrMsg  string
}

func CreateError(code int, msg string) *Error {
	return &Error{ErrCode: code, ErrMsg: msg}
}

func (err *Error) Error() string {
	return err.ErrMsg
}

// root task
type RootTask struct {
}

// executor of task
func (this *RootTask) Execute(node *TaskNode) bool {
	node.execute.startTime = time.Now().UnixNano() / 1e6
	return true
}

// result of task
func (this *RootTask) GetResult() interface{} {
	return ""
}

// 最后运行的系统作业
type FinalizeTask struct {
}

// 方法
func (this *FinalizeTask) Execute(node *TaskNode) bool {
	//fmt.Println("")
	node.execute.endTime = time.Now().UnixNano() / 1e6
	node.execute.executeTime = node.execute.endTime - node.execute.startTime
	//所有任务完成
	node.execute.isDoneChan <- 1
	return true
}

// 接口
func (this *FinalizeTask) GetResult() interface{} {
	return ""
}

type Queue interface {
	Offer(e interface{})
	Poll() interface{}
	Watch() interface{}
	Clear() bool
	Size() int
	IsEmpty() bool
}

type LinkedList struct {
	index    int
	elements []interface{}
}

func CreateQueue() *LinkedList {
	return &LinkedList{}
}

func (queue *LinkedList) Offer(e interface{}) {
	queue.elements = append(queue.elements, e)
}

func (queue *LinkedList) Watch() interface{} {
	if queue.IsEmpty() {
		return nil
	}
	//firstElement := queue.elements[0]
	curElement := queue.elements[queue.index]
	return curElement
}

func (queue *LinkedList) Next() interface{} {
	if queue.IsEmpty() {
		return nil
	}
	firstElement := queue.elements[queue.index]
	queue.index = (queue.index + 1) % queue.Size()
	return firstElement
}

func (queue *LinkedList) Poll() interface{} {
	if queue.IsEmpty() {
		//.Println("Poll error : queue is Empty")
		return nil
	}

	if queue.index == 0 {
		firstElement := queue.elements[0]
		queue.elements = queue.elements[1:]
		return firstElement
	}

	curElement := queue.elements[queue.index]
	head := queue.elements[0:queue.index]
	tail := queue.elements[queue.index+1:]
	if queue.index > 0 {
		queue.index = queue.index - 1
	}
	queue.elements = append(head, tail...)

	return curElement
}

func (queue *LinkedList) Size() int {
	return len(queue.elements)
}

func (queue *LinkedList) IsEmpty() bool {
	return len(queue.elements) == 0
}

func (queue *LinkedList) Clear() bool {
	if queue.IsEmpty() {
		//fmt.Println("queue is Empty!")
		return false
	}
	for i := 0; i < queue.Size(); i++ {
		queue.elements[i] = nil
	}
	queue.elements = nil
	return true
}

type Worker interface {
	Execute(node *TaskNode) bool
	GetResult() interface{}
}

// worker
type TaskNode struct {
	name         string
	taskType     int32 //任务类型
	status       int32 //0,任务未执行，1，任务已执行
	notifyNumber int32
	waitNumber   int32
	waitList     []*TaskNode
	notifyList   []*TaskNode
	waitMap      map[string]*TaskNode
	notifyMap    map[string]string

	isInQueue int32

	worker      Worker
	execute     *TaskExecute
	executeTime int64
	startTime   int64
	endTime     int64
	timeOut     int64
	errno       int32
	errmsg      string
	isError     bool
	isTimeOut   bool
	isDone      bool
	isPrintDone bool //print执行时
}

// task executor
type TaskExecute struct {
	root        *TaskNode
	finalNode   *TaskNode
	taskList    []*TaskNode
	taskMap     map[string]*TaskNode
	undoNumber  int32
	queue       *LinkedList
	executeList *LinkedList
	readyChan   chan string
	isDoneChan  chan int

	startTime   int64
	endTime     int64
	executeTime int64
	timeOut     int64
	isError     bool
	isTimeOut   bool
	isDone      bool

	isCancel bool

	logMap     cmap.ConcurrentMap[string, string]
	warnLogMap cmap.ConcurrentMap[string, string]
	hasLog     bool
	hasWarnLog bool

	printExecuteStruct bool //打印执行过程结构
	isDebug            bool //debug 模式

	usePool  bool       //使用协程池
	poolSize int        //协程池数量
	pool     *ants.Pool //协程池
}

/*
*
创建任务执行器
*/
func CreateExecute() *TaskExecute {
	execute := new(TaskExecute)
	execute.taskList = make([]*TaskNode, 0, 10)
	execute.taskMap = make(map[string]*TaskNode, 10)
	execute.root = execute.CreatTask(ROOT_NODE, &RootTask{})
	execute.finalNode = execute.CreatTask(FINAL_NODE, &FinalizeTask{})
	execute.queue = CreateQueue()
	execute.executeList = CreateQueue()
	execute.timeOut = DEFALUT_EXECUTE_TIMEOUT
	execute.isDoneChan = make(chan int, 1)
	execute.isTimeOut = false
	execute.isDone = false
	execute.isCancel = false
	execute.isError = false
	execute.logMap = cmap.New[string]()
	execute.warnLogMap = cmap.New[string]()
	execute.hasLog = false
	execute.hasWarnLog = false
	execute.printExecuteStruct = false
	execute.isDebug = false
	execute.usePool = false
	execute.poolSize = 0
	return execute
}

type taskNodePointer *TaskNode

/**
 *  设置debug
 */
func (execute *TaskExecute) Debug() {
	execute.isDebug = true
}

/**
 *  使用协程池
 */
func (execute *TaskExecute) NewPool(poolSize int) {
	execute.usePool = true

	poolSize = poolSize + MINI_POOL_SIZE

	execute.poolSize = poolSize
	pool, error := ants.NewPool(poolSize) //新建一个pool对象，其他同上
	if nil != error {
		execute.pool = pool
	}
}

/**
 *  释放协程池
 */
func (execute *TaskExecute) ReleasePool() {
	if execute.usePool && nil != execute.pool {
		execute.pool.Release()
	}
}

/**
 *  日志debug
 */
func (execute *TaskExecute) logDebug(format string, a ...interface{}) {
	log := "Debug:" + format
	now := time.Now().UnixNano()
	prefix := strconv.FormatInt(now, 10)
	execute.hasLog = true
	execute.logMap.Set(prefix, fmt.Sprintf(log, a))
	if execute.printExecuteStruct || execute.isDebug {
		fmt.Println(log, a)
	}
}

/**
 *  日志notice
 */
func (execute *TaskExecute) logNotice(format string, a ...interface{}) {
	log := "Notice:" + format
	now := time.Now().UnixNano()
	prefix := strconv.FormatInt(now, 10)
	execute.hasLog = true
	execute.logMap.Set(prefix, fmt.Sprintf(log, a))
	if execute.printExecuteStruct || execute.isDebug {
		fmt.Println(log, a)
	}
}

/**
 *  日志warn
 */
func (execute *TaskExecute) logWarn(format string, a ...interface{}) {
	log := "Warn:" + format
	now := time.Now().UnixNano()
	prefix := strconv.FormatInt(now, 10)
	execute.hasWarnLog = true
	execute.warnLogMap.Set(prefix, fmt.Sprintf(log, a))
	if execute.printExecuteStruct || execute.isDebug {
		fmt.Println(log, a)
	}
}

/**
 *  日志fatal
 */
func (execute *TaskExecute) logFatal(format string, a ...interface{}) {
	log := "Fatal:" + format
	now := time.Now().UnixNano()
	prefix := strconv.FormatInt(now, 10)
	execute.hasWarnLog = true
	execute.warnLogMap.Set(prefix, fmt.Sprintf(log, a))
	if execute.printExecuteStruct || execute.isDebug {
		fmt.Println(log, a)
	}
}

/**
 *  打印日志
 */
func (execute *TaskExecute) PrintLog(logFunc func(log string)) {
	if !execute.hasLog {
		return
	}
	execute.sortLogPrint(execute.logMap, logFunc)
}

/**
 *  打印warn,fatal日志
 */
func (execute *TaskExecute) PrintWarnLog(logFunc func(log string)) {
	if !execute.hasWarnLog {
		return
	}
	execute.sortLogPrint(execute.warnLogMap, logFunc)
}

/**
 *  task 状态数据
 */
func (execute *TaskExecute) StatisticTask(taskFunc func(task *TaskNode)) {
	if execute.taskList != nil && len(execute.taskList) > 0 {
		for _, task := range execute.taskList {
			if nil != task {
				taskFunc(task)
			}
		}
	}
}

/**
 *  日志排序
 */
func (execute *TaskExecute) sortLogPrint(logMap cmap.ConcurrentMap[string, string], logFunc func(log string)) {
	m := make(map[string]string)
	var keys []string

	logMap.IterCb(func(key string, v string) {
		m[key] = v
		keys = append(keys, key)

	})

	sort.Strings(keys)

	prefix := "taskDepend"
	for _, k := range keys {
		logFunc(prefix + m[k])
	}
}

/**
 *  图执行过程
 */
func (execute *TaskExecute) printExecureGraph() {
	execute.printExecuteStruct = true
	execute.DoExecuteGraph()
	if execute.taskList != nil {
		for _, task := range execute.taskList {
			if false == task.isPrintDone {
				execute.logDebug("deadlock:", task.GetTaskName())
			}
		}
	}
}

/**
 *  final添加依赖
 */
func (execute *TaskExecute) finalDepend() {
	if execute.finalNode != nil {
		for _, node := range execute.taskList {
			if node.notifyNumber == 0 {
				execute.finalNode.TaskDepend(node)
			}
		}

		execute.taskList = append(execute.taskList, execute.finalNode)
	}
}

/**
 * 添加依赖的任务数
 */
func (execute *TaskExecute) AddUndoNumber(delta int32) {
	atomic.AddInt32(&execute.undoNumber, delta)
}

/**
 * 超时设置
 */
func (execute *TaskExecute) SetTimeout(timeOut int64) {
	execute.timeOut = timeOut
}

/*
*

	execute是否成功
*/
func (execute *TaskExecute) IsDone() bool {
	return execute.isDone
}

/*
*
创建任务
*/
func (execute *TaskExecute) CreatTask(name string, worker Worker) *TaskNode {
	task := new(TaskNode)
	task.name = name
	task.taskType = TASK_TYPE_DEFAULT
	task.status = TASK_STATUS_INTI
	task.notifyList = make([]*TaskNode, 0, 10)
	task.waitList = make([]*TaskNode, 0, 10)
	task.waitMap = make(map[string]*TaskNode, 10)
	task.notifyMap = make(map[string]string, 10)
	task.worker = worker
	task.execute = execute
	task.errmsg = ""
	task.timeOut = DEFALUT_TASK_TIMEOUT
	task.isTimeOut = false
	task.isDone = false
	task.isError = false
	task.isPrintDone = false
	//先关联tasklist
	if name != ROOT_NODE && name != FINAL_NODE {
		execute.taskList = append(execute.taskList, task)
		execute.taskMap[name] = task
	}
	//fmt.Println("creatTask:",execute.taskList)
	return task
}

/*
*

	启动线性执行器
*/
func (this *TaskExecute) DoExecute() error {
	//finaldepend
	this.finalDepend()
	go this.DoExecuteWrap()
	select {
	case <-this.isDoneChan:
		//fmt.Println("isDone")
		this.logNotice("DoExecute isDone")
		this.isDone = true
	case <-time.After(time.Millisecond * time.Duration(this.timeOut)):
		// timeout  超时
		//fmt.Println("allTimeout")
		this.logWarn("DoExecute allTimeout")
		this.isTimeOut = true
	}

	if this.isTimeOut == true {
		return CreateError(ERROR_NO_EXECUTE_TIMEOUT, ERROR_MSG_EXECUTE_TIMEOUT)
	}

	return nil
}

// 线性执行器
func (this *TaskExecute) DoExecuteWrap() {
	defer func() {
		if err := recover(); nil != err {
			log.Printf("TaskExecute DoExecuteWrap panic, err:%v\n", err)
		}
	}()
	//如果任务没有依赖关联root
	for _, node := range this.taskList {
		if len(node.waitList) == 0 {
			node.TaskDepend(this.root)
		}
	}
	this.queue.Offer(this.root)
	this.logDebug("offer root")
	for !this.queue.IsEmpty() {
		visit := this.queue.Poll().(*TaskNode)
		this.executeList.Offer(visit)
		//不存在并发
		this.undoNumber = this.undoNumber + 1
		this.logDebug("visit:", visit.GetTaskName())
		//this.queue.Poll()

		if visit.notifyNumber > 0 {
			for _, node := range visit.notifyList {
				//fmt.Println(node.GetTaskName())
				if node.isInQueue == 0 {
					this.queue.Offer(node)
					node.isInQueue = 1
				}
			}
		}
	}
	this.readyChan = make(chan string, this.undoNumber)
	isRootRun := 1
	for this.undoNumber > 0 {
		node, ok := this.executeList.Watch().(*TaskNode)
		if !ok {
			break
		}

		skipLength := this.executeList.Size()
		for ; skipLength > 0 && !this.executeList.IsEmpty(); node, ok = this.executeList.Watch().(*TaskNode) {
			if !ok {
				break
			}

			if isRootRun != 1 {
				for {
					select {
					case ready := <-this.readyChan:
						this.logDebug("ready to run: %s\n", ready)
					}
					break
				}
			}

			isRootRun = 0

			if node.waitNumber == 0 {
				go node.Execute()
				this.executeList.Poll()
				//this.executeList.Next()
			} else {
				this.executeList.Next()
				//noop
			}
			skipLength--
		}

	}
}

/*
*

	execute 后执行资源释放
*/
func (this *TaskExecute) finalize() {
	//如果有协程池则释放
	this.ReleasePool()
}

/*
*

	启动图执行器
*/
func (this *TaskExecute) DoExecuteGraph() error {
	//释放资源
	this.finalize()
	//finaldepend
	this.finalDepend()
	this.DoExecuteWrapGraph()

	select {
	case <-this.isDoneChan:
		//fmt.Println("isDone")
		this.logDebug("DoExecuteGraph isDone")
		this.isDone = true
	case <-time.After(time.Millisecond * time.Duration(this.timeOut)):
		// timeout  超时
		//fmt.Println("allTimeout")
		this.logWarn("DoExecuteGraph allTimeout:%d", this.timeOut)
		this.isTimeOut = true
	}

	if this.isTimeOut == true {
		return CreateError(ERROR_NO_EXECUTE_TIMEOUT, ERROR_MSG_EXECUTE_TIMEOUT)
	}

	return nil
}

/*
*
具体逻辑
*/
func (this *TaskExecute) DoExecuteWrapGraph() {
	//如果任务没有依赖关联root
	for _, node := range this.taskList {
		if len(node.waitList) == 0 {
			node.TaskDepend(this.root)
		}
	}
	if this.usePool && nil != this.pool {
		this.pool.Submit(func() {
			this.root.ExecuteGraph()
		})
	} else {
		go this.root.ExecuteGraph()
	}
	//go this.root.ExecuteGraph()
}

/*
*

	executor 执行时间
*/
func (this *TaskExecute) GetExecuteTime() int64 {
	return this.executeTime
}

/*
*

	是否是在协程池中执行
*/
func (this *TaskNode) isRunInPool() bool {
	if this.execute.usePool && nil != this.execute.pool {
		return true
	}
	return false
}

/*
*

	任务是否失败
*/
func (this *TaskNode) IsError() bool {
	return this.isError
}

/*
*

	任务是否超时
*/
func (this *TaskNode) IsTimeOut() bool {
	return this.isTimeOut
}

/*
*

	任务是否成功
*/
func (this *TaskNode) IsDone() bool {
	return this.isDone
}

/*
*

	启动执行器
*/
func (this *TaskNode) Execute() {
	defer func() {
		if err := recover(); nil != err {
			log.Printf("TaskNode Execute panic, err:%v\n", err)
		}
	}()
	if 0 == this.GetWaitNumber() {
		this.execute.AddUndoNumber(-1)
		done := make(chan int, 1)
		go this.ExecuteWrap(done)

		select {
		case taskStatus := <-done:
			if taskStatus == SIGNAL_TASK_DONE {
				//fmt.Println("call task successfully:%s", this.GetTaskName())
				this.execute.logDebug("call task successfully:%s", this.GetTaskName())
				this.status = TASK_STATUS_SUCCESS
				this.isDone = true
			} else if taskStatus == SIGNAL_TASK_PANIC {
				//fmt.Println("call task panic :", this.GetTaskName())
				this.execute.logFatal("call task panic:%s", this.GetTaskName())
				this.errno = ERROR_NO_TASK_PANIC
				this.errmsg = ERROR_MSG_TASK_ERROR
				this.status = TASK_STATUS_ERROR
				this.isError = true
			} else if taskStatus == SIGNAL_TASK_FAIL {
				//fmt.Println("call task fail :", this.GetTaskName())
				this.execute.logWarn("call task fail:%s", this.GetTaskName())
				this.errno = ERROR_NO_TASK_ERROR
				this.errmsg = ERROR_MSG_TASK_ERROR
				this.status = TASK_STATUS_ERROR
				this.isError = true
			}
		case <-time.After(time.Millisecond * time.Duration(this.timeOut)):
			//fmt.Println("timeout:", this.GetTaskName())
			this.execute.logWarn("task timeout:%s", this.GetTaskName())
			this.isTimeOut = true
			this.status = TASK_STATUS_TIMEOUT
			this.errno = ERROR_NO_TASK_TIMEOUT
			this.errmsg = ERROR_MSG_TASK_TIMEOUT
			size := len(this.notifyList)
			for i := 0; i < size; i++ {
				notifyNode := this.notifyList[i]
				//当waitnumer==0即进入可执行状态
				notifyNode.AddWaitNumber(-1)
			}

			//notifyAll
			//fmt.Println("notify by:", this.GetTaskName())
			this.execute.logDebug("notify by:%s", this.GetTaskName())
			this.execute.readyChan <- this.GetTaskName()
		}

	}

}

/*
*

	启动执行器
*/
func (this *TaskNode) ExecuteWrap(done chan int) {
	//fmt.Println("doexe:" + this.GetTaskName())
	this.execute.logDebug("doexe:" + this.GetTaskName())
	var startTime = time.Now()
	this.startTime = time.Now().UnixNano() / 1e6
	defer func() {
		if err := recover(); nil != err {
			this.errmsg = string(debug.Stack())
			//fmt.Println(this.errmsg)
			this.execute.logFatal(this.errmsg + " fask fatal:" + this.errmsg)
			size := len(this.notifyList)
			for i := 0; i < size; i++ {
				notifyNode := this.notifyList[i]
				//当waitnumer==0即进入可执行状态
				notifyNode.AddWaitNumber(-1)
			}
			//notifyAll
			//fmt.Println("notify by:", this.GetTaskName())
			this.execute.logDebug("notify by:" + this.GetTaskName())
			this.execute.readyChan <- this.GetTaskName()
			done <- SIGNAL_TASK_PANIC
		}
		cost := time.Since(startTime).Milliseconds()
		if cost < 1 {
			cost = 0
		}
		this.executeTime = cost
		this.endTime = time.Now().UnixNano() / 1e6
	}()
	if nil != this.worker {
		success := this.worker.Execute(this)
		if !success {
			this.errorNotifyChild(TASK_STATUS_ERROR)
			done <- SIGNAL_TASK_FAIL
		}
	}
	//this.execute.AddUndoNumber(-1)
	size := len(this.notifyList)
	for i := 0; i < size; i++ {
		notifyNode := this.notifyList[i]
		//当waitnumer==0即进入可执行状态
		notifyNode.AddWaitNumber(-1)
	}
	//notifyAll
	//fmt.Println("notify by:", this.GetTaskName())
	this.execute.logDebug("notify by:" + this.GetTaskName())
	this.execute.readyChan <- this.GetTaskName()
	done <- SIGNAL_TASK_DONE

}

/*
*

	启动执行器
*/
func (this *TaskNode) ExecuteGraph() {
	defer func() {
		if err := recover(); nil != err {
			this.errmsg = string(debug.Stack())
			//this.execute.logFatal(this.errmsg + "node ExecuteGraph fatal:"+ this.errmsg)
			this.execute.logFatal(this.GetTaskName() + " out task panic:" + this.errmsg)
		}
	}()
	//等待的任务都完成了
	if 0 == this.GetWaitNumber() ||
		(this.name == FINAL_NODE && (this.execute.isTimeOut == true || this.execute.isCancel)) { //执行器超时的特殊处理逻辑
		//当前任务执行中
		if this.status == TASK_STATUS_INTI {
			this.status = TASK_STATUS_RUNNING
		}
		this.execute.AddUndoNumber(-1)
		done := make(chan int, 1)

		if this.isRunInPool() {
			this.execute.pool.Submit(func() {
				this.ExecuteWrapGraph(done)
			})
		} else {
			go this.ExecuteWrapGraph(done)
		}

		select {
		case taskStatus := <-done:
			if taskStatus == SIGNAL_TASK_DONE {
				//fmt.Println("call task successfully:", this.GetTaskName())
				this.execute.logDebug("call task successfully:", this.GetTaskName())
				this.isDone = true
				this.status = TASK_STATUS_SUCCESS
			} else if taskStatus == SIGNAL_TASK_PANIC {
				//fmt.Println("call task panic :", this.GetTaskName())
				this.execute.logFatal("call task panic :", this.GetTaskName())
				this.errno = ERROR_NO_TASK_PANIC
				this.errmsg = ERROR_MSG_TASK_PANIC
				this.status = TASK_STATUS_ERROR
				this.isError = true
			} else if taskStatus == SIGNAL_TASK_FAIL {
				//fmt.Println("call task fail :", this.GetTaskName())
				this.execute.logWarn("call task fail :", this.GetTaskName())
				this.errno = ERROR_NO_TASK_ERROR
				this.errmsg = ERROR_MSG_TASK_ERROR
				this.status = TASK_STATUS_ERROR
				this.isError = true
			} else if taskStatus == SIGNAL_TASK_FAIL_WAIT_NODE {
				//fmt.Println("call task fail by wait node :", this.GetTaskName())
				this.execute.logWarn("call task fail by wait node :", this.GetTaskName())
				this.errno = ERROR_NO_TASK_FAIL_WAIT_ERROR
				this.errmsg = ERROR_MSG_TASK_FAIL_WAIT_ERROR
				this.status = TASK_STATUS_WAIT_NODE_FAIL
				this.isError = true
			}
			//notifyAll
			//fmt.Println("notify by:", this.GetTaskName())
			this.execute.logDebug("notify by:", this.GetTaskName())
			this.NotifyAll()
		case <-time.After(time.Millisecond * time.Duration(this.timeOut)):
			//fmt.Println("timeout:", this.GetTaskName())
			this.execute.logWarn("timeout:", this.GetTaskName())
			this.isTimeOut = true
			this.errno = ERROR_NO_TASK_TIMEOUT
			this.errmsg = ERROR_MSG_TASK_TIMEOUT
			this.status = TASK_STATUS_TIMEOUT
			this.errorNotifyChild(TASK_STATUS_TIMEOUT)
			this.NotifyAll()

			//notifyAll
			//fmt.Println("notify by:", this.GetTaskName())
			this.execute.logDebug("notify by:", this.GetTaskName())

		}

	}

}

/*
*

	启动执行器
*/
func (this *TaskNode) ExecuteWrapGraph(done chan int) {
	//fmt.Println("doexe:" + this.GetTaskName())
	this.execute.logDebug("doexe:" + this.GetTaskName())
	var startTime = time.Now()
	this.startTime = time.Now().UnixNano() / 1e6
	defer func() {
		if err := recover(); nil != err {
			this.errmsg = string(debug.Stack())
			//.Println(this.errmsg)
			this.execute.logFatal(this.GetTaskName() + " task panic:" + this.errmsg)
			done <- SIGNAL_TASK_PANIC
		}
		cost := time.Since(startTime).Milliseconds()
		if cost < 1 {
			cost = 0
		}
		this.executeTime = cost
		this.endTime = time.Now().UnixNano() / 1e6
	}()

	//依赖的任务已经失败
	if this.status == TASK_STATUS_WAIT_NODE_FAIL && this.name != FINAL_NODE {
		//fmt.Println("failwait:", this.GetTaskName())
		this.execute.logWarn("failwait:", this.GetTaskName())
		this.errorNotifyChild(TASK_STATUS_ERROR)
		done <- SIGNAL_TASK_FAIL_WAIT_NODE
		return
	}

	if nil != this.worker {
		success := false
		if this.execute.printExecuteStruct == true {
			this.isPrintDone = true
			success = true
		} else {
			success = this.worker.Execute(this)
		}

		if !success {
			//fmt.Println("failself:", this.GetTaskName())
			this.execute.logWarn("failself:", this.GetTaskName())
			this.errorNotifyChild(TASK_STATUS_TIMEOUT)
			//this.taskType
			done <- SIGNAL_TASK_FAIL
			return
		}
	}

	done <- SIGNAL_TASK_DONE

}

func (node *TaskNode) NotifyAll() {
	size := len(node.notifyList)
	for i := 0; i < size; i++ {
		notifyNode := node.notifyList[i]
		//当waitnumer==0即进入可执行状态
		notifyNode.AddWaitNumber(-1)

		//如果执行器超时，则停止触发未完成的任务
		if nil != node.execute && (node.execute.isTimeOut == true || node.execute.isCancel == true) {
			//fmt.Println("executer timeout can not notify")
			node.execute.logWarn("executer timeout can not notify")
			//if final node not run，run
			if node.execute.finalNode.status == TASK_STATUS_INTI {
				if node.isRunInPool() {
					node.execute.pool.Submit(func() {
						node.execute.finalNode.ExecuteGraph()
					})
				} else {
					go node.execute.finalNode.ExecuteGraph()
				}

			}
			return
		}

		if node.isTimeOut == true || (node.isError == true && node.taskType == TASK_TYPE_FAIL_CORE_FAIL) {
			//fmt.Println("task timeout or fail can not notify")
			node.execute.logWarn("task timeout or fail can not notify")
			//if final node not run，run
			node.execute.isCancel = true
			if node.execute.finalNode.status == TASK_STATUS_INTI {
				if node.isRunInPool() {
					node.execute.pool.Submit(func() {
						node.execute.finalNode.ExecuteGraph()
					})
				} else {
					go node.execute.finalNode.ExecuteGraph()
				}
			}
			return
		}

		if notifyNode.waitNumber == 0 {
			if node.isRunInPool() {
				node.execute.pool.Submit(func() {
					notifyNode.ExecuteGraph()
				})
			} else {
				go notifyNode.ExecuteGraph()
			}
		}
	}
}

/*
*

	设置节点失败时，通知子节点不执行
*/
func (this *TaskNode) taskTypeFailNotifyChildCancel() {
	this.taskType = TASK_TYPE_FAIL_NOTIFY_CHILD_CANCEL
}

/*
*

	设置节点失败时，整体执行器失败
*/
func (this *TaskNode) taskTypeFailCoreFail() {
	this.taskType = TASK_TYPE_FAIL_CORE_FAIL
}

/*
*

	任务失败或超时，通知子节点
*/
func (this *TaskNode) errorNotifyChild(status int32) {
	this.status = status
	if this.taskType >= TASK_TYPE_FAIL_NOTIFY_CHILD_CANCEL {
		if nil != this.notifyList && len(this.notifyList) > 0 {
			for _, task := range this.notifyList {
				if task.GetTaskName() != FINAL_NODE {
					task.status = TASK_STATUS_WAIT_NODE_FAIL
				}
			}
		}
	}

}

func (node *TaskNode) GetTaskName() string {
	return node.name
}

func (node *TaskNode) HasWaitDepend(delta int32) {
}

/**添加依赖的任务数
 */
func (node *TaskNode) AddWaitNumber(delta int32) {

	atomic.AddInt32(&node.waitNumber, delta)
}

func (node *TaskNode) GetWaitMap() map[string]*TaskNode {
	return node.waitMap
}

func (node *TaskNode) GetExecuteTime() int64 {
	return node.executeTime
}

func (node *TaskNode) GetErrorMsg() string {
	return node.errmsg
}

func (node *TaskNode) GetWaitNode(taskName string) *TaskNode {
	return node.waitMap[taskName]
}

func (node *TaskNode) GetResutlt(taskName string) interface{} {
	task := node.waitMap[taskName]
	if nil != task && nil != task.worker {
		return task.worker.GetResult()
	}

	return nil
}

func (node *TaskNode) SetTimeout(timeOut int64) {
	node.timeOut = timeOut
}

func (node *TaskNode) GetWaitNumber() int32 {
	return node.waitNumber
}

func (node *TaskNode) AddNotifyNumber(delta int32) {
	/*for{
		v:=atomic.LoadInt32(&node.notifyNumber)
		if atomic.CompareAndSwapInt32(&v,node.notifyNumber,(delta+v)){
			break
		}
	}*/
	atomic.AddInt32(&node.notifyNumber, delta)
}

// 依赖多个任务
func (node *TaskNode) TaskDependGroup(tasks ...string) error {
	if nil == tasks {
		return CreateError(ERROR_NO_NULL_DEPEND, ERROR_MSG_NULL_DEPEND)
	}

	var err error
	for _, taskName := range tasks {
		task := node.execute.taskMap[taskName]
		if nil == task {
			return CreateError(ERROR_NO_NULL_DEPEND, ERROR_MSG_NULL_DEPEND)
		}
		err = node.TaskDepend(task)
	}

	return err
}

func (node *TaskNode) TaskDependName(name string) error {
	task := node.execute.taskMap[name]
	if nil == task {
		return CreateError(ERROR_NO_NULL_DEPEND, ERROR_MSG_NULL_DEPEND)
	}

	return node.TaskDepend(task)
}

func (node *TaskNode) TaskDepend(waitTask *TaskNode) error {
	if nil == waitTask {
		return CreateError(ERROR_NO_NULL_DEPEND, ERROR_MSG_NULL_DEPEND)
	}

	if node == waitTask {
		return CreateError(ERROR_NO_SELF_DEPEND, ERROR_MSG_SELF_DEPEND)
	}

	//是否依赖过，
	if _, ok := node.waitMap[waitTask.name]; !ok {
		node.waitMap[waitTask.name] = waitTask
		node.waitList = append(node.waitList, waitTask)
		node.AddWaitNumber(1)
		//只有被等待的节点>default时，才赋值到子节点，避免子节点设置的失败级别失效
		if waitTask.taskType >= TASK_TYPE_FAIL_NOTIFY_CHILD_CANCEL {
			node.taskType = waitTask.taskType //task 类型具有传导性，当父节点失败时逐级通知子节点不要执行
		}
	}

	//是否被依赖过，
	if _, ok := waitTask.notifyMap[node.name]; !ok {
		waitTask.notifyMap[node.name] = node.name
		waitTask.notifyList = append(waitTask.notifyList, node)
		waitTask.AddNotifyNumber(1)
	}

	return nil

}
