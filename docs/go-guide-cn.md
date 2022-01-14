# Go Guide

这篇文档主要讲解 Go 语言的基本使用方法和技巧，适合刚接触 Go 语言的读者。

- 基础概念
    - [Interface](#interface)
    - [Combine](#combine)
    - [Type Assert](#type-assert)
    - [Closure](#closure)
- 数据结构
    - [Map](#map)
    - [Set](#set)
    - [Array/Slice](#slice)
- 并发
    - [Goroutine](#goroutine)
    - [Channel](#channel)
    - [Atomic](#atomic)
    - [Mutex](#mutex)
    - [WaitGroup](#waitgroup)
    - [Cond](#cond)
- Debug
	- [Stack](#stack)
	- [Delve](#dlv)
- 环境变量
	- [GO111MODULE](#GO111MODULE)
	- [GOPATH](#GOPATH)
	- [GOROOT](#GOROOT)
	- [GOGC](#GOGC)
	- [GOPROXY](#GOPROXY)


## 基础概念

Go 的类型系统比较简单，同时也比较动态，这里我们讲解一些常见的用法，类型转换，不研究 reflect。

### Interface

Go 使用 interface 实现多态，interface 定义了实现接口所需要的方法，如果一个类型拥有一个 interface 所定义的所有方法，就可以被当作这个类型进行使用。

```go
package main

import "fmt"

type I interface {
	cost() int
}

type A int
type B string
type C struct {
	price int
	count int
}

func (A) cost() int {
	return 1
}

func (b B) cost() int {
	return len(b)
}

func (c *C) cost() int {
	return c.price + c.count
}

func printlnCost(i I) {
	fmt.Println("cost is", i.cost())
}

func main() {
	printlnCost(A(1)) // 1
	b := B("hello")
	printlnCost(b) // 5
	c := C{
		price: 10,
		count: 5,
	}
	printlnCost(&c) // 50
}
```

### Combine

Go 支持组合 struct 和 interface。


```go
package main

import "fmt"

type IA interface {
	costA() int
}

type I interface {
	IA
	costB() int
}

type A struct {
	price int
}

type B struct {
	price int
}

type C struct {
	A
	B
	count int
}

func (c *C) costA() int {
	return c.count * c.A.price
}

func (c *C) costB() int {
	return c.count * c.B.price
}

func printlnCost(i I) {
	fmt.Println("cost:", i.costA()) // 50
	fmt.Println("cost:", i.costB()) // 0
}

func main() {
	c := C{
		A:     A{price: 10},
		count: 5,
	}
	printlnCost(&c)
}
```

### Type Assert

Go 的类型是动态的，支持 type assert。

```go
package main

type I interface {
	cost() int
}

type A struct {
	c int
}

type B struct {
	price int
	count int
}

func (a *A) cost() int {
	return a.c
}

func (b *B) cost() int {
	return b.price * b.count
}

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func ACost(i I) {
	a := i.(*A)
	assertEq(i.cost(), a.c)
}

func BCost(i I) {
	b := i.(*B)
	assertEq(i.cost(), b.price*b.count)
}

func ICost1(i I) {
	if a, ok := i.(*A); ok {
		assertEq(i.cost(), a.c)
		return
	}
	if b, ok := i.(*B); ok {
		assertEq(i.cost(), b.price*b.count)
		return
	}
	panic("unreachable")
}

func ICost2(i I) {
	switch v := i.(type) {
	case *A:
		assertEq(i.cost(), v.c)
	case *B:
		assertEq(i.cost(), v.price*v.count)
	default:
		panic("unreachable")
	}
}

func main() {
	a := A{100}
	b := B{100, 10}
	ACost(&a)
	BCost(&b)
	ICost1(&a)
	ICost1(&b)
	ICost2(&a)
	ICost2(&b)
}
```

### Closure

闭包函数可以以引用方式捕获当前作用域的变量。


```go
package main

func process(input int, fn func(int) int) int {
	return fn(input)
}

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func main() {
	a := 2
	b := 3
	assertEq(process(a, func(x int) int { return x + b }), 5)
	assertEq(process(a, func(x int) int { return x * b }), 6)
}
```

## 数据结构

Go 没有泛型，所以用户无法编写自己的容器，Go 原生的容器需要编译器打洞。

### Map

map 数据结构实现了单线程的 hash map。

```go
package main

func assert(t bool) {
	if !t {
		panic("not true")
	}
}

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func main() {
	// init empty map
	m := make(map[string]int)
	m["Answer"] = 42
	v, ok := m["Answer"]
	assert(ok)
	assertEq(v, 42)
	// init with values
	m = map[string]int{
		"LoveJustice": 6,
	}
	v, ok = m["LoveJustice"]
	assert(ok)
	assertEq(v, 6)
}
```

### Set

Go 不支持原生的 set，需要通过 map 进行实现。

```go
package main

func assert(t bool) {
	if !t {
		panic("not true")
	}
}

func main() {
	s := make(map[string]struct{})
	s["a"] = struct{}{}
	_, ok := s["a"]
	_, notOk := s["b"]
	assert(ok)
	assert(!notOk)
}
```

### Slice

Go 一般使用变长的 Slice（类似 C++ 的 Vector），使用方法比较固定。

```go
package main

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func main() {
	// allocate a slice of with len 0 and cap 16
	a := make([]int, 0, 16)
	a = append(a, 4)
	a = append(a, 5, 6)
	b := append([]int{0, 1, 2, 3}, a...)
	assertEq(len(b), 7)
	for i, v := range b {
		assertEq(v, i)
	}
}
```

## 并发

Go 语言的 runtime 使用 Green Threads 模型，每个 Goroutine 内部是同步的。

### Goroutine

Goroutine 是 Go 使用并发的基本方法，通过 `go` 关键字调用函数会让这个函数在独立的 Goroutine 中运行，不阻塞当前 Goroutine。

### Channel

Channel 是 Goroutine 之间通信的主要方法。我们常常用以下方法做一个循环处理任务的逻辑。

```go
package main

type worker struct {
	closeCh chan struct{}
	taskCh  chan int
}

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func (w *worker) run() {
	i := 0
	for {
		select {
		case task := <-w.taskCh:
			assertEq(task, i)
			i++
		case <-w.closeCh:
			return
		}
	}
}

func main() {
	closeCh, taskCh := make(chan struct{}), make(chan int)
	worker := &worker{
		closeCh: closeCh,
		taskCh:  taskCh,
	}
	go worker.run()
	for i := 0; i < 1000; i++ {
		taskCh <- i
	}
	close(closeCh)
}
```

### Atomic

Go 的 Atomic 不支持如 C++ 般复杂的 [memory order](https://en.cppreference.com/w/cpp/atomic/memory_order)，所有的 Atomic 操作都是 `seq_cst` 的。

```go
package main

import "sync/atomic"

func assertEq(a, b uint64) {
	if a != b {
		panic("a != b")
	}
}

func litmus() {
	var a, b uint64
	resCh := make(chan uint64)
	go func() {
		atomic.AddUint64(&a, 1)
		r1 := b
		assertEq(r1, atomic.LoadUint64(&b))
		resCh <- r1
	}()
	go func() {
		atomic.AddUint64(&b, 1)
		r2 := a
		assertEq(r2, atomic.LoadUint64(&a))
		resCh <- r2
	}()
	r1 := <-resCh
	r2 := <-resCh
	if r1 == 0 && r2 == 0 {
		panic("r1 and r2 both 0")
	}
}

func main() {
	litmus()
}
```

### Mutex

Mutex 是 Go 中最常用的提供临界区的方法。

```go
package main

import "sync"

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func main() {
	var mu sync.Mutex
	a := 0
	go func() {
		mu.Lock()
		defer mu.Unlock()
		a = 10
		assertEq(a, 10)
	}()

	mu.Lock()
	a = 20
	assertEq(a, 20)
	mu.Unlock()
}

```

### RWMutex

Go 也提供了读写互斥量，读锁共享，写锁排他。

```go
package main

import "sync"

func assert(t bool) {
	if !t {
		panic("not true")
	}
}

func assertEq(a, b int) {
	if a != b {
		panic("a != b")
	}
}

func main() {
	closeCh := make(chan struct{})
	var mu sync.RWMutex
	m := make(map[int]int)
	m[0] = 0
	for i := 0; i < 100; i++ {
		go func() {
			for {
				select {
				case <-closeCh:
					return
				default:
				}
				mu.RLock()
				index := len(m) - 1
				v, ok := m[index]
				assert(ok)
				assertEq(v, index*10)
				mu.RUnlock()
			}
		}()
	}

	for i := 0; i < 1000; i++ {
		mu.Lock()
		m[i] = i * 10
		mu.Unlock()
	}
	close(closeCh)
}
```

### WaitGroup

WaitGroup 用于控制一系列并行任务，在他们全部运行结束时被唤醒。注意例子中，如果希望在 goroutine 中捕获外部的变量，则需要确保它不能被改变，如果他可能被改变（如例子中的循环变量 `i`），我们可以通过函数传参的方式复制它。

```go
package main

import (
	"sync"
	"time"
)

func assert(t bool) {
	if !t {
		panic("not true")
	}
}

func main() {
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			time.Sleep(time.Duration(i+1) * time.Millisecond)
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert(time.Since(start) >= time.Duration(100)*time.Millisecond)
}
```

### Cond

Cond 可以为 Mutex 添加一个条件，通过 `Cond.Signal` 释放一个被 Cond 阻塞的协程，通过 `Cond.Broadcast` 释放所有被阻塞的协程。

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	for i := 0; i < 10; i++ {
		go func(i int) {
			fmt.Println("before cond", i)
			cond.L.Lock()
			cond.Wait()
			fmt.Println("after cond", i)
			cond.L.Unlock()
		}(i)
	}
	time.Sleep(time.Second)
	fmt.Println("signal")
	cond.Signal()
	time.Sleep(time.Second)
	fmt.Println("broadcast")
	cond.Broadcast()
	time.Sleep(time.Second)
}
```

## Debug

在遇到问题时，我们需要对程序进行 Debug，本节将讲一些 Debug 的技巧。

### Stack

在 Debug 时，我们希望看到程序的 Stack。

```go
package main

import (
	"os"
	"runtime/debug"
	"runtime/pprof"
)

func main() {
	// current thread
	debug.PrintStack()
	// all threads
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	// panic also prints stack trace
	panic("print stack")
}
```

如果程序 hang 住了，并且不确定当前进程 hang 在了哪里，可以使用一个单独的 Goroutine 去查看所有 Goroutine 的 Stack。

```go
package main

import (
	"os"
	"runtime/pprof"
	"time"
)

func main() {
	go func() {
		time.Sleep(10 * time.Second)
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		panic("exit")
	}()

	ch := make(chan struct{})
	go func() {
		// process hangs here
		time.Sleep(1000 * time.Second)
		ch <- struct{}{}
	}()
	<-ch
}
```

运行上述程序会得到如下结果，注意有 Goroutine 卡在了 `time.Sleep` 函数上，在 `main.go:19` 被调用。

```sh
/tmp » go run main.go
goroutine profile: total 3
1 @ 0x434d36 0x4055ec 0x405058 0x4ad4e7 0x434967 0x45e2e1
#       0x4ad4e6        main.main+0x86          /tmp/main.go:22
#       0x434966        runtime.main+0x226      /usr/lib/go/src/runtime/proc.go:255

1 @ 0x434d36 0x45b38e 0x4ad52c 0x45e2e1
#       0x45b38d        time.Sleep+0x12d        /usr/lib/go/src/runtime/time.go:193
#       0x4ad52b        main.main.func2+0x2b    /tmp/main.go:19

1 @ 0x459b25 0x4a3ff5 0x4a3e0d 0x4a0f8b 0x4ad5ae 0x45e2e1
#       0x459b24        runtime/pprof.runtime_goroutineProfileWithLabels+0x24   /usr/lib/go/src/runtime/mprof.go:746
#       0x4a3ff4        runtime/pprof.writeRuntimeProfile+0xb4                  /usr/lib/go/src/runtime/pprof/pprof.go:724
#       0x4a3e0c        runtime/pprof.writeGoroutine+0x4c                       /usr/lib/go/src/runtime/pprof/pprof.go:684
#       0x4a0f8a        runtime/pprof.(*Profile).WriteTo+0x14a                  /usr/lib/go/src/runtime/pprof/pprof.go:331
#       0x4ad5ad        main.main.func1+0x4d                                    /tmp/main.go:12

panic: exit

goroutine 6 [running]:
main.main.func1()
        /tmp/main.go:13 +0x65
created by main.main
        /tmp/main.go:10 +0x26
exit status 2
```

### Delve

Delve 是 Go 语言的 Debug 工具，这里会讲述简单的用法。

安装。

```sh
go install github.com/go-delve/delve/cmd/dlv@latest
echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope
```

以 TinyKV 的 raftstore 测试 hang 住为例，首先需要编译当前版本代码的测试 binary，执行下列命令应当在当前目录出现 `test_raftstore.test` 文件。

```sh
cd tinykv/kv/test_raftstore
go test -c
```

执行测试，然后发现他会卡住。

```sh
» make lab1P1a
...
```

查看 Makefile 可以发现，实际上，`make lab1P1a` 等价于：

```
» go test -v ./kv/test_raftstore -run TestBasic2BLab1P1a
```

使用 Delve 进行测试，我们首先使用 `continue` 命令让测试开始，发现这个测试还是会卡住，于是按下 `ctrl + c` 让进程停止，但是保留现场，接着使用 `grs -with user` 查看所有用户空间下（与 runtime 相对）的 goroutines，发现 Goroutine 268 可能有些问题，接着使用 `gr 268` 切换到这个 goroutine 之上，最后使用 `bt` 打出这个 goroutine 的 stack，我们发现他正运行到 `./kv/test_raftstore/test_test.go:210`，而在我的代码中，这里存在一个死循环。

```sh
» dlv test ./kv/test_raftstore -- -test.run TestBasic2BLab1P1a
Type 'help' for list of commands.
(dlv) continue
...
// send ctrl + c to pause it
(dlv) grs -with user
...
  Goroutine 268 - User: ./kv/test_raftstore/test_test.go:210 github.com/pingcap-incubator/tinykv/kv/test_raftstore.GenericTest.func1 (0xcaf9f7) (thread 40165)
...
(dlv) gr 268
Switched from 0 to 268 (thread 40165)
(dlv) bt
0  0x0000000000caf9f7 in github.com/pingcap-incubator/tinykv/kv/test_raftstore.GenericTest.func1
   at ./kv/test_raftstore/test_test.go:210
1  0x0000000000cabe9a in github.com/pingcap-incubator/tinykv/kv/test_raftstore.runClient.func2
   at ./kv/test_raftstore/test_test.go:28
2  0x000000000057541b in testing.tRunner
   at /usr/lib/go/src/testing/testing.go:1259
3  0x00000000005769d9 in testing.(*T).Run·dwrap·21
   at /usr/lib/go/src/testing/testing.go:1306
4  0x0000000000478541 in runtime.goexit
   at /usr/lib/go/src/runtime/asm_amd64.s:1581
```

## 环境变量

所有的环境变量都可以通过 `go env` 命令查看。

### GO111MODULE

`GO111MODULE` 是 Go 1.11 版本新增的环境变量，用于控制是否使用 go mod 进行依赖管理。一般设置为 `GO111MODULE=on`，此时我们不需要在 `GOPATH` 下进行工作。

### GOPATH

`GOPATH` 是 Go 的工作空间，在 `GO111MODULE` 出现之前所有的 Go 项目必须在 `GOPATH` 之下。

### GOROOT

`GOROOT` 是 Go SDK 的路径，必须设置。

### GOGC

`GOGC` 是控制 Go runtime 触发 GC 的环境变量，默认为 100，值越小，GC 越容易触发，当发现内存不够用时，可以调小 `GOGC` 环境变量。

```sh
export GOGC=20
```

### GOPROXY

## 参考资料

[Go for C programmers](https://talks.golang.org/2012/goforc.slide)
