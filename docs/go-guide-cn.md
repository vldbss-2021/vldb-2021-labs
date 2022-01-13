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
    - [Monitor](#monitor)
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

闭包函数可以捕获当前作用域的变量。


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

Go 没有范型，所以用户无法编写自己的容器，Go 原生的容器需要编译器打洞。

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

func litmus() {
	var a, b uint64
	resCh := make(chan uint64)
	go func() {
		atomic.AddUint64(&a, 1)
		r1 := b
		resCh <- r1

	}()
	go func() {
		atomic.AddUint64(&b, 1)
		r2 := a
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

### WaitGroup

### Monitor

## Debug

### Stack

### Delve

## 环境变量

### GO111MODULE

### GOPATH

### GOROOT

### GOGC

### GOPROXY
