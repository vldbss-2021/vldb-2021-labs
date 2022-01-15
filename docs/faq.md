# FAQ

## 项目整体

### 我不知道如何开始

本实验有 4 个 lab，每个 lab 都有对应的文档，其中 lab1 和 lab2 主要关注的是 tinykv 部分，lab3 和 lab4 主要关注的是 tinysql 部分。

每个实验对应的文档可以在对应文件夹的 `doc_ss` 中找到。

按照对应文档的指示，阅读部分代码，并在现有框架的基础上在代码内的相应位置填入相应的代码，最终通过测试，即可完成实验。

完成实验后，将代码上传到 GitHub 上，系统会自动评分。

### 本地运行测试报错 fatal error: unexpected signal during runtime execution

参考 [这个 issue](https://github.com/vldbss-2021/vldb-2021-labs/issues/18)，这是 Go 1.17 标准库的 bug，将 Go 降级到 1.16 即可解决问题。

### GitHub 评分失败，报错 build cache is required

本实验的自动评分需要使用我们自定义classroom.yml，可以在项目的根文件夹中执行：

```sh
cp scripts/classroom.yml .github/workflows/classroom.yml
```

来覆盖 classroom 默认生成的 `classroom.yml` 文件，然后重新 push 即可。

## lab1

### 在哪里可以查到 badger 的文档？

可以参考 [这里的文档](https://pkg.go.dev/github.com/Connor1996/badger)。

### 我需要完全理解 Raft 吗？

从完成实验的角度来说并不需要，鉴于本次课程的重点是分布式事务而非共识算法，对 Raft 只需有基本的了解，知道 Raft 部分在系统中的作用以及它如何和其他部分交互即可。

### TestBasic2BLab1P1a 卡死

建议检查一下 `HandleRaftReady` 的返回值是如何被使用的。

## Lab 2

## Lab 3

### key not exist 错误

建议检查一下各个阶段使用的 key 和 primary key 和相关结构 （如 mutations）是否正确。

## Lab 4

### 运行测试超时，发现 server 在 bootstrap 之后进行 select 查询卡死

你会在 `projection.go` 中发现一些用于处理 `projectionInput` 和 `projectionOutput` channel 和的帮助函数，想想该如何使用它们。
