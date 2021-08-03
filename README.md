# VLDB Summer School 2021 Labs

## Introduction

This is the labs of VLDB Summer School 2021. The main target of is implementation of the distributed transaction.

There are several modules in a distributed database.

- TinyKV, the storage engine of the system.
- TinyScheduler, it is used to manager and schedule TinyKV cluster.
- TinySQL, the SQL layer of TinyKV engine.

## Labs

There are 4 labs in this course.

- [Lab 1](./tinykv/doc_ss/lab1.md), implement the storage and log layer in TinyKV.
- [Lab 2](./tinykv/doc_ss/lab2.md), implement the transaction layer in TinyKV.
- [Lab 3](./tinysql/doc_ss/lab3-README-zh_CN.md), implement the Percolator protocol.
- Lab 4, implement the SQL execution layer.
    - [Lab 4-A](./tinysql/doc_ss/lab4a-README-zh_CN.md), implement SQL protocol.
    - [Lab 4-B](./tinysql/doc_ss/lab4b-README-zh_CN.md), implement update executor.
    - [Lab 4-C](./tinysql/doc_ss/lab4c-README-zh_CN.md), implement select and projection executor.

The code is separated into 2 parts, TinyKV and TinyScheduler is in [tinykv](./tinykv), and TinySQL is in [tinysql](./tinysql). 

You need to follow the order in the [labs](#labs) chapter. You may learn more from the README files in [TinyKV](./tinykv/README.md) and [TinySQL](./tinysql/README.md).

There are some TinyKV course materials, which is not included in this course but may help you understand the codebase.

- [StandaloneKV](./tinykv/doc/project1-StandaloneKV.md)
- [RaftKV](./tinykv/doc/project2-RaftKV.md)
- [MultiRaftKV](./tinykv/doc/project3-MultiRaftKV.md)
- [Transaction](./tinykv/doc/project4-Transaction.md)

