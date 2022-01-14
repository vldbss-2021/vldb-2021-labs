# TinySQL

TinySQL is a course designed to teach you how to implement a distributed relational database in Go. TinySQL is also the name of the simplifed version of [TiDB](https://github.com/pingcap/tidb).

## Prerequisites

Experience with Go is required. If not, it is recommended to learn [A Tour of Go](https://tour.golang.org/) first.

## Course Overview

The detailed information you can get in the directory `courses`.

This course will take you from idea to implementation, with the essential topics of distributed relational database covered. 

The course is organized into three parts:

1. Implement the distributed transaction protocol based on [Percolator](https://research.google/pubs/pub36726/).

2. Explains the life of a SQL and transaction, in which there are 3 sub tasks.
    - Implement the general part of SQL execution phases.
    - Implement update statement executor.
    - Implement select and projection executor.

3. There is a further course, implement pessimistic transaction. Pessimistic transaction eliminate contention collapse.

## License

TinySQL is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
