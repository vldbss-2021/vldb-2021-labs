# VLDB Summer School 2021 Labs

## Introduction

This is the labs of VLDB Summer School 2021. The target is to build a distributed database.

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

## Autograding

The details of classroom usage can be found in the [classroom doc](./docs/classroom).

Autograding is a workflow which can automatically run test cases. However there are some limitations in Github classroom, in order to make golang works and run it in our self-hosted machines, **you need to overwrite the workflow generated by Github classroom and commit it**.

```sh
cp scripts/classroom.yml .github/workflows/classroom.yml
```

### Getting started

First, please clone the repository with git to get the source code of the project.

``` bash
git clone https://github.com/vldbss-2021/vldb-2021-labs-{{username}}.git
```

Then make sure you have installed [go](https://golang.org/doc/install) >= 1.13 toolchains. You should also have installed `make`.
Now you can run `make` under `tinykv` or `tinysql` dir to check that everything is working as expected. You should see it runs successfully.

## Deploy a cluster

Rather than a course, you can try TinyKV by deploying a real cluster, and interact with it through TinySQL.

### Build

```
cd tinykv
make kv
```

It builds the binary of `tinykv-server` and `tinyscheduler-server` to `bin` dir.

```
cd tinysql
make server
```
It buillds the binary of `tinysql-server` to `bin` dir.


### Deploy By Hand

Put the binary of `tinyscheduler-server`, `tinykv-server` and `tinysql-server` into a single dir.
Under the binary dir, run the following commands:

```
mkdir -p data
```

```
./tinyscheduler-server
```

```
./tinykv-server -path=data
```

```
./tinysql-server --store=tikv --path="127.0.0.1:2379"
```

### Deploy Use Cluster Command

Deploy the cluster in the local environment
```
# compile the cluster binary
cd tinykv
make deploy-cluster

# deploy the cluster, by default the number of scheduler server is 1 and the number of kv server is 3.
./bin/cluster deploy

# start the deployed cluster
./bin/cluster start

# stop the deployed cluster
./bin/cluster stop

# update the binary, please stop the cluster then do the upgrade
./bin/cluster upgrade

# unsafe destroy the whole cluster
./bin/cluster destroy

```

Note this does not deploy a `tinysql` server, to deploy a `tinysql` server, use
```
./tinysql-server --store=tikv --path="127.0.0.1:2379"
```

### Play

```
mysql -u root -h 127.0.0.1 -P 4000
```
