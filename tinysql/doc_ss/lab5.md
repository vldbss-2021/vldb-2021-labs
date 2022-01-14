# LAB 5 The Transaction Mode And Conflict Scenario Performance

## The Design

In the previous labs, we've implemented a complete distributed transaction database system. The transaction working flow is like this:

![working_flow](imgs/working_flow.PNG)

After the `BEGIN` statement, a timestamp named `start_ts` is fetched from `tinyscheduler` server. The `start_ts` is used as the identifier for this transaction, and all the reads
in this transaction will use the snapshot data of `start_ts`. This isolation level is defined as the `snapshot isolation` or `repeatable read`. During the execution of the transaction, all the  writes by each SQL statements are stored in the transaction memory buffer in `tinysql` server, they will be written into the log engine in the 2PC process. If the transaction is
rolled back before the 2PC processing, nothing will happen in the log engine and there is no RPC requests. These transactions which does write logs only in 2PC process are called
`optimistic transactions`. The advantage for this mode is obvious that there is no RPC costs during the SQL execution phase, until the `COMMIT` statement is executed. Also the disadvantages are obvious, there will be much retry cost if the concurrent transactions are more likely to conflict with others, as the conflicts
will only be detected until the 2PC stage, the whole transaction will be aborted and cleaned up if `write conflict error` is detected. So the performance will be bad for such 
schenarios and a lot of transaction retry will use much resources. Besides the performance problem, some common workloads are not compatbile with the `optimistic transaction`
mode. For example the `TPC-C` benchmark test workload, the transactions will often use the `select for update` statement to lock specific rows during the transaction execution. But in the `optimistic mode`, there is no `select for update LOCK` support for it and rows could not be locked immediately.

To optimize the performance for the conflict scenarios and make the database compatible with `TPC-C` like workloads, it's needed to introduce another transaction mode which is 
called the `pessimistic mode`.

In the `pessimistic mode`, the rows or keys will need to be locked durtin the `statement execution phase` but not in the `2PC commit phase`. Then the rows which are locked by `select for update` or `write DMLs` will be learned by other transctions, the blocked transcations will then stop and wait for the release of these row locks. To achive this we need to introducte the `pessimsitic lock` phase during the execution of a SQL statement, then the working flow is like this:

![working_flow_pess](imgs/working_flow_pess.PNG)

The 2PC commit phase is almost the same as the `optimistic mode`, while an extra `pessimistic lock` execution phase is added during SQL executions. For example if a `row1` is to
be inserted into the database, after the `INSERT` statement execution a `pessimistic lock` will be put on the `row1_rowkey`, so that other transactions who's trying to `pessimistic lock` the `row1_rowkey` will be waiting.

## Introduce The `Pessimistic Lock`

In the optimistic mode, the keys will be locked by prewrite requests, these locks could be called `prewrite lock`. If there is a `prewrite lock` on a specific key, this transaction is in the `2PC` phase or `COMMIT` phase. The read requests encountered the `prewrite lock` could not decide the actual commit of this key and its `commit_ts`, so
generally the read requests will have to wait for the `prewrite lock`. However the `pessimistic lock` will not block read requests as the related transaction **MUST NOT** be in
the `2PC` phase and the `COMMIT` statement is not triggered, so it's safe for the read requests to ignore the pessimistic lock and read the correspond value on this key. This is 
the most important thing for the `pessimistic lock`.

As the `pessimistic lock` should be seen by all the other transactions, it should be persisted into the log and storage engine. So here comes a new transaction interface and its
 related message types:

```
message PessimisticLockRequest {
    Context context = 1;
    // In this case the Op of the mutation must be Lock.
    repeated Mutation mutations = 2;
    bytes primary_lock = 3;
    uint64 start_version = 4;
    uint64 lock_ttl = 5;
    uint64 for_update_ts = 6;
    // If the request is the first lock request, we don't need to detect deadlock.
    bool is_first_lock = 7;
    // Time to wait for lock released in milliseconds when encountering locks.uint64 wait_timeout = 8;
    // 0 means using default timeout in TiKV. Negative means no wait.
    int64 wait_timeout = 8;
}

message PessimisticLockResponse {
    errorpb.Error region_error = 1;
    repeated KeyError errors = 2;
}

message PessimisticRollbackRequest {
    Context context = 1;
    uint64 start_version = 2;
    uint64 for_update_ts = 3;
    repeated bytes keys = 4;
}

message PessimisticRollbackResponse {
    errorpb.Error region_error = 1;
    repeated KeyError errors = 2;
}
```

After the execution of the `DML write statemtns` or `select for update` statements, the correspond locks will be put into the `tinykv` servers, this gurantees that there will be **no write conflicts** in the prewrite phase as the transaction already owns the lock. The only thing need to do in the prewrite phase is to change the `pessimistic lock` into 
a `prewrite lock` and then continues the 2PC process, and conflict transactions will not need to retry the whole transaction, instead it will retry the conflict SQ statements.
That's to say, **the `pessimistic mode` changes the retry from transaction level to statement level**.

## Statement Retry And Lock Wait

Different statements from different transactions could conflict with each other, so the statment retry and lock wait processing are needed. In this [document](https://docs.pingcap.com/zh/tidb/stable/troubleshoot-lock-conflicts#%E6%82%B2%E8%A7%82%E9%94%81), there's the introductions for the retry mechanism and the deadlock processing mechanism.

An important thing to note for the pessimistic retry is that, the original `start_ts` could not be used to read, and a new `for_update_ts` should be fetched from the scheduler to read the latest change which causes the `write conflict error`. Every time a pessimistic retry happens, a new timestamp will be allocated for the current statement. Also the
if the current execution is blocked by a `pessimistic lock`, lock wait happens and some backoff stratigies will be used to avoid too much lock retry. If the expected lock wait timeout is exceeded, an timeout error could be reported to the clients just like MySQL does.

As the `pessimistic lock` could also be left after failover, it's needed to make the `resolve` and `checkTxnStatus` handle the left locks too. What's different is that rollback a `pessimistic lock` is different from the `prewrite lock` that the finally rollback record may not be needed. If a pessimistic lock is pessimistically rolled back, the related transaction may not fail. This is quite different from the prewrite lock rollback. 

## More Details

Related resources for pessimistic mode transactions:
- [pessimistic transaction in TiDB](https://pingcap.com/blog-cn/pessimistic-transaction-the-new-features-of-tidb)
- [pessimistic transaction in V4.0 TiDB](https://pingcap.com/blog-cn/tidb-4.0-pessimistic-lock)
- [pessimistic transaction document](https://docs.pingcap.com/zh/tidb/stable/pessimistic-transaction)
