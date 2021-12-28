// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InsertExec represents an insert executor.
type InsertExec struct {
	*InsertValues

	Priority mysql.PriorityEnum
}

func (e *InsertExec) exec(ctx context.Context, rows [][]types.Datum) error {
	sessVars := e.ctx.GetSessionVars()
	defer sessVars.CleanBuffers()
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return err
	}
	sessVars.GetWriteStmtBufs().BufStore = kv.NewBufferStore(txn, kv.TempTxnMemBufCap)
	sessVars.StmtCtx.AddRecordRows(uint64(len(rows)))
	for _, row := range rows {
		logutil.BgLogger().Debug("row", zap.Int("col", len(row)))
		var err error
		// Hint: step II.4
		// YOUR CODE HERE (lab4)
		panic("YOUR CODE HERE")
		if err != nil {
			return err
		}
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *InsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	var err error
	if len(e.children) > 0 && e.children[0] != nil {
		// Hint: step II.3.2
		// YOUR CODE HERE (lab4)
		panic("YOUR CODE HERE")
		return err
	}
	// Hint: step II.3.1
	// YOUR CODE HERE (lab4)
	panic("YOUR CODE HERE")
	return err
}

// Close implements the Executor Close interface.
func (e *InsertExec) Close() error {
	e.ctx.GetSessionVars().CurrInsertValues = chunk.Row{}
	if e.SelectExec != nil {
		return e.SelectExec.Close()
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *InsertExec) Open(ctx context.Context) error {
	if e.SelectExec != nil {
		var err error
		// Hint: step II.2
		// YOUR CODE HERE (lab4)
		panic("YOUR CODE HERE")
		return err
	}
	if !e.allAssignmentsAreConstant {
		e.initEvalBuffer()
	}
	return nil
}
