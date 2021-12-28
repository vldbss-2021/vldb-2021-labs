// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields     []*ast.ResultField
	executor   Executor
	stmt       *ExecStmt
	lastErr    error
	txnStartTS uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = colNames2ResultFields(a.executor.Schema(), a.stmt.OutputNames, a.stmt.Ctx.GetSessionVars().CurrentDB)
	}
	return a.fields
}

func colNames2ResultFields(schema *expression.Schema, names []*types.FieldName, defaultDB string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schema.Len())
	defaultDBCIStr := model.NewCIStr(defaultDB)
	for i := 0; i < schema.Len(); i++ {
		dbName := names[i].DBName
		if dbName.L == "" && names[i].TblName.L != "" {
			dbName = defaultDBCIStr
		}
		origColName := names[i].OrigColName
		if origColName.L == "" {
			origColName = names[i].ColName
		}
		rf := &ast.ResultField{
			Column:       &model.ColumnInfo{Name: origColName, FieldType: *schema.Columns[i].RetType},
			ColumnAsName: names[i].ColName,
			Table:        &model.TableInfo{Name: names[i].OrigTblName},
			TableAsName:  names[i].TblName,
			DBName:       dbName,
		}
		// This is for compatibility.
		// See issue https://github.com/pingcap/tidb/issues/10513 .
		if len(rf.ColumnAsName.O) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.O = rf.ColumnAsName.O[:mysql.MaxAliasIdentifierLen]
		}
		// Usually the length of O equals the length of L.
		// Add this len judgement to avoid panic.
		if len(rf.ColumnAsName.L) > mysql.MaxAliasIdentifierLen {
			rf.ColumnAsName.L = rf.ColumnAsName.L[:mysql.MaxAliasIdentifierLen]
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we update last query found rows in session variable as current found rows.
// The reason we need update is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply update last query found rows by the number of row in chunk.
func (a *recordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.stmt.Text), zap.Stack("stack"))
	}()

	err = Next(ctx, a.executor, req)
	if err != nil {
		a.lastErr = err
		return err
	}
	numRows := req.NumRows()
	if numRows == 0 {
		if a.stmt != nil {
			a.stmt.Ctx.GetSessionVars().LastFoundRows = a.stmt.Ctx.GetSessionVars().StmtCtx.FoundRows()
		}
		return nil
	}
	if a.stmt != nil {
		a.stmt.Ctx.GetSessionVars().StmtCtx.AddFoundRows(uint64(numRows))
	}
	return nil
}

// NewChunk create a chunk base on top-level executor's newFirstChunk().
func (a *recordSet) NewChunk() *chunk.Chunk {
	return newFirstChunk(a.executor)
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	sessVars := a.stmt.Ctx.GetSessionVars()
	sessVars.PrevStmt = FormatSQL(a.stmt.OriginText())
	return err
}

// ExecStmt implements the sqlexec.Statement interface, it builds a planner.Plan to an sqlexec.Statement.
type ExecStmt struct {
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan plannercore.Plan
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx sessionctx.Context

	// LowerPriority represents whether to lower the execution priority of a query.
	LowerPriority bool

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
}

// OriginText returns original statement as a string.
func (a *ExecStmt) OriginText() string {
	return a.Text
}

// IsReadOnly returns true if a statement is read only.
func (a *ExecStmt) IsReadOnly() bool {
	return ast.IsReadOnly(a.StmtNode)
}

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(string); !ok {
			panic(r)
		}
		err = errors.Errorf("%v", r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", a.Text), zap.Stack("stack"))
	}()

	sctx := a.Ctx
	var e Executor
	// Hint: step I.4.1
	// YOUR CODE HERE (lab4)
	panic("YOUR CODE HERE")
	if err != nil {
		return nil, err
	}

	// Hint: step I.4.2
	// YOUR CODE HERE (lab4)
	panic("YOUR CODE HERE")
	if err != nil {
		terror.Call(e.Close)
		return nil, err
	}

	if handled, result, err := a.handleNoDelay(ctx, e); handled {
		return result, err
	}

	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}
	return &recordSet{
		executor:   e,
		stmt:       a,
		txnStartTS: txnStartTS,
	}, nil
}

// HandleNoDelay exposes handleNoDelay, only for test usage
func (a *ExecStmt) HandleNoDelay(ctx context.Context, e Executor) (bool, sqlexec.RecordSet, error) {
	return a.handleNoDelay(ctx, e)
}

// handleNoDelay execute the given Executor if it does not return results to the client.
// The first return value stands for if it handle the executor.
func (a *ExecStmt) handleNoDelay(ctx context.Context, e Executor) (bool, sqlexec.RecordSet, error) {
	toCheck := e

	// If the executor doesn't return any result to the client, we execute it without delay.
	if toCheck.Schema().Len() == 0 {
		// Hint: step I.4.3
		// YOUR CODE HERE (lab4)
		panic("YOUR CODE HERE")
		//return true, r, err
		return true, nil, nil
	}

	return false, nil, nil
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e Executor) (sqlexec.RecordSet, error) {
	var err error
	defer func() {
		terror.Log(e.Close())
	}()

	// Hint: step I.4.3.1
	// YOUR CODE HERE (lab4)
	panic("YOUR CODE HERE")
	if err != nil {
		return nil, err
	}
	return nil, err
}

// BuildExecutor exposes buildExecutor, only for test usage
func (a *ExecStmt) BuildExecutor() (Executor, error) {
	return a.buildExecutor()
}

// buildExecutor build a executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor() (Executor, error) {
	ctx := a.Ctx

	b := newExecutorBuilder(ctx, a.InfoSchema)
	e := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	return e, nil
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

// FormatSQL is used to format the original SQL, e.g. truncating long SQL, appending prepared arguments.
func FormatSQL(sql string) stringutil.StringerFunc {
	return func() string {
		length := len(sql)
		if uint64(length) > logutil.DefaultQueryLogMaxLen {
			sql = fmt.Sprintf("%.*q(len:%d)", logutil.DefaultQueryLogMaxLen, sql, length)
		}
		return QueryReplacer.Replace(sql)
	}
}
