// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package session

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// Session context, it is consistent with the lifecycle of a client connection.
type Session interface {
	sessionctx.Context
	Status() uint16                                               // Flag of current status, such as autocommit.
	LastInsertID() uint64                                         // LastInsertID is the last inserted auto_increment ID.
	AffectedRows() uint64                                         // Affected rows by latest executed stmt.
	Execute(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.
	String() string                                               // String is used to debug.
	CommitTxn(context.Context) error
	RollbackTxn(context.Context)
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	SetCommandValue(byte)
	SetTLSState(*tls.ConnectionState)
	SetCollation(coID int) error
	Close()
	// PrePareTxnCtx is exported for test.
	PrepareTxnCtx(context.Context)
	// FieldList returns fields list of a table.
	FieldList(tableName string) (fields []*ast.ResultField, err error)
}

var (
	_ Session = (*session)(nil)
)

type stmtRecord struct {
	st      sqlexec.Statement
	stmtCtx *stmtctx.StatementContext
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(st sqlexec.Statement, stmtCtx *stmtctx.StatementContext) {
	s := &stmtRecord{
		st:      st,
		stmtCtx: stmtCtx,
	}
	h.history = append(h.history, s)
}

// Count returns the count of the history.
func (h *StmtHistory) Count() int {
	return len(h.history)
}

type session struct {
	txn TxnState

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]interface{}
	}

	store kv.Storage

	parser *parser.Parser

	sessionVars *variable.SessionVars

	// ddlOwnerChecker is used in `select tidb_is_ddl_owner()` statement;
	ddlOwnerChecker owner.DDLOwnerChecker

	// shared coprocessor client per session
	client kv.Client
}

// DDLOwnerChecker returns s.ddlOwnerChecker.
func (s *session) DDLOwnerChecker() owner.DDLOwnerChecker {
	return s.ddlOwnerChecker
}

func (s *session) getMembufCap() int {
	return kv.DefaultTxnMembufCap
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}

func (s *session) LastInsertID() uint64 {
	if s.sessionVars.StmtCtx.LastInsertID > 0 {
		return s.sessionVars.StmtCtx.LastInsertID
	}
	return s.sessionVars.StmtCtx.InsertID
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) SetTLSState(tlsState *tls.ConnectionState) {
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *session) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}

func (s *session) SetCollation(coID int) error {
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		return err
	}
	for _, v := range variable.SetNamesVariables {
		terror.Log(s.sessionVars.SetSystemVar(v, cs))
	}
	terror.Log(s.sessionVars.SetSystemVar(variable.CollationConnection, co))
	return nil
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*ast.ResultField, error) {
	is := infoschema.GetInfoSchema(s)
	dbName := model.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := model.NewCIStr(tableName)
	table, err := is.TableByName(dbName, tName)
	if err != nil {
		return nil, err
	}

	cols := table.Cols()
	fields := make([]*ast.ResultField, 0, len(cols))
	for _, col := range table.Cols() {
		rf := &ast.ResultField{
			ColumnAsName: col.Name,
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta(),
			Column:       col.ColumnInfo,
		}
		fields = append(fields, rf)
	}
	return fields, nil
}

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}
	defer func() {
		s.txn.changeToInvalid()
		s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	}()
	if s.txn.IsReadOnly() {
		return nil
	}

	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) && kv.IsMockCommitErrorEnable() {
			kv.MockCommitErrorDisable()
			failpoint.Return(kv.ErrTxnRetryable)
		}
	})

	// Get the related table IDs.
	relatedTables := s.GetSessionVars().TxnCtx.TableDeltaMap
	tableIDs := make([]int64, 0, len(relatedTables))
	for id := range relatedTables {
		tableIDs = append(tableIDs, id)
	}
	// Set this option for 2 phase commit to validate schema lease.
	s.txn.SetOption(kv.SchemaChecker, domain.NewSchemaChecker(domain.GetDomain(s), s.sessionVars.TxnCtx.SchemaVersion, tableIDs))

	return s.txn.Commit(sessionctx.SetCommitCtx(ctx, s))
}

func (s *session) commitTxn(ctx context.Context) error {
	defer func() {
		s.txn.changeToInvalid()
	}()
	if !s.txn.Valid() {
		// If the transaction is invalid, maybe it has already been rolled back by the client.
		return nil
	}
	err := s.doCommit(ctx)

	if isoLevelOneShot := &s.sessionVars.TxnIsolationLevelOneShot; isoLevelOneShot.State != 0 {
		switch isoLevelOneShot.State {
		case 1:
			isoLevelOneShot.State = 2
		case 2:
			isoLevelOneShot.State = 0
			isoLevelOneShot.Value = ""
		}
	}

	if err != nil {
		logutil.Logger(ctx).Warn("commit failed",
			zap.String("finished txn", s.txn.GoString()),
			zap.Error(err))
		return err
	}
	return nil
}

func (s *session) CommitTxn(ctx context.Context) error {
	err := s.commitTxn(ctx)

	failpoint.Inject("keepHistory", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(err)
		}
	})

	s.sessionVars.TxnCtx.Cleanup()
	return err
}

func (s *session) RollbackTxn(ctx context.Context) {
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
	}
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (s *session) GetClient() kv.Client {
	return s.client
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"id":         sessVars.ConnectionID,
		"user":       "",
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	if s.txn.Valid() {
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}
	if sessVars.StmtCtx.LastInsertID > 0 {
		data["lastInsertID"] = sessVars.StmtCtx.LastInsertID
	}
	b, err := json.MarshalIndent(data, "", "  ")
	terror.Log(errors.Trace(err))
	return string(b)
}

// SchemaChangedWithoutRetry is used for testing.
var SchemaChangedWithoutRetry uint32

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	if s.txn.doNotCommit == nil {
		return nil
	}
	// If the transaction is aborted, the following statements do not need to execute, except `commit` and `rollback`,
	// because they are used to finish the aborted transaction.
	if _, ok := stmt.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
		return nil
	}
	if _, ok := stmt.(*executor.ExecStmt).StmtNode.(*ast.RollbackStmt); ok {
		return nil
	}
	return errors.New("current transaction is aborted, commands ignored until end of transaction block:" + s.txn.doNotCommit.Error())
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

func (s *session) sysSessionPool() sessionPool {
	return domain.GetDomain(s).SysSessionPool()
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *session) ExecRestrictedSQL(sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, err
	}
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)

	return execRestrictedSQL(ctx, se, sql)
}

func execRestrictedSQL(ctx context.Context, se *session, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	recordSets, err := se.Execute(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		tmp, err := drainRecordSet(ctx, se, rs)
		if err != nil {
			return nil, nil, err
		}
		if err = rs.Close(); err != nil {
			return nil, nil, err
		}

		if i == 0 {
			rows = tmp
			fields = rs.Fields()
		}
	}
	return rows, fields, nil
}

func createSessionFunc(store kv.Storage) pools.Factory {
	return func() (pools.Resource, error) {
		se, err := createSession(store)
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutoCommit, types.NewStringDatum("1"))
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.MaxExecutionTime, types.NewUintDatum(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func createSessionWithDomainFunc(store kv.Storage) func(*domain.Domain) (pools.Resource, error) {
	return func(dom *domain.Domain) (pools.Resource, error) {
		se, err := CreateSessionWithDomain(store, dom)
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.AutoCommit, types.NewStringDatum("1"))
		if err != nil {
			return nil, err
		}
		err = variable.SetSessionSystemVar(se.sessionVars, variable.MaxExecutionTime, types.NewUintDatum(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		return se, nil
	}
}

func drainRecordSet(ctx context.Context, se *session, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	var rows []chunk.Row
	req := rs.NewChunk()
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.sessionVars.MaxChunkSize)
	}
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx sessionctx.Context, sql string) (string, error) {
	rows, fields, err := s.ExecRestrictedSQL(sql)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", executor.ErrResultIsEmpty
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		return "", err
	}
	return value, nil
}

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (s *session) GetAllSysVars() (map[string]string, error) {
	if s.Value(sessionctx.Initing) != nil {
		return nil, nil
	}
	sql := `SELECT VARIABLE_NAME, VARIABLE_VALUE FROM %s.%s;`
	sql = fmt.Sprintf(sql, mysql.SystemDB, mysql.GlobalVariablesTable)
	rows, _, err := s.ExecRestrictedSQL(sql)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]string)
	for _, r := range rows {
		k, v := r.GetString(0), r.GetString(1)
		ret[k] = v
	}
	return ret, nil
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(s, sql)
	if err != nil {
		if executor.ErrResultIsEmpty.Equal(err) {
			if sv, ok := variable.SysVars[name]; ok {
				return sv.Value, nil
			}
			return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
		}
		return "", err
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name, value string) error {
	if name == variable.SQLModeVar {
		value = mysql.FormatSQLModeStr(value)
		if _, err := mysql.GetSQLMode(value); err != nil {
			return err
		}
	}
	var sVal string
	var err error
	sVal, err = variable.ValidateSetSystemVar(s.sessionVars, name, value)
	if err != nil {
		return err
	}
	name = strings.ToLower(name)
	sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name, sVal)
	_, _, err = s.ExecRestrictedSQL(sql)
	return err
}

func (s *session) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, []error, error) {
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	return s.parser.Parse(sql, charset, collation)
}

func (s *session) executeStatement(ctx context.Context, connID uint64, stmtNode ast.StmtNode, stmt sqlexec.Statement, recordSets []sqlexec.RecordSet, inMulitQuery bool) ([]sqlexec.RecordSet, error) {
	s.SetValue(sessionctx.QueryString, stmt.OriginText())
	if _, ok := stmtNode.(ast.DDLNode); ok {
		s.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		s.ClearValue(sessionctx.LastExecuteDDL)
	}
	recordSet, err := runStmt(ctx, s, stmt)
	if err != nil {
		if !kv.ErrKeyExists.Equal(err) {
			logutil.Logger(ctx).Warn("run statement failed",
				zap.Int64("schemaVersion", s.sessionVars.TxnCtx.SchemaVersion),
				zap.Error(err),
				zap.String("session", s.String()))
		}
		return nil, err
	}

	if inMulitQuery && recordSet == nil {
		recordSet = &multiQueryNoDelayRecordSet{
			affectedRows: s.AffectedRows(),
			warnCount:    s.sessionVars.StmtCtx.WarningCount(),
			lastInsertID: s.sessionVars.StmtCtx.LastInsertID,
			status:       s.sessionVars.Status,
		}
	}

	if recordSet != nil {
		recordSets = append(recordSets, recordSet)
	}
	return recordSets, nil
}

func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	if recordSets, err = s.execute(ctx, sql); err != nil {
		s.sessionVars.StmtCtx.AppendError(err)
	}
	return
}

func (s *session) execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	s.PrepareTxnCtx(ctx)
	connID := s.sessionVars.ConnectionID
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return nil, err
	}

	charsetInfo, collation := s.sessionVars.GetCharsetInfo()

	// Step1: Compile query string to abstract syntax trees(ASTs).
	parseStartTime := time.Now()

	var (
		stmtNodes []ast.StmtNode
		warns     []error
	)

	// Hint: step I.3.1
	// YOUR CODE HERE (lab4)
	panic("YOUR CODE HERE")
	if err != nil {
		s.rollbackOnError(ctx)
		logutil.Logger(ctx).Warn("parse SQL failed",
			zap.Error(err),
			zap.String("SQL", sql))
		return nil, util.SyntaxError(err)
	}
	durParse := time.Since(parseStartTime)
	s.GetSessionVars().DurationParse = durParse

	compiler := executor.Compiler{Ctx: s}
	multiQuery := len(stmtNodes) > 1
	logutil.Logger(ctx).Debug("session execute", zap.Uint64("connID", connID),
		zap.String("charsetInfo", charsetInfo), zap.String("collation", collation),
		zap.Uint64("compiler conn", compiler.Ctx.GetSessionVars().ConnectionID),
		zap.Bool("multiQuery", multiQuery))
	for _, stmtNode := range stmtNodes {
		s.sessionVars.StartTime = time.Now()
		s.PrepareTxnCtx(ctx)

		// Step2: Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
		// Some executions are done in compile stage, so we reset them before compile.
		if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
			return nil, err
		}
		var stmt *executor.ExecStmt
		// Hint: step I.3.2
		// YOUR CODE HERE (lab4)
		panic("YOUR CODE HERE")
		if stmt != nil {
			logutil.Logger(ctx).Debug("stmt", zap.String("sql", stmt.Text))
		}
		if err != nil {
			s.rollbackOnError(ctx)
			logutil.Logger(ctx).Warn("compile SQL failed",
				zap.Error(err),
				zap.String("SQL", sql))
			return nil, err
		}
		durCompile := time.Since(s.sessionVars.StartTime)
		s.GetSessionVars().DurationCompile = durCompile

		// Step3: Execute the physical plan.

		// Hint: step I.3.3
		// YOUR CODE HERE (lab4)
		panic("YOUR CODE HERE")
		if err != nil {
			return nil, err
		}
	}

	if s.sessionVars.ClientCapability&mysql.ClientMultiResults == 0 && len(recordSets) > 1 {
		// return the first recordset if client doesn't support ClientMultiResults.
		recordSets = recordSets[:1]
	}

	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	return recordSets, nil
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		s.RollbackTxn(ctx)
	}
}

func (s *session) Txn(active bool) (kv.Transaction, error) {
	if !s.txn.validOrPending() && active {
		return &s.txn, kv.ErrInvalidTxn
	}
	if s.txn.pending() && active {
		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		txnCap := s.getMembufCap()
		if err := s.txn.changePendingToValid(txnCap); err != nil {
			logutil.BgLogger().Error("active transaction fail",
				zap.Error(err))
			s.txn.cleanup()
			s.sessionVars.TxnCtx.StartTS = 0
			return &s.txn, err
		}
		s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		if !s.sessionVars.IsAutocommit() {
			s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
		if s.sessionVars.GetReplicaRead().IsFollowerRead() {
			s.txn.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
		}
	}
	return &s.txn, nil
}

func (s *session) NewTxn(ctx context.Context) error {
	if s.txn.Valid() {
		txnID := s.txn.StartTS()
		err := s.CommitTxn(ctx)
		if err != nil {
			return err
		}
		vars := s.GetSessionVars()
		logutil.Logger(ctx).Info("NewTxn() inside a transaction auto commit",
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", txnID))
	}

	txn, err := s.store.Begin()
	if err != nil {
		return err
	}
	txn.SetCap(s.getMembufCap())
	txn.SetVars(s.sessionVars.KVVars)
	if s.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		txn.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	s.txn.changeInvalidToValid(txn)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
	}
	return nil
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) interface{} {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

// Close function does some clean work when session end.
func (s *session) Close() {
	ctx := context.TODO()
	s.RollbackTxn(ctx)
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (Session, error) {
	s, err := CreateSession(store)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().InitChunkSize = 2
		s.GetSessionVars().MaxChunkSize = 32
	}
	return s, err
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	s, err := createSession(store)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	initLoadCommonGlobalVarsSQL()

	if !getStoreBootstrap(store) {
		runInBootstrapSession(store, bootstrap)
	}

	se, err := createSession(store)
	if err != nil {
		return nil, err
	}

	dom := domain.GetDomain(se)

	se1, err := createSession(store)
	if err != nil {
		return nil, err
	}
	err = dom.UpdateTableStatsLoop(se1)
	if err != nil {
		return nil, err
	}

	return dom, err
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// runInBootstrapSession create a special session for boostrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(Session)) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}

	s.SetValue(sessionctx.Initing, true)
	bootstrap(s)
	finishBootstrap(store)
	s.ClearValue(sessionctx.Initing)

	dom := domain.GetDomain(s)
	dom.Close()
	domap.Delete(store)
}

func createSession(store kv.Storage) (*session, error) {
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	s := &session{
		store:           store,
		parser:          parser.New(),
		sessionVars:     variable.NewSessionVars(),
		ddlOwnerChecker: dom.DDL().OwnerManager(),
		client:          store.GetClient(),
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()
	return s, nil
}

// CreateSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSesion directly.
func CreateSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	s := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
		client:      store.GetClient(),
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()
	return s, nil
}

const (
	notBootstrapped = 0
	bootstrapped    = 1
)

func getStoreBootstrap(store kv.Storage) bool {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		return true
	}

	var ver int64
	// check in kv store
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})

	if err != nil {
		logutil.BgLogger().Fatal("check bootstrapped failed",
			zap.Error(err))
	}

	if ver != notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	return ver != notBootstrapped
}

func finishBootstrap(store kv.Storage) {
	setStoreBootstrapped(store.UUID())

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(bootstrapped)
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

var builtinGlobalVariable = []string{
	variable.AutoCommit,
	variable.SQLModeVar,
	variable.MaxAllowedPacket,
	variable.TimeZone,
	variable.BlockEncryptionMode,
	variable.WaitTimeout,
	variable.InteractiveTimeout,
	variable.MaxPreparedStmtCount,
	variable.InitConnect,
	variable.TxnIsolation,
	variable.TxReadOnly,
	variable.TransactionIsolation,
	variable.TransactionReadOnly,
	variable.NetBufferLength,
	variable.QueryCacheType,
	variable.QueryCacheSize,
	variable.CharacterSetServer,
	variable.AutoIncrementIncrement,
	variable.CollationServer,
	variable.NetWriteTimeout,
	variable.MaxExecutionTime,
	variable.InnodbLockWaitTimeout,

	/* TiDB specific global variables: */
	variable.TiDBSkipUTF8Check,
	variable.TiDBIndexLookupSize,
	variable.TiDBIndexLookupConcurrency,
	variable.TiDBIndexLookupJoinConcurrency,
	variable.TiDBIndexSerialScanConcurrency,
	variable.TiDBHashJoinConcurrency,
	variable.TiDBProjectionConcurrency,
	variable.TiDBHashAggPartialConcurrency,
	variable.TiDBHashAggFinalConcurrency,
	variable.TiDBBackoffLockFast,
	variable.TiDBBackOffWeight,
	variable.TiDBConstraintCheckInPlace,
	variable.TiDBDDLReorgWorkerCount,
	variable.TiDBDDLReorgBatchSize,
	variable.TiDBDDLErrorCountLimit,
	variable.TiDBOptInSubqToJoinAndAgg,
	variable.TiDBOptCorrelationThreshold,
	variable.TiDBOptCorrelationExpFactor,
	variable.TiDBOptCPUFactor,
	variable.TiDBOptCopCPUFactor,
	variable.TiDBOptNetworkFactor,
	variable.TiDBOptScanFactor,
	variable.TiDBOptDescScanFactor,
	variable.TiDBOptMemoryFactor,
	variable.TiDBOptDiskFactor,
	variable.TiDBOptConcurrencyFactor,
	variable.TiDBDistSQLScanConcurrency,
	variable.TiDBInitChunkSize,
	variable.TiDBMaxChunkSize,
	variable.TiDBEnableCascadesPlanner,
	variable.TiDBEnableVectorizedExpression,
	variable.TiDBEnableNoopFuncs,
	variable.TiDBMaxDeltaSchemaCount,
}

var (
	loadCommonGlobalVarsSQLOnce sync.Once
	loadCommonGlobalVarsSQL     string
)

func initLoadCommonGlobalVarsSQL() {
	loadCommonGlobalVarsSQLOnce.Do(func() {
		vars := append(make([]string, 0, len(builtinGlobalVariable)), builtinGlobalVariable...)
		loadCommonGlobalVarsSQL = "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" + strings.Join(vars, quoteCommaQuote) + "')"
	})
}

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	initLoadCommonGlobalVarsSQL()
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return nil
	}

	var err error
	// Use GlobalVariableCache if TiDB just loaded global variables within 2 second ago.
	// When a lot of connections connect to TiDB simultaneously, it can protect TiKV meta region from overload.
	gvc := domain.GetDomain(s).GetGlobalVarsCache()
	succ, rows, fields := gvc.Get()
	if !succ {
		// Set the variable to true to prevent cyclic recursive call.
		vars.CommonGlobalLoaded = true
		rows, fields, err = s.ExecRestrictedSQL(loadCommonGlobalVarsSQL)
		if err != nil {
			vars.CommonGlobalLoaded = false
			logutil.BgLogger().Error("failed to load common global variables.")
			return err
		}
		gvc.Update(rows, fields)
	}

	for _, row := range rows {
		varName := row.GetString(0)
		varVal := row.GetDatum(1, &fields[1].Column.FieldType)
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = variable.SetSessionSystemVar(s.sessionVars, varName, varVal)
			if err != nil {
				return err
			}
		}
	}

	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		if varVal, ok := vars.GetSystemVar(variable.InteractiveTimeout); ok {
			if err := vars.SetSystemVar(variable.WaitTimeout, varVal); err != nil {
				return err
			}
		}
	}

	vars.CommonGlobalLoaded = true
	return nil
}

// PrepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx context.Context) {
	if s.txn.validOrPending() {
		return
	}

	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
	}
}

// PrepareTxnFuture uses to try to get txn future.
func (s *session) PrepareTxnFuture(ctx context.Context) {
	if s.txn.validOrPending() {
		return
	}

	txnFuture := s.getTxnFuture(ctx)
	s.txn.changeInvalidToPending(txnFuture)
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	if err := s.doCommit(ctx); err != nil {
		return err
	}

	return s.NewTxn(ctx)
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *session) InitTxnWithStartTS(startTS uint64) error {
	if s.txn.Valid() {
		return nil
	}

	// no need to get txn from txnFutureCh since txn should init with startTs
	txn, err := s.store.BeginWithStartTS(startTS)
	if err != nil {
		return err
	}
	s.txn.changeInvalidToValid(txn)
	s.txn.SetCap(s.getMembufCap())
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return err
	}
	return nil
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

type multiQueryNoDelayRecordSet struct {
	sqlexec.RecordSet

	affectedRows uint64
	status       uint16
	warnCount    uint16
	lastInsertID uint64
}

func (c *multiQueryNoDelayRecordSet) Close() error {
	return nil
}

func (c *multiQueryNoDelayRecordSet) AffectedRows() uint64 {
	return c.affectedRows
}

func (c *multiQueryNoDelayRecordSet) WarnCount() uint16 {
	return c.warnCount
}

func (c *multiQueryNoDelayRecordSet) Status() uint16 {
	return c.status
}

func (c *multiQueryNoDelayRecordSet) LastInsertID() uint64 {
	return c.lastInsertID
}
