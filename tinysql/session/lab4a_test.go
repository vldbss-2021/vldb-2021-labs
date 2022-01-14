package session_test

import (
	"context"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = SerialSuites(&lab4ASessionSuite{})

type lab4ASessionSuite struct {
	testSessionSuiteBase
}

func (s *lab4ASessionSuite) prepareStmt(c *C, tk *testkit.TestKit, sql string) *executor.ExecStmt {
	charsetInfo, collation := tk.Se.GetSessionVars().GetCharsetInfo()
	ps := parser.New()
	stmtNodes, warns, err := ps.Parse(sql, charsetInfo, collation)
	c.Assert(err, IsNil)
	c.Assert(len(warns), Equals, 0)
	compiler := executor.Compiler{Ctx: tk.Se}
	stmt, err := compiler.Compile(context.Background(), stmtNodes[0])
	c.Assert(err, IsNil)
	return stmt
}

func (s *lab4ASessionSuite) mustInTxn(c *C, tk *testkit.TestKit) {
	status := tk.Se.GetSessionVars().GetStatusFlag(mysql.ServerStatusInTrans)
	c.Assert(status, IsTrue)
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	valid := txn.Valid()
	c.Assert(valid, IsTrue)
}

func (s *lab4ASessionSuite) mustNotInTxn(c *C, tk *testkit.TestKit) {
	status := tk.Se.GetSessionVars().GetStatusFlag(mysql.ServerStatusInTrans)
	c.Assert(status, IsFalse)
	txn, err := tk.Se.Txn(false)
	c.Assert(err, IsNil)
	valid := txn.Valid()
	c.Assert(valid, IsFalse)
	ctx := tk.Se.GetSessionVars().TxnCtx
	c.Assert(ctx.TableDeltaMap, IsNil)
}

func (s *lab4ASessionSuite) TestGetResults(c *C) {
	data := "vldbss-2021"
	tk := testkit.NewTestKitWithInit(c, s.store)
	rss, err := tk.Se.Execute(context.Background(), "select \""+data+"\"")
	c.Assert(err, IsNil)
	c.Assert(len(rss), Equals, 1)
	c.Assert(rss[0].Fields()[0].Column.Name.O, Equals, data)
	sRows, err := session.ResultSetToStringSlice(context.Background(), tk.Se, rss[0])
	c.Assert(err, IsNil)
	c.Assert(len(sRows), Equals, 1)
	c.Assert(len(sRows[0]), Equals, 1)
	c.Assert(sRows[0][0], Equals, data)
	_, err = tk.Se.Execute(context.Background(), "select1 \""+data+"\"")
	c.Assert(err, NotNil)
}

func (s *lab4ASessionSuite) TestRunStmt(c *C) {
	data := "vldbss-2021"
	sql := "select \"" + data + "\""
	tk := testkit.NewTestKitWithInit(c, s.store)
	stmt := s.prepareStmt(c, tk, sql)
	rs, err := session.RunStmt(context.Background(), tk.Se, stmt)
	c.Assert(err, IsNil)
	sRows, err := session.ResultSetToStringSlice(context.Background(), tk.Se, rs)
	c.Assert(err, IsNil)
	c.Assert(len(sRows), Equals, 1)
	c.Assert(len(sRows[0]), Equals, 1)
	c.Assert(sRows[0][0], Equals, data)
}

func (s *lab4ASessionSuite) TestHandleNoDelay(c *C) {
	data := "vldbss-2021"
	sql := "select \"" + data + "\""
	tk := testkit.NewTestKitWithInit(c, s.store)
	stmt := s.prepareStmt(c, tk, sql)
	e, err := stmt.BuildExecutor()
	c.Assert(err, IsNil)
	noDelay, _, err := stmt.HandleNoDelay(context.Background(), e)
	c.Assert(err, IsNil)
	c.Assert(noDelay, IsFalse)

	// test write SQL, this should be handled no delay
	tk.MustExec("create table t(id int primary key, val int)")
	sql = "insert into t values(1, 1)"
	stmt = s.prepareStmt(c, tk, sql)
	e, err = stmt.BuildExecutor()
	c.Assert(err, IsNil)
	noDelay, _, err = stmt.HandleNoDelay(context.Background(), e)
	c.Assert(err, IsNil)
	c.Assert(noDelay, IsTrue)

	// test read SQL, this should be handled delay
	sql = "select * from t"
	stmt = s.prepareStmt(c, tk, sql)
	e, err = stmt.BuildExecutor()
	c.Assert(err, IsNil)
	noDelay, _, err = stmt.HandleNoDelay(context.Background(), e)
	c.Assert(err, IsNil)
	c.Assert(noDelay, IsFalse)
}

func (s *lab4ASessionSuite) TestTxnBegin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	s.mustNotInTxn(c, tk)
	tk.MustExec("begin")
	s.mustInTxn(c, tk)
	ctx := tk.Se.GetSessionVars().TxnCtx
	c.Assert(ctx, NotNil)
	history := ctx.History

	// begin inside begin
	tk.MustExec("begin")
	s.mustInTxn(c, tk)
	ctx = tk.Se.GetSessionVars().TxnCtx
	c.Assert(ctx, NotNil)
	sameHistory := ctx.History == history
	c.Assert(sameHistory, IsFalse)
}

func (s *lab4ASessionSuite) TestTxnCommit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	s.mustNotInTxn(c, tk)
	tk.MustExec("begin")
	s.mustInTxn(c, tk)
	tk.MustExec("commit")
	s.mustNotInTxn(c, tk)
}

func (s *lab4ASessionSuite) TestTxnRollback(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	s.mustNotInTxn(c, tk)
	tk.MustExec("begin")
	s.mustInTxn(c, tk)
	tk.MustExec("rollback")
	s.mustNotInTxn(c, tk)
}
