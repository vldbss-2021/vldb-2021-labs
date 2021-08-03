package executor_test

import (
	"context"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testkit"
	"reflect"
)

var _ = Suite(&testSuiteLab4B{&baseTestSuite{}})

type testSuiteLab4B struct{ *baseTestSuite }

func (s *testSuiteLab4B) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuiteLab4B) prepareStmt(c *C, tk *testkit.TestKit, sql string) *executor.ExecStmt {
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

func (s *testSuiteLab4B) Test(c *C) {
	s.testBuildInsert(c)
	s.testInsertOpen(c)
	s.testInsertExec(c)
	s.testInsertSQL(c)
}

func (s *testSuiteLab4B) testInsertSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, val int)")
	tk.MustExec("insert into t values(1, 11), (2, 12), (13, 3), (4, 14), (15, 5)")

	r := tk.MustQuery(`select * from t order by id asc`)
	r.Check(testkit.Rows("1 11", "2 12", "4 14", "13 3", "15 5"))

	tk.MustExec("insert into t(id, val) select id + 10, val from t;")
	r = tk.MustQuery(`select * from t order by id asc`)
	r.Check(testkit.Rows("1 11", "2 12", "4 14", "11 11", "12 12", "13 3", "14 14", "15 5", "23 3", "25 5"))
}

func (s *testSuiteLab4B) testBuildInsert(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, val int)")
	stmt := s.prepareStmt(c, tk, "insert into t(id, id, val) values(1, 1, 1), (2, 2, 2)")
	_, err := stmt.BuildExecutor()
	c.Assert(err, NotNil)

	// test buildInsert
	stmt = s.prepareStmt(c, tk, "insert into t(id, val) values(1, 1), (2, 2), (3, 3)")
	exec, err := stmt.BuildExecutor()
	c.Assert(err, IsNil)
	insert, ok := exec.(*executor.InsertExec)
	c.Assert(ok, IsTrue)
	c.Assert(insert.Table.Meta().Name.L, Equals, "t")
	c.Assert(len(insert.Columns), Equals, 2)
	c.Assert(len(insert.Lists), Equals, 3)
	c.Assert(len(insert.SetList), Equals, 0)
}

func (s *testSuiteLab4B) testInsertOpen(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, val int)")
	tk.MustExec("create table t1(id int primary key, val int)")
	stmt := s.prepareStmt(c, tk, "insert into t(id, id, val) values(1, 1, 1), (2, 2, 2)")
	_, err := stmt.BuildExecutor()
	c.Assert(err, NotNil)

	stmt = s.prepareStmt(c, tk, "insert into t(id, val) select id, val from t1;")
	exec, err := stmt.BuildExecutor()
	c.Assert(err, IsNil)
	insert, ok := exec.(*executor.InsertExec)
	c.Assert(ok, IsTrue)
	err = insert.Open(context.Background())
	c.Assert(err, IsNil)
	selectExec, ok := insert.SelectExec.(*executor.TableReaderExecutor)
	c.Assert(ok, IsTrue)
	resultHandler := reflect.ValueOf(selectExec).Elem().FieldByName("resultHandler")
	c.Assert(resultHandler.IsNil(), IsFalse)

	stmt = s.prepareStmt(c, tk, "insert into t(id, val) select id + 10, val from t;")
	exec, err = stmt.BuildExecutor()
	c.Assert(err, IsNil)
	insert, ok = exec.(*executor.InsertExec)
	c.Assert(ok, IsTrue)
	err = insert.Open(context.Background())
	c.Assert(err, IsNil)
	_, ok = insert.SelectExec.(*executor.ProjectionExec)
	c.Assert(ok, IsTrue)
}

func (s *testSuiteLab4B) testInsertExec(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(id int primary key, val int)")
	tk.MustExec("create table t1(id int primary key, val int)")
	tk.MustExec("begin")

	stmt := s.prepareStmt(c, tk, "insert into t(id, val) values(1, 1), (2, 2), (3, 3);")
	exec, err := stmt.BuildExecutor()
	c.Assert(err, IsNil)
	insert, ok := exec.(*executor.InsertExec)
	c.Assert(ok, IsTrue)
	err = insert.Open(context.Background())
	c.Assert(err, IsNil)
	chk := new(chunk.Chunk)
	err = insert.Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.RecordRows(), Equals, uint64(3))

	stmt = s.prepareStmt(c, tk, "insert into t1(id, val) select id, val from t;")
	exec, err = stmt.BuildExecutor()
	c.Assert(err, IsNil)
	insert, ok = exec.(*executor.InsertExec)
	c.Assert(ok, IsTrue)
	err = insert.Open(context.Background())
	c.Assert(err, IsNil)
	chk = new(chunk.Chunk)
	err = insert.Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.RecordRows(), Equals, uint64(3))
	tk.MustExec("rollback")
}
