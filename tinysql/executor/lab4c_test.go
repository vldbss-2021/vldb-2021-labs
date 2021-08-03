package executor_test

import (
	"context"
	"fmt"
	"reflect"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testSuiteLab4C{&baseTestSuite{}})

type testSuiteLab4C struct{ *baseTestSuite }

func (s *testSuiteLab4C) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuiteLab4C) prepareStmt(c *C, tk *testkit.TestKit, sql string) *executor.ExecStmt {
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

func (s *testSuiteLab4C) Test(c *C) {
	s.testBuildProjection(c)
	s.testReadSQL(c)
}

func (s *testSuiteLab4C) testBuildProjection(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, val int)")

	stmt := s.prepareStmt(c, tk, "select id, val + 10 from t where val < 10")
	exec, err := stmt.BuildExecutor()
	c.Assert(err, IsNil)
	proj, ok := exec.(*executor.ProjectionExec)
	c.Assert(ok, IsTrue)
	childrenVal := reflect.ValueOf(proj).Elem().FieldByName("children")
	c.Assert(childrenVal.IsNil(), IsFalse)
}

func (s *testSuiteLab4C) testReadSQL(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, val int)")
	tk.MustExec("insert into t values(1, 11), (2, 12), (13, 3), (4, 14), (15, 5)")

	r := tk.MustQuery(`select id, val + 10 from t where val < 10`)
	r.Check(testkit.Rows("13 13", "15 15"))
}
