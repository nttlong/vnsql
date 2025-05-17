package compiler_test

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql/compiler"
	"github.com/stretchr/testify/assert"
)

var SqlCompiler compiler.Compiler

func TestCompiler(t *testing.T) {
	SqlCompiler = compiler.Compiler{
		TableDict: make(map[string]compiler.DbTableDictionaryItem),
		FieldDict: make(map[string]string),
		Quote: compiler.QuoteIdentifier{
			Left:  "\"",
			Right: "\"",
		},
	}
	t.Log(SqlCompiler)
	//pg connection string host localhost port 5432 user postgres password 123456 dbname db_001124 sslmode disable
	pgConnStr := "host=localhost port=5432 user=postgres password=123456 dbname=db_001124 sslmode=disable"
	db, err := sql.Open("postgres", pgConnStr)
	assert.NoError(t, err)

	err = db.Ping()
	assert.NoError(t, err)
	SqlCompiler.LoadDbDictionary(db)
	assert.NotEmpty(t, SqlCompiler.TableDict)
	assert.NotEmpty(t, SqlCompiler.FieldDict)

}

var sqlTest = []string{
	"select id from employeeInfo where id = ?->SELECT \"EmployeeInfo\".\"Id\" FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" = $1",
	"select len(code)+id from employeeInfo where id = ? ->SELECT LENGTH(\"EmployeeInfo\".\"Code\") + \"EmployeeInfo\".\"Id\" FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" = $1",
	"select emp.Id from employeeInfo emp left join Dept dept on emp.DeptId = dept.Id where emp.Id = ?->SELECT \"emp\".\"Id\" FROM \"EmployeeInfo\" AS \"emp\" left join \"Dept\" AS \"dept\" ON \"emp\".\"DeptId\" = \"dept\".\"Id\" WHERE \"emp\".\"Id\" = $1",
	"select emp.Id from employeeInfo emp where emp.Id = ?->SELECT \"emp\".\"Id\" FROM \"EmployeeInfo\" AS \"emp\" WHERE \"emp\".\"Id\" = $1",

	"select len(code) from employeeInfo where id = ?->SELECT LENGTH(\"EmployeeInfo\".\"Code\") FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" = $1",
	"select * from employeeInfo where id = ?->SELECT * FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" = $1",
}

func TestCompilerSQl(t *testing.T) {
	TestCompiler(t)
	assert.NotEmpty(t, &SqlCompiler.TableDict)
	assert.NotEmpty(t, &SqlCompiler.FieldDict)
	for i, sql := range sqlTest {
		sqlInput := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]

		sqlResult, err := SqlCompiler.Parse(sqlInput)
		assert.NoError(t, err)
		if err != nil {
			continue

		}
		if sqlExpected != sqlResult {
			sqtPrint := strings.Replace(sqlResult, "\"", "\\\"", -1)
			fmt.Println("[", i, "]", sqlInput+"->"+sqtPrint)
		}
		assert.Equal(t, sqlExpected, sqlResult)

	}

}
