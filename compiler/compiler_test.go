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
var Red = "\033[31m"
var Reset = "\033[0m"

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
	"select sql.id,sql.phone from (select id,accountId from personalInfo union select id,phone from userInfo) sql where sql.id = ?->SELECT \"id\", \"AccountId\" FROM (SELECT \"PersonalInfo\".\"Id\", \"PersonalInfo\".\"Phone\" FROM \"PersonalInfo\" union SELECT \"UserInfo\".\"Id\", \"UserInfo\".\"Phone\" FROM \"UserInfo\") AS \"sql\" WHERE \"sql\".\"id\" = $1",
	"select per.id,per.phone from personalInfo per->SELECT \"per\".\"Id\", \"per\".\"Phone\" FROM \"PersonalInfo\" AS \"per\"",
	"select id,code  from employeeInfo group by accountId having id*10>100->SELECT \"EmployeeInfo\".\"Id\", \"EmployeeInfo\".\"Code\" FROM \"EmployeeInfo\" GROUP BY \"EmployeeInfo\".\"AccountId\" HAVING \"EmployeeInfo\".\"Id\" * 10 > 100",

	"select per.id,per.phone from personalInfo per inner join employeeInfo emp on per.id=emp.id inner join dept on emp.deptId = dept.id->SELECT \"per\".\"Id\", \"per\".\"Phone\" FROM \"PersonalInfo\" AS \"per\" join \"EmployeeInfo\" AS \"emp\" ON \"per\".\"Id\" = \"emp\".\"Id\" join \"Dept\" ON \"emp\".\"DeptId\" = \"Dept\".\"Id\"",

	"SELECT id, phone FROM personalInfo->SELECT \"PersonalInfo\".\"Id\", \"PersonalInfo\".\"Phone\" FROM \"PersonalInfo\"",
	"select sum(sql.id),phone from (select id,phone from personalInfo union select id,phone from userInfo) sql->SELECT sum(\"sql\".\"id\"), \"phone\" FROM (SELECT \"PersonalInfo\".\"Id\", \"PersonalInfo\".\"Phone\" FROM \"PersonalInfo\" union SELECT \"UserInfo\".\"Id\", \"UserInfo\".\"Phone\" FROM \"UserInfo\") AS \"sql\"",
	"select * from personalInfo per,employeeInfo  where per.id = employeeInfo.id->SELECT * FROM \"PersonalInfo\" AS \"per\", \"EmployeeInfo\" WHERE \"per\".\"Id\" = \"EmployeeInfo\".\"Id\"",
	"select employeeInfo.phone, per.id from personalInfo per,employeeInfo  where per.id = employeeInfo.id->SELECT \"EmployeeInfo\".\"phone\", \"per\".\"Id\" FROM \"PersonalInfo\" AS \"per\", \"EmployeeInfo\" WHERE \"per\".\"Id\" = \"EmployeeInfo\".\"Id\"",

	"select max(id),phone from personalInfo group by phone having max(id) > 100 ->SELECT max(\"PersonalInfo\".\"Id\"), \"PersonalInfo\".\"Phone\" FROM \"PersonalInfo\" GROUP BY \"PersonalInfo\".\"Phone\" HAVING max(\"PersonalInfo\".\"Id\") > 100",
	"select max(id),phone from personalInfo->SELECT max(\"PersonalInfo\".\"Id\"), \"PersonalInfo\".\"Phone\" FROM \"PersonalInfo\"",
	"select id,phone from personalInfo union select id,phone from userInfo->SELECT \"PersonalInfo\".\"Id\", \"PersonalInfo\".\"Phone\" FROM \"PersonalInfo\" union SELECT \"UserInfo\".\"Id\", \"UserInfo\".\"Phone\" FROM \"UserInfo\"",
	"select code,concat(firstName,' ', lastName) as fullName from personalInfo where id  in (select id from employeeInfo)->SELECT \"PersonalInfo\".\"code\", concat(\"PersonalInfo\".\"FirstName\", ' ', \"PersonalInfo\".\"LastName\") AS \"fullName\" FROM \"PersonalInfo\" WHERE \"PersonalInfo\".\"Id\" in (SELECT \"EmployeeInfo\".\"Id\" FROM \"EmployeeInfo\")",
	"select 'insert into Stetament(code,sql) select code,sql from Gude',code as SQL from employeeInfo ->SELECT 'insert into Stetament(code,sql) select code,sql from Gude', \"EmployeeInfo\".\"Code\" AS \"sql\" FROM \"EmployeeInfo\"",
	// "insert info personalInfo (id, firstName, lastName)  select id, firstName, lastName from personalInfo ->INSERT INTO \"PersonalInfo\" (\"Id\", \"FirstName\", \"LastName\") SELECT \"Id\", \"FirstName\", \"LastName\" FROM \"PersonalInfo\" WHERE \"Id\" = $1",
	"select * from employeeInfo where id !=null->SELECT * FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" != NULL",

	"select concat(firstName,' ', lastName) as fullName from personalInfo->SELECT concat(\"PersonalInfo\".\"FirstName\", ' ', \"PersonalInfo\".\"LastName\") AS \"fullName\" FROM \"PersonalInfo\"",

	"insert into employeeInfo (id, name, code) values (?, ?, ?) ->INSERT INTO \"EmployeeInfo\" (\"Id\", \"name\", \"Code\") VALUES ($1, $2, $3)",
	"select id from employeeInfo where id = ?->SELECT \"EmployeeInfo\".\"Id\" FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" = $1",
	"select len(code)+id from employeeInfo where id = ? ->SELECT LENGTH(\"EmployeeInfo\".\"Code\") + \"EmployeeInfo\".\"Id\" FROM \"EmployeeInfo\" WHERE \"EmployeeInfo\".\"Id\" = $1",
	"select emp.Id from employeeInfo emp left join Dept dept on emp.DeptId = dept.Id where emp.Id = ?->SELECT \"emp\".\"Id\" FROM \"EmployeeInfo\" AS \"emp\" left join \"Dept\" AS \"dept\" ON \"emp\".\"DeptId\" = \"Dept\".\"Id\" WHERE \"emp\".\"Id\" = $1",
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
			fmt.Println(Red+"[", i, "]", sqlInput+"->"+sqtPrint+Reset)
		} else {
			fmt.Println("[", i, "]", sqlResult)
		}
		assert.Equal(t, sqlExpected, sqlResult)

	}

}
