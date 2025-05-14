package xdb_test

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/nttlong/vnsql/internal/xdb"
	_ "github.com/nttlong/vnsql/internal/xdb"
	"github.com/nttlong/vnsql/internal/xdb/common"
	"github.com/nttlong/vnsql/internal/xdb/isql"
	"github.com/nttlong/vnsql/internal/xdb/parser"
	pgParser "github.com/nttlong/vnsql/internal/xdb/parser/postgres"
	"github.com/stretchr/testify/assert"
)

type IBaseModle interface {
}
type BaseModel struct {
	IBaseModle
	Id          uuid.UUID  `db:"pk"`
	Code        string     `db:"unique;varchar(10)"`
	CreatedOn   time.Time  `db:"index"`
	UpdatedOn   *time.Time `db:"index"`
	CreatedBy   string
	UpdatedBy   *string
	Description string
}
type Emp struct {
	BaseModel
	FirtsName string `db:"index"`
	LastName  string `db:"index"`
	Birthday  time.Time
}
type User struct {
	BaseModel

	Name     string `db:"index"`
	Email    string `db:"unique"`
	Password string
	Phone    string `db:"unique:phone_code"`
	Code     string `db:"unique:phone_code;varchar(10)"`
	Emp      *Emp   `db:"fk:Id"`
}

var PgSql isql.ISql
var TbaleInfo common.TableInfo

func TestCommonGetColumnsInfoOfType(t *testing.T) {
	tblInfo, err := common.GetTableInfo("postgres", &User{}, func(col *common.ColInfo) error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, "User", tblInfo.Name)
	assert.Equal(t, 12, len(tblInfo.Columns))
	assert.True(t, len(tblInfo.RelationTables) > 0)
	assert.Equal(t, "Emp", tblInfo.RelationTables[0].ForeingTable)
	assert.Equal(t, "Id", tblInfo.RelationTables[0].ForeingKey[0])
	fmt.Print(tblInfo)
	TbaleInfo = *tblInfo
}
func TestGetAllIndexInColsInfo(t *testing.T) {
	TestCommonGetColumnsInfoOfType(t)
	indexInfo := common.GetAllIndexInColsInfo(TbaleInfo.Columns)
	for k, v := range indexInfo {
		fmt.Println(k, v)
	}
}
func TestConfig(t *testing.T) {
	sql, err := xdb.NewSql("postgres")
	PgSql = sql
	assert.NoError(t, err)
	PgSql.SetConfig(isql.DbCfg{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
	})
	err = PgSql.PingDb()
	assert.NoError(t, err)
}
func TestGetTableInfo(t *testing.T) {
	TestConfig(t)
	tblInfo, err := PgSql.GetMigrate("test").GetColumnsInfo(&User{})
	assert.NoError(t, err)
	assert.Equal(t, "User", tblInfo.Name)
	assert.Equal(t, 11, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		if col.Name == "Code" {
			assert.Equal(t, 10, col.Size)
		}
	}

}
func TestGetIndexinfo(t *testing.T) {
	TestGetTableInfo(t)
	idxInfo, err := PgSql.GetMigrate("test").GetIndexInfo(&User{})
	assert.NoError(t, err)
	assert.Equal(t, 5, len(idxInfo)) // has five index
	fmt.Println(idxInfo)
}
func TestSqlCreateTable(t *testing.T) {
	TestGetIndexinfo(t)
	srrSQl, err := PgSql.GetMigrate("test").GetSqlCreateTable(&User{})
	assert.NoError(t, err)
	db, err := PgSql.OpenDb("test")
	if err != nil {
		fmt.Println(err)
	}

	for _, sql := range srrSQl {
		_, err := db.Exec(sql)
		if err != nil {

			strErr := err.Error()
			if strings.Contains(strErr, "already exists") {
				fmt.Println(err)
			} else {
				assert.NoError(t, err)
			}
		} else {
			assert.NoError(t, err)
		}

		fmt.Println(sql)
	}

}
func TestDoMigrate(t *testing.T) {
	start := time.Now()
	TestSqlCreateTable(t)

	err := PgSql.GetMigrate("test").DoMigrate(&User{})
	if pgErr, ok := err.(*pq.Error); ok {
		fmt.Println("PostgreSQL Error:")
		fmt.Println("  SQLState:", pgErr.SQLState()) // Corrected line: calling the method
		fmt.Println("  Message:", pgErr.Message)
		fmt.Println("  Detail:", pgErr.Detail)
		fmt.Println("  Hint:", pgErr.Hint)
		fmt.Println("  Where:", pgErr.Where)
		// You can access other fields of the pq.Error struct as well
	} else {
		fmt.Println("Generic SQL error:", err)
	}
	fmt.Println(time.Now().Sub(start).Milliseconds())
	assert.NoError(t, err)

	for i := 0; i < 10000; i++ {
		start = time.Now()
		PgSql.GetMigrate("test").DoMigrate(&User{})
		fmt.Println(time.Now().Sub(start).Nanoseconds())
	}
}

var testList []string = []string{

	"select * from A,B order by A.id,B.id asc->SELECT * FROM \"A\", \"B\" ORDER BY \"A\".\"id\" ASC, \"B\".\"id\" ASC",
	"select * from user where id = ?  union select * from dept->SELECT * FROM \"user\" WHERE \"id\" = v1 union SELECT * FROM \"dept\"",
	"select sum(amount),min(amount), max(amount) max,deptId from dept group by deptId having max<10000->SELECT sum(\"amount\"), min(\"amount\"), max(\"amount\") AS \"max\", \"deptId\" FROM \"dept\" GROUP BY \"deptId\" HAVING \"max\" < 10000",
	"select sum(amount),min(amount), max(amount) max,deptId from dept group by deptId->SELECT sum(\"amount\"), min(\"amount\"), max(\"amount\") AS \"max\", \"deptId\" FROM \"dept\" GROUP BY \"deptId\"",
	//"select sum(amount),min(amount), max(amount) max,deptId from dept group by deptId->SELECT sum(\"amount\") AS \"min\", max(\"amount\") AS \"max\", \"deptId\" FROM \"dept\" GROUP BY \"deptId\"",
	"select sum(amount),deptId from dept group by deptId->SELECT sum(\"amount\"), \"deptId\" FROM \"dept\" GROUP BY \"deptId\"",
	"select emp.id,dept.dept_id from emp left join dept on dept.id = emp.dept_id->SELECT \"emp\".\"id\", \"dept\".\"dept_id\" FROM \"emp\" left join \"dept\" ON \"dept\".\"id\" = \"emp\".\"dept_id\"",
	"select emp.id,dept.dept_id from emp  join dept on dept.id = emp.dept_id->SELECT \"emp\".\"id\", \"dept\".\"dept_id\" FROM \"emp\" join \"dept\" ON \"dept\".\"id\" = \"emp\".\"dept_id\"",
	"select ? id->SELECT v1 AS \"id\" FROM \"dual\"",
	"select * from user U,dept D->SELECT * FROM \"user\" AS \"U\", \"dept\" AS \"D\"",
	"select * from user where id = ?->SELECT * FROM \"user\" WHERE \"id\" = v1",

	"select * from user U->SELECT * FROM \"user\" AS \"U\"",
	"select * from user->SELECT * FROM \"user\"",

	"select concat(firdtName, ?, lastName) fullName->SELECT concat(\"firdtName\", v1, \"lastName\") AS \"fullName\" FROM \"dual\"",
	"select len(code) +? as Lt->SELECT len(\"code\") + v1 AS \"Lt\" FROM \"dual\"",
	"select (?+15*2) id->SELECT (v1 + 15 * 2) AS \"id\" FROM \"dual\"",

	"select @p1 id->SELECT p1 AS \"id\" FROM \"dual\"",
	"select abc.*->SELECT \"abc\".* FROM \"dual\"",

	"select *->SELECT * FROM \"dual\"",

	"select abc.*,cdf.*->SELECT \"abc\".*, \"cdf\".* FROM \"dual\"",
}
var testSubQuries = []string{
	"select * from user where id = (select id from dept where dept_id = ?)->SELECT * FROM \"user\" WHERE \"id\" = (SELECT \"id\" FROM \"dept\" WHERE \"dept_id\" = v1)",
	"select * from (select * from user where id = ?) A->SELECT * FROM (SELECT * FROM \"user\" WHERE \"id\" = v1) AS \"A\"",
}
var testlLikeSql = []string{
	"select * from user where code not like ?->SELECT * FROM \"user\" WHERE \"code\" not like v1",
	"select * from user where code like ?->SELECT * FROM \"user\" WHERE \"code\" like v1",
	"select * from user where code like ? and id = ?->SELECT * FROM \"user\" WHERE \"code\" like v1 AND \"id\" = v2",
}

func TestParser(t *testing.T) {

	w := parser.Walker{
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	//testAll := append(testList, testSubQuries...)
	for i, sql := range testlLikeSql {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}

}

var testInsertIntoQuery = []string{
	"insert into A (a,b,c) select d,e,f from G->INSERT INTO \"A\" (\"a\", \"b\", \"c\") SELECT \"d\", \"e\", \"f\" FROM \"G\"",
	"insert into user (id,code,created_on,updated_on,created_by,updated_by,description) values (?,?)->INSERT INTO \"user\" (\"id\", \"code\", \"created_on\", \"updated_on\", \"created_by\", \"updated_by\", \"description\") VALUES (v1, v2)",
}

func TestInsertIntoParser(t *testing.T) {
	w := parser.Walker{
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	for i, sql := range testInsertIntoQuery {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}
}

var testUpdateQuery = []string{
	"update A set a = ?, b = ?, c = ? where d = ?->UPDATE \"A\" SET \"a\" = v1, \"b\" = v2, \"c\" = v3, WHERE WHERE \"d\" = v4",
	"update user set code = (select code from dept where dept_id = ?), updated_on = ? where id = ?->UPDATE \"user\" SET \"code\" = (SELECT \"code\" FROM \"dept\" WHERE WHERE \"dept_id\" = v1), \"updated_on\" = v2, WHERE WHERE \"id\" = v3",
}

func TestUpdateParser(t *testing.T) {
	w := parser.Walker{
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	for i, sql := range testUpdateQuery {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}
}

var testSeletctTopAndLimit = []string{
	"select  A.* from A order by id limit ?,?->SELECT \"A\".* FROM \"A\" ORDER BY \"id\" ASC LIMIT @p1,@p2",
	"select  A.* from A order by id limit 50,?->SELECT \"A\".* FROM \"A\" ORDER BY \"id\" ASC LIMIT 50,@p1",
	"select  A.* from A order by id limit 50,100->SELECT \"A\".* FROM \"A\" ORDER BY \"id\" ASC LIMIT 50,100",
}

func TestSelectTopAndLimitParser(t *testing.T) {
	w := parser.Walker{
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Params {
				node.V = strings.Replace(node.V, "v", "@p", -1)
			}
			if node.Nt == parser.OffsetAndLimit {
				if node.Offset != "" && node.Limit != "" {
					node.V = "LIMIT " + node.Offset + "," + node.Limit
					return node, nil
				}
				if node.Offset == "" && node.Limit != "" {
					node.V = "LIMIT " + node.Limit
					return node, nil
				}
				if node.Offset != "" && node.Limit == "" {
					node.V = "LIMIT " + node.Offset
					return node, nil
				}
				if node.Offset == "" && node.Limit == "" {

					return node, errors.New("loi roi")
				}
			}
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	for i, sql := range testSeletctTopAndLimit {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}
}

var TestDelete = []string{
	"delete from A using B where A.id = B.id ->DELETE FROM \"A\" USING \"B\" WHERE \"A\".\"id\" = \"B\".\"id\"",
	"select * from A where d='2001-01-01'->SELECT * FROM \"A\" WHERE \"d\" = '01-01-2001'",

	"delete from A where id = ?->DELETE FROM  USING \"A\" WHERE \"id\" = @p1",
	"delete from user where id = ? and code = ?->DELETE FROM  USING \"user\" WHERE \"id\" = @p1 AND \"code\" = @p2",
}

func TestDeleteParser(t *testing.T) {
	w := parser.Walker{
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Value {
				if td, ok := node.IsDate(node.V); ok {
					//dd-MM-yyyy
					strT := td.Format("02-01-2006")
					node.V = "'" + strT + "'"

				}

			}
			if node.Nt == parser.Params {
				node.V = strings.Replace(node.V, "v", "@p", -1)
			}
			if node.Nt == parser.OffsetAndLimit {
				if node.Offset != "" && node.Limit != "" {
					node.V = "LIMIT " + node.Offset + "," + node.Limit
					return node, nil
				}
				if node.Offset == "" && node.Limit != "" {
					node.V = "LIMIT " + node.Limit
					return node, nil
				}
				if node.Offset != "" && node.Limit == "" {
					node.V = "LIMIT " + node.Offset
					return node, nil
				}
				if node.Offset == "" && node.Limit == "" {

					return node, errors.New("loi roi")
				}
			}
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	for i, sql := range TestDelete {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}
}

var sqlSelectCaseWhen = []string{
	"select case when a.c=1 then 'a' when a.c=2 then 'b' else 'c' end as name from A->SELECT CASE WHEN \"a\".\"c\" = 1 THEN a WHEN \"a\".\"c\" = 2 THEN b ELSE c END AS \"name\" FROM \"A\"",
	"select case when id = ? then 'a' when id = ? then 'b' else ? end as name from A->SELECT CASE WHEN \"id\" = @p1 THEN a WHEN \"id\" = @p2 THEN b ELSE @p3 END AS \"name\" FROM \"A\"",
	"select case when id = 1 then 'a' when id = 2 then 'b' else 'c' end as name from A->SELECT CASE WHEN \"id\" = 1 THEN a WHEN \"id\" = 2 THEN b ELSE c END AS \"name\" FROM \"A\"",
	"select case when id = 1 then 'a' when id = 2 then 'b' else 'c' end as name from A where id = ?->SELECT CASE WHEN \"id\" = 1 THEN a WHEN \"id\" = 2 THEN b ELSE c END AS \"name\" FROM \"A\" WHERE \"id\" = @p1",
}

func TestSqlSelectCaseWhen(t *testing.T) {
	w := parser.Walker{
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Params {
				node.V = strings.Replace(node.V, "v", "@p", -1)
			}
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	for i, sql := range sqlSelectCaseWhen {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}
}

var sqlPostgresParseTest = []string{
	"select * from A where b=true->SELECT * FROM \"A\" WHERE \"b\" = TRUE",
	"select len(a) la, now() as n from A->SELECT length(\"a\") AS la, current_timestamp() AS n FROM \"A\"",
	"select * from A where d='2001-01-01'->SELECT * FROM \"A\" WHERE \"d\" = '2001-01-01'",
	"select * from A where d='2001-01-01 12:00:00'->SELECT * FROM \"A\" WHERE \"d\" = '2001-01-01 12:00:00'",
}

func TestPostgresParse(t *testing.T) {
	w := parser.Walker{
		Resolver: pgParser.PostgresResoler,
	}
	for i, sql := range sqlPostgresParseTest {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.Parse(sqlR, nil)
		if err != nil {
			panic(err)
		}
		assert.NoError(t, err)
		assert.Equal(t, sqlExpected, sqlP)
		if sqlP != sqlExpected {
			fmt.Println(fmt.Sprintf("[%d] %s->%s", i, sqlR, sqlP))
		}
	}
}

var testCreateDb = []string{
	"create database testdb->CREATE DATABASE \"testdb\" in db owner;CREATE EXTENSION IF NOT EXISTS citext in db testdb;",
}

func TestCreateDb(t *testing.T) {
	OnCreateDB := func(dbName string) (parser.DBDDLCmds, error) {
		ret := []*parser.DBDDLCommand{}

		sqlCreateDb := "CREATE DATABASE \"" + dbName + "\""
		ret = append(ret, &parser.DBDDLCommand{
			CommandText: &sqlCreateDb,
		})

		sqlEnableCiTextExtension := "CREATE EXTENSION IF NOT EXISTS citext"
		ret = append(ret, &parser.DBDDLCommand{
			CommandText: &sqlEnableCiTextExtension,
			DbName:      &dbName,
		})
		return ret, nil

	}
	w := parser.Walker{
		OnCreateDb: &OnCreateDB,
		Resolver: func(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
			if node.Nt == parser.Function {
				return node, nil
			}
			if node.Nt == parser.Field || node.Nt == parser.TableName || node.Nt == parser.Alias {
				node.V = "\"" + node.V + "\""
			}
			return node, nil
		},
	}
	for _, sql := range testCreateDb {
		sqlR := strings.Split(sql, "->")[0]
		sqlExpected := strings.Split(sql, "->")[1]
		sqlP, err := w.ParseDBDLL(sqlR)
		assert.NoError(t, err)
		assert.Equal(t, sqlP.String(), sqlExpected)

	}
}

type JobsProfile struct {
	ProfileId   int `db:"pk"`
	FromTime    time.Time
	ToTime      *time.Time
	Description string
	Point       float32 `db:"default:14.0"`
	OrderId     int32   `db:"df:auto"`
}
type BaseInfo struct {
	CreatedOn time.Time
	UpdatedOn time.Time
}
type Employee struct {
	BaseInfo

	Id        int    `db:"pk"`
	Name      string `db:"idx"`
	Code      string `db:"unique;varchar(10)"`
	Col1      *string
	Guild     uuid.UUID
	Guild1    *uuid.UUID
	TimeCol   time.Time
	TimeCol1  *time.Time
	ColIndex1 int            `db:"index:idx1"`
	ColIndex2 *time.Time     `db:"index:idx1"`
	COlUUID   *uuid.UUID     `db:"index:idx1"`
	Profile   []*JobsProfile `db:"foreignkey:ProfileId"`
	JobTime   time.Time
	StartTimr time.Time
	DeptId    *int
}
type Dept struct {
	BaseInfo
	Id   int    `db:"pk;df:auto"`
	Name string `db:"idx"`
	Code string `db:"unique;varchar(10)"`
	//Emps              []*Employee `db:"foreignkey:DeptId"`
	CreateOn          time.Time `db:"default:now()"`
	Description       string    `db:"df:''"`
	CreatedOn         time.Time `db:"default:now()"`
	UpdatedOn         *time.Time
	CreatedBy         string     `db:"default:'system';idx"`
	UpdatedBy         *string    `db:"idx"`
	EstablishmentDate time.Time  `db:"idx;df:now()"`
	DissolutionDate   *time.Time `db:"idx"`
	SecretKey         uuid.UUID  `db:"default:gen_random_uuid()"`
}

var expectValues = []string{
	"<nil>",
	"Name:Id ; IndexName: ; IsPrimary:true ; IsUnique:false ; IsIndex:false ; Len:-1 ; AllowNull:false ",
	"Name:Name ; IndexName:Name ; IsPrimary:false ; IsUnique:false ; IsIndex:true ; Len:-1 ; AllowNull:false ",
	"Name:Code ; IndexName: ; IsPrimary:false ; IsUnique:true ; IsIndex:false ; Len:10 ; AllowNull:false ",
	"Name:Col1 ; IndexName: ; IsPrimary:false ; IsUnique:false ; IsIndex:false ; Len:-1 ; AllowNull:true ",
	"Name:Guild ; IndexName: ; IsPrimary:false ; IsUnique:false ; IsIndex:false ; Len:-1 ; AllowNull:false ",
	"Name:Guild1 ; IndexName: ; IsPrimary:false ; IsUnique:false ; IsIndex:false ; Len:-1 ; AllowNull:true ",
	"Name:TimeCol ; IndexName: ; IsPrimary:false ; IsUnique:false ; IsIndex:false ; Len:-1 ; AllowNull:false ",
	"Name:TimeCol1 ; IndexName: ; IsPrimary:false ; IsUnique:false ; IsIndex:false ; Len:-1 ; AllowNull:true ",
}

func TestGetColuImfo(t *testing.T) {

	typ := reflect.TypeOf(Employee{})
	for i := 0; i < typ.NumField(); i++ {
		col := parser.GetColInfo(typ.Field(i))
		val := col.String()
		if i < len(expectValues) {
			if expectValues[i] != val {
				fmt.Println("[", i, "] ", val)
			}
			assert.Equal(t, expectValues[i], val)
		} else {
			fmt.Println("[", i, "] ", val)
		}

	}
}
func TestGetTableInfoByType(t *testing.T) {
	tbl, err := parser.GetTableInfoByType(reflect.TypeOf(Employee{}))
	assert.NoError(t, err)
	print(tbl)

}
func TestGetTableInfoOfEntity(t *testing.T) {
	tbl, err := parser.GetTableInfo(&Employee{})
	assert.NoError(t, err)
	print(tbl)

}
func TestCreateSQL(t *testing.T) {
	tbl, err := parser.GetTableInfo(&Dept{})
	assert.NoError(t, err)
	sqls := tbl.GetSql("postgres")
	assert.NoError(t, err)
	db, err := sql.Open("postgres", "postgres://postgres:123456@localhost:5432/testdb?sslmode=disable")
	assert.NoError(t, err)
	for _, sqlE := range sqls {
		_, err := db.Exec(sqlE)

		if err != nil && !strings.Contains(err.Error(), "already exists") {
			assert.NoError(t, err)
			println(err.Error())
		}
		println(sqlE)

	}

}
func TestDbContext(t *testing.T) {
	dbContext := parser.NewDbContext(parser.DbCfg{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		UseSSL:   false,
	})
	dbContext.Open()
	err := dbContext.Ping()
	assert.NoError(t, err)
	dbContext.Close()
	tanetDb, err := dbContext.CreateCtx("db_001")
	assert.NoError(t, err)
	start := time.Now()
	tanetDb.Open()
	// err = tanetDb.Insert(&Dept{
	// 	Name:      "test",
	// 	Code:      "test2ee",
	// 	CreatedBy: "admin",
	// })
	n := time.Since(start).Abs().Milliseconds()
	fmt.Println(fmt.Sprintf("elapse time: %04d", n))

	for i := 70000; i < 80000; i++ {
		start = time.Now()
		dd := time.Now().Add(time.Hour * 24)
		dep := Dept{
			Name:              "test",
			Code:              "test" + fmt.Sprintf("%04d", i),
			CreatedBy:         "admin",
			EstablishmentDate: time.Now(),
			DissolutionDate:   &dd,
		}
		err = tanetDb.Insert(&dep)

		n := time.Since(start).Abs().Milliseconds()
		if err != nil {
			fmt.Println(fmt.Sprintf("elapse time: %04d %s", n, err))
		} else {
			fmt.Println(fmt.Sprintf("elapse time: %04d", n))
		}

	}

	assert.NoError(t, err)
	tanetDb.Close()

}
