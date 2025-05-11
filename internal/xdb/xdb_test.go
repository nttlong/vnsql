package xdb_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/nttlong/vnsql/internal/xdb"
	_ "github.com/nttlong/vnsql/internal/xdb"
	"github.com/nttlong/vnsql/internal/xdb/common"
	"github.com/nttlong/vnsql/internal/xdb/isql"
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
