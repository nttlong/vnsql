package pg_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nttlong/vnsql/pg/internal/utils"
	"github.com/nttlong/vnsql/pg/internal/utils/pg"
	"github.com/stretchr/testify/assert"
)

type BaseModel struct {
	Id       string     `db:"pk;text(36)"`
	Created  time.Time  `db:"index;"`
	Modified *time.Time `db:"index"`
}

type Emp struct {
	BaseModel
	Id       uuid.UUID `db:"pk;text(36);primary_key"`
	Name     string    `db:"unique;text(50)"`
	Birthday time.Time
}
type Dept struct {
	BaseModel
	Id          int    `db:"pk;int"`
	Name        string `db:"text(50)"`
	Description string
	DisposeOn   *time.Time
	Director    string `db:"text(50);default('admin')"`
	Code        string `db:"text(50);unique:dept_code_idx"`
	Col1        string `db:"text(50);unique:dept_code_idx"`
	Col2        string `db:"text(50);unique:dept_code_idx"`
	Col3        string `db:"text(50);unique:dept_code_idx"`
}

func TestConnect(t *testing.T) {

}
func OnResolveCol(col *utils.ColInfo) error {
	dbMapType := utils.GoTypeToPOSTGRESType
	// get the type of the field
	fieldType := col.Typ
	col.DbType = dbMapType[fieldType]
	return nil

}
func TestGetTableInfoByType(t *testing.T) {

	info, err := utils.GetTableInfoByType(
		"postgres", reflect.TypeOf(Emp{}), func(col *utils.ColInfo) error {
			dbMapType := utils.GoTypeToPOSTGRESType
			// get the type of the field
			fieldType := col.Typ
			col.DbType = dbMapType[fieldType]
			return nil

		})
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, "Emp", info.Name)
	assert.Equal(t, 5, len(info.Columns))
	t.Log(info.Name)
	t.Log(info.Columns[0].Name)
	t.Log(info.Columns[1].Name)
	t.Log(info.Columns[2].Name)
	t.Log(info.Columns[3].Name)

}
func TestSQlCreateTablePg(t *testing.T) {
	info, err := utils.GetTableInfoByType(
		"postgres", reflect.TypeOf(Emp{}), func(col *utils.ColInfo) error {
			dbMapType := utils.GoTypeToPOSTGRESType
			// get the type of the field
			fieldType := col.Typ
			col.DbType = dbMapType[fieldType]
			return nil

		})
	if err != nil {
		t.Error(err)
	}
	sql := pg.GenratePostgresCreateTableIfNotExists(*info)
	fmt.Print(sql)
}
func TestExecSQlCommand(t *testing.T) {

	cfg := pg.DbCfg{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		NoSSL:    true,
	}
	err := cfg.ExecSQl("SELECT 1")
	if err != nil {
		t.Error(err)
	}
	err = cfg.CreatDbIfNotExist("test0001")
	if err != nil {
		t.Error(err)
	}

	tblInfo, err := cfg.GetTableInfo("test0001", "Emp")
	if err != nil {
		t.Error(err)
	}
	info, err := utils.GetTableInfoByType(
		"postgres", reflect.TypeOf(Dept{}), func(col *utils.ColInfo) error {
			dbMapType := utils.GoTypeToPOSTGRESType
			// get the type of the field
			fieldType := col.Typ
			col.DbType = dbMapType[fieldType]
			return nil

		})
	if err != nil {
		t.Error(err)
	}
	sql := pg.GenratePostgresCreateTableIfNotExists(*info)
	fmt.Print(sql)
	err = cfg.ExecSqlWithDbName("test0001", []string{sql})
	if err != nil {
		t.Error(err)
	}

	t.Log(tblInfo)
	err = cfg.DoMigration("test0001", &Dept{})
	if err != nil {
		t.Error(err)
	}
	t.Log("ok")
	idx, err := cfg.GetAllIndex("test0001", "Dept")
	if err != nil {
		t.Error(err)
	}
	t.Log(idx)
	idx2 := utils.GetAllIndexInColsInfo(info.Columns)
	t.Log(idx2)

}
