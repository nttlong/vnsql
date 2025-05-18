package dbx_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nttlong/vnsql/compiler"
	_ "github.com/nttlong/vnsql/compiler"
	"github.com/nttlong/vnsql/dbx"
	_ "github.com/nttlong/vnsql/dbx"
	"github.com/nttlong/vnsql/entities_example"
	_ "github.com/nttlong/vnsql/entities_example"
	"github.com/stretchr/testify/assert"
)

var cfg dbx.DbCfg
var Dbx dbx.DBX

func TestDbx(t *testing.T) {
	cfg = dbx.DbCfg{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		UseSSL:   false,
	}
	expectedDns := "postgres://postgres:123456@localhost:5432/postgres?sslmode=disable"
	assert.Equal(t, expectedDns, cfg.GetDns("postgres"))
	expectedDnsWithDbName := "postgres://postgres:123456@localhost:5432/test?sslmode=disable"
	assert.Equal(t, expectedDnsWithDbName, cfg.GetDns("test"))
	Dbx = dbx.NewDBX(&cfg)
	Dbx.Open()
	defer Dbx.Close()

	err := Dbx.Ping()
	assert.NoError(t, err)
	dbx.RegisterEntities(&entities_example.Departments{}, &entities_example.Employees{})
}
func TestCompiler(t *testing.T) {
	TestDbx(t)
	cpl := compiler.Compiler{}
	Dbx.Open()
	defer Dbx.Close()
	err := cpl.LoadDbDictionary(Dbx.DB)
	assert.NoError(t, err)
}
func TestExecutor(t *testing.T) {
	TestCompiler(t)
	Dbx.Open()
	defer Dbx.Close()
	Dbx.Executor.GetTableInfoFormDb(Dbx.DB, "test")

}
func TestMigrate(t *testing.T) {
	TestExecutor(t)
	dbTenant, err := Dbx.GetTenant("test001")
	if err != nil {
		t.Error(err)
	}
	assert.NoError(t, err)
	dbTenant.Open()
	defer dbTenant.Close()
	rs, err := dbTenant.Exec("select * from employees")
	assert.NoError(t, err)
	row, err := rs.RowsAffected()
	assert.NoError(t, err)
	assert.True(t, row >= 0)
	_, err = dbTenant.Exec("select * from employee")
	assert.Error(t, err)
	// dbTenant.Migrate(&entities_example.Departments{})
}
func TestInsert(t *testing.T) {
	TestMigrate(t)
	dbTenant, err := Dbx.GetTenant("test001")
	assert.NoError(t, err)
	dbTenant.Open()
	defer dbTenant.Close()
	rs, err := dbTenant.Exec("delete from departments")
	assert.NoError(t, err)
	rc, err := rs.RowsAffected()
	assert.NoError(t, err)
	assert.True(t, rc >= 0)
	avg := int64(0)
	for i := 10; i < 100000; i++ {
		start := time.Now()
		des := "test"
		dep := entities_example.Departments{
			Code:        "dep" + fmt.Sprintf("%04d", i),
			Name:        "dep" + fmt.Sprintf("%04d", i),
			CreatedBy:   "test",
			CreatedOn:   time.Now(),
			Description: &des,
		}
		err := dbTenant.Insert(&dep)
		t.Log("insert", i, "err", err)
		cost := time.Since(start).Milliseconds()
		t.Log("insert", i, "cost", cost)
		fmt.Println("insert", i, "err", err, "cost", cost)
		avg += cost
	}
	fmt.Println("avg", avg/100000)
}
