package testerxdb_test

import (
	"testing"

	"github.com/nttlong/vnsql/internal/xdb"
	"github.com/nttlong/vnsql/internal/xdb/isql"
	"github.com/stretchr/testify/assert"
)

type Users struct {
	Id   int    `db:"pk"`
	Name string `db:"varchar(50)"`
}

func Setup() isql.ISqlStore {
	ret, err := xdb.NewSql("postgres")
	if err != nil {
		panic(err)
	}
	ret.SetConfig(isql.DbCfg{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
	})
	return ret.GetSqlStore("testdb")

}
func TestPin(t *testing.T) {
	st := Setup()
	ctx, err := st.GetCtx()
	assert.NoError(t, err)
	r, e := ctx.Exec("Select len('abc')")
	if e != nil {
		t.Error(e)
	}

	t.Log(r)
	r1, e := ctx.Query("Select len('abc')")

	re := 0
	if r1.Next() { // Gọi Next() để di chuyển đến hàng đầu tiên (và duy nhất)
		e = r1.Scan(&re)
		if e != nil {
			t.Error(e)
		}
	}

	if e != nil {
		t.Error(e)
	}
	assert.NoError(t, err)
	assert.Equal(t, 3, re)

}
func TestEnity(t *testing.T) {
	st := Setup()
	ctx, err := st.GetCtx()
	assert.NoError(t, err)
	var usesr []Users = []Users{}
	ctx.Selector(&usesr)

}
