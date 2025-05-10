package storage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nttlong/vnsql"
	"github.com/nttlong/vnsql/pg/storage"
	_ "github.com/nttlong/vnsql/pg/storage"
	"github.com/stretchr/testify/assert"
)

type BaseStruct struct {
	ID        int64     `db:"pk"`
	CreatedAt time.Time `db:"auto_now_add"`
	UpdatedAt time.Time `db:"auto_now"`
}
type Test002 struct {
	BaseStruct
	ID        uuid.UUID `db:"pk"`
	StringCol string    `db:"len:50"`
	IntCol    int
	BoolCol   bool
	FloatCol  float64
	TimeCol   time.Time
	UUIDCol   uuid.UUID
	BytesCol  []byte
	Int16Col  int16
	Int32Col  int32
	Int64Col  int64
	UintCol   uint
	Uint16Col uint16
	Uint32Col uint32
	Uint64Col uint64
}

func TestMain(t *testing.T) {
	st := storage.NewStorage("test")
	h := vnsql.NewHelper()
	tbl := h.GetTableInfo(&Test002{})
	m := st.GetMigrate()

	for k, v := range tbl.Columns {
		col, err := m.GetDbColInfo(v.Typ)
		assert.Empty(t, err)
		fmt.Println(k, col.DbType)
	}
	dbTableIndo := m.GetDbTableInfo(&Test002{})
	sql := dbTableIndo.GetGenerateSql()
	fmt.Print(sql)

	assert.NotEmpty(t, dbTableIndo)

}
