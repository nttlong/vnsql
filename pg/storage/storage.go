package storage

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nttlong/vnsql"
)

type Migrate struct {
	dbTableInfo DbTableInfo
}
type DbTableInfo struct {
	tblInfo vnsql.TableInfo
}
type Storage struct {
	connector vnsql.IConnector
	migrator  vnsql.IMigrate
	dbName    string
}

/*
*
SetTableInfo(table *TableInfo)

	GetTableInfo() *TableInfo
*/
func (info *DbTableInfo) SetTableInfo(table vnsql.TableInfo) {
	info.tblInfo = table

}
func (info *DbTableInfo) GetTableInfo() vnsql.TableInfo {
	return info.tblInfo

}
func (info *DbTableInfo) GetGenerateSql() string {
	return getGenerateSql(info)
}
func (s *Storage) SetConnector(conn vnsql.IConnector) {
	s.connector = conn

}
func (s *Storage) SetMigrate(migrate vnsql.IMigrate) {
	s.migrator = migrate
}
func (s *Storage) GetMigrate() vnsql.IMigrate {
	return s.migrator
}
func (s *Storage) Connect() error {
	panic("implement me")
}

func (s *Storage) GetConnector() vnsql.IConnector {
	return s.connector

}

func (s *Storage) GetDbName() string {
	return s.dbName

}

var cachDbStorage = sync.Map{}

func (m *Migrate) GetDbColInfo(fldType reflect.Type) (*vnsql.ColInfo, error) {
	pgType, ok := mapGoTypeToPgDbType[fldType]
	if !ok {
		return nil, fmt.Errorf("unsupported type %s", fldType)
	}
	return &vnsql.ColInfo{
		Typ:        fldType,
		TypeName:   pgType,
		IndexName:  "",
		IsPk:       false,
		IsUnique:   false,
		IsNullable: true,
		DbType:     pgType,
	}, nil

}

func NewStorage(dbName string) vnsql.IStorage {

	if v, ok := cachDbStorage.Load(dbName); ok {
		return v.(*Storage)
	}
	s := &Storage{
		migrator: &Migrate{},
		dbName:   dbName,
	}

	cachDbStorage.Store(dbName, s)
	return s

}

// ======================================================
var mapGoTypeToPgDbType = map[reflect.Type]string{
	reflect.TypeOf(int(0)):           "integer",
	reflect.TypeOf(int8(0)):          "smallint",
	reflect.TypeOf(int16(0)):         "smallint",
	reflect.TypeOf(int32(0)):         "integer",
	reflect.TypeOf(int64(0)):         "bigint",
	reflect.TypeOf(uint(0)):          "integer",
	reflect.TypeOf(uint8(0)):         "smallint",
	reflect.TypeOf(uint16(0)):        "integer",
	reflect.TypeOf(uint32(0)):        "bigint",
	reflect.TypeOf(uint64(0)):        "numeric",
	reflect.TypeOf(float32(0)):       "real",
	reflect.TypeOf(float64(0)):       "double precision",
	reflect.TypeOf(string("")):       "citext",
	reflect.TypeOf(bool(false)):      "boolean",
	reflect.TypeOf([]byte(nil)):      "bytea",
	reflect.TypeOf(time.Time{}):      "timestamp with time zone",
	reflect.TypeOf(time.Duration(0)): "interval",
	reflect.TypeOf(uuid.UUID{}):      "uuid",
}

func getGenerateSql(info *DbTableInfo) string {

	sql := "CREATE TABLE IF NOT EXISTS \"" + info.tblInfo.Name + "\" (\n"
	type orderColums struct {
		index   int
		name    string
		colInfo vnsql.ColInfo
	}
	// sort info.tblInfo.Columns by index to orderColums sort byt info.tblInfo.Columns.Order
	orderCols := make([]orderColums, 0)
	for colName, colInfo := range info.tblInfo.Columns {
		orderCols = append(orderCols, orderColums{
			index:   colInfo.Order,
			name:    colName,
			colInfo: colInfo,
		})
	}
	sort.Slice(orderCols, func(i, j int) bool {
		return orderCols[i].index < orderCols[j].index
	})

	for _, c := range orderCols {
		colInfo := c.colInfo
		colInfo.DbType = getDbTypeColunm(colInfo.Typ)

		colInfo.DbType = getDbTypeColunm(colInfo.Typ)

		sql += fmt.Sprintf("    \"%s\" %s", c.name, colInfo.DbType)
		if colInfo.IsPk {
			sql += " PRIMARY KEY"
		}
		if colInfo.IsUnique {
			sql += " UNIQUE"
		}
		if !colInfo.IsNullable {
			sql += " NOT NULL"
		}
		sql += ",\n"
	}
	sql = sql[:len(sql)-2] + "\n);"
	return sql
}
