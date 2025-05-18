package types

import (
	"database/sql"
	"reflect"
	"time"

	"github.com/google/uuid"
)

type IExecutor interface {
	CreateInsertCommand(entity interface{}, tableInfo TableInfo) (*SqlWithParams, error)
	CreatePostgresDbIfNotExist(ctx *sql.DB, dbName string, tenantDns string) error
	GetTableInfoFormDb(ctx *sql.DB, dbName string) (*TableMapping, error)
	CreateSqlMigrate(table TableInfo) []string
}

type TableInfo struct {
	TableName              string
	ColInfos               []ColInfo
	Relationship           []*RelationshipInfo
	MapCols                map[string]*ColInfo
	AutoValueCols          map[string]*ColInfo
	EntityType             reflect.Type
	AutoValueColsName      []string
	IsHasAutoValueColsName bool
}
type RelationshipInfo struct {
	FromTable TableInfo
	ToTable   TableInfo
	FromCols  []ColInfo
	ToCols    []ColInfo
}
type ColInfo struct {
	Name          string
	FieldType     reflect.StructField
	Tag           string
	IndexName     string
	IsPrimary     bool
	IsUnique      bool
	IsIndex       bool
	Len           int
	AllowNull     bool
	DefaultValue  string
	IndexOnStruct int
	FieldSt       reflect.StructField
}
type SqlWithParams struct {
	Sql    string
	Params []interface{}
}
type DbTbaleInfo struct {
	TableName  string
	ColInfos   map[string]string
	EntityType reflect.Type
}
type TableMapping map[string]DbTbaleInfo

func (t *TableMapping) String() string {
	if t == nil {
		return "nil"
	}
	ret := ""
	for k, v := range *t {
		ret += k + " : " + v.TableName + "\n"
		for k1, v1 := range v.ColInfos {
			ret += "\t" + k1 + " : " + v1 + "\n"
		}
	}
	return ret

}

var MapDefaultValueOfGoType = map[reflect.Type]interface{}{
	reflect.TypeOf(int(0)):      0,
	reflect.TypeOf(int8(0)):     0,
	reflect.TypeOf(int16(0)):    0,
	reflect.TypeOf(int32(0)):    0,
	reflect.TypeOf(int64(0)):    0,
	reflect.TypeOf(uint(0)):     0,
	reflect.TypeOf(uint8(0)):    0,
	reflect.TypeOf(uint16(0)):   0,
	reflect.TypeOf(uint32(0)):   0,
	reflect.TypeOf(uint64(0)):   0,
	reflect.TypeOf(float32(0)):  0,
	reflect.TypeOf(float64(0)):  0,
	reflect.TypeOf(bool(false)): false,
	reflect.TypeOf(string("")):  "",
	reflect.TypeOf(time.Time{}): time.Time{},
	reflect.TypeOf(uuid.UUID{}): uuid.UUID{},
}
