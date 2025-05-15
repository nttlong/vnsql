package types

import (
	"database/sql"
	"reflect"
	"time"

	"github.com/google/uuid"
)

type IExecutor interface {
	CreateInsertCommand(entity interface{}, tableInfo TableInfo) (*SqlWithParams, error)
	CreatePosgresDbIfNotExist(ctx *sql.DB, dbName string, tenantDns string) error
}
type TableInfo struct {
	TableName     string
	ColInfos      []ColInfo
	Relationship  []*RelationshipInfo
	MapCols       map[string]*ColInfo
	AutoValueCols map[string]*ColInfo
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
}
type SqlWithParams struct {
	Sql    string
	Params []interface{}
}

var MapDefaulValueOfGoType = map[reflect.Type]interface{}{
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
