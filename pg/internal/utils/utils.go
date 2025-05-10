package utils

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ColInfo struct {
	Typ        reflect.Type
	TypeName   string
	IndexName  string
	IsPk       bool
	IsUnique   bool
	IsNullable bool
	DbType     string
	Order      int
	Name       string
	DefaultVal string
}
type TableInfo struct {
	Name    string
	Columns []ColInfo
}

var tableInfoCache sync.Map

func GetTableInfoByType(dbType string, typ reflect.Type, onResolveCol func(col *ColInfo) error) (*TableInfo, error) {
	key := dbType + ":" + typ.String()
	table := TableInfo{
		Name:    typ.Name(),
		Columns: []ColInfo{},
	}
	if v, ok := tableInfoCache.Load(key); ok {
		return v.(*TableInfo), nil
	}
	mapCols, err := getTableInfoByType(dbType, typ, onResolveCol)
	if err != nil {
		return nil, err
	}
	table.Columns = SortColumns(*mapCols)
	tableInfoCache.Store(key, table)
	return &table, nil

}
func GetTableInfo(dbType string, entity interface{}, onResolveCol func(col *ColInfo) error) (*TableInfo, error) {
	typ := reflect.TypeOf(entity)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("entity must be a struct or a pointer to a struct, type: %s", typ.String())
	}
	cols, err := getTableInfoByType(dbType, typ, onResolveCol)
	if err != nil {
		return nil, err
	}
	return &TableInfo{
		Name:    typ.Name(),
		Columns: SortColumns(*cols),
	}, nil

}

// ==============================
var dbTypeTagKeWords = map[string][]string{
	"df":  {"default", "default_value"},
	"pk":  {"primary_key", "primarykey", "primary_key_index", "primarykey_index", "pk_index", "pkindex", "primary_key_idx", "primarykey_idx", "pkidx"},
	"uk":  {"unique_index", "uniquekey", "unique_key", "uniquekey_index", "unique_key_index", "uk_index", "ukindex", "unique_key_idx", "uniquekey_idx", "ukidx"},
	"idx": {"index", "key", "key_index", "keyindex", "idx_index", "idxindex", "key_idx", "keyidx", "idxidx"},
}

func replaceTag(tag string) string {
	tag = strings.ToLower(tag)
	tag = strings.ReplaceAll(tag, " ", "")
	tag = ";" + tag + ";"
	for k, v := range dbTypeTagKeWords {
		for _, word := range v {
			if k != "df" {
				tag = strings.ReplaceAll(tag, ";"+word+";", ";"+k+";")
				tag = strings.ReplaceAll(tag, ";"+word+":", ";"+k+":")
			} else {

				tag = strings.ReplaceAll(tag, ";"+word+"(", ";"+k+"(")
			}

		}
	}
	return tag
}
func getTableInfoByType(dbType string, typ reflect.Type, onResolveCol func(col *ColInfo) error) (*map[string]ColInfo, error) {
	ret := make(map[string]ColInfo)
	var order = 0
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		ft := field.Type
		if ft.Kind() == reflect.Struct {
			if ft == reflect.TypeOf(time.Time{}) {
				continue
			}
			if (ft == reflect.TypeOf(uuid.UUID{})) {
				continue
			}
			subCols, err := getTableInfoByType(dbType, ft, onResolveCol)
			if err != nil {
				return nil, err
			}
			for _, subCol := range *subCols {

				subCol.Order = order
				order++
				ret[subCol.Name] = subCol
			}

		}
	}
	for i := 0; i < typ.NumField(); i++ {

		field := typ.Field(i)
		ft := field.Type
		if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(time.Time{}) && (ft != reflect.TypeOf(uuid.UUID{})) {
			continue

		}
		isNullable := false
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
			isNullable = true
		}

		tags := replaceTag(field.Tag.Get("db"))
		isPk := strings.Contains(tags, ";pk;") || strings.Contains(tags, ";pk:")
		isHasIndex := strings.Contains(tags, ";idx;") || strings.Contains(tags, ";idx:")
		hasDefault := strings.Contains(tags, ";df(")
		defaultVal := ""
		if hasDefault {
			defaultVal = strings.Split(tags, ";df(")[1]
			defaultVal = strings.Split(defaultVal, ")")[0]
		}

		isUnique := strings.Contains(tags, ";uk;") ||
			strings.Contains(tags, ";uk:")
		indexName := ""
		if isHasIndex {
			if strings.Contains(tags, ";idx:") {
				indexName = strings.Split(tags, ";idx:")[1]
				indexName = strings.Split(indexName, ";")[0]
			} else {
				indexName = field.Name + "_idx"
			}

		}
		dbDataType, err := GetDbMapType(dbType, ft)
		if err != nil {
			return nil, err
		}

		col := ColInfo{

			Typ:        ft,
			IndexName:  indexName,
			IsUnique:   isUnique,
			IsPk:       isPk,
			TypeName:   ft.Name(),
			Name:       field.Name,
			DefaultVal: defaultVal,

			IsNullable: isNullable,
			DbType:     *dbDataType,
			Order:      order,
		}
		err = onResolveCol(&col)
		if err != nil {
			return nil, err
		}
		order++

		ret[field.Name] = col
	}

	return &ret, nil
}

var GoTypeToPOSTGRESType = map[reflect.Type]string{
	reflect.TypeOf(int(0)):      "integer",
	reflect.TypeOf(int8(0)):     "smallint",
	reflect.TypeOf(int16(0)):    "smallint",
	reflect.TypeOf(int32(0)):    "integer",
	reflect.TypeOf(int64(0)):    "bigint",
	reflect.TypeOf(uint(0)):     "integer",
	reflect.TypeOf(uint8(0)):    "smallint",
	reflect.TypeOf(uint16(0)):   "smallint",
	reflect.TypeOf(uint32(0)):   "integer",
	reflect.TypeOf(uint64(0)):   "bigint",
	reflect.TypeOf(float32(0)):  "real",
	reflect.TypeOf(float64(0)):  "double precision",
	reflect.TypeOf(bool(false)): "boolean",
	reflect.TypeOf(string("")):  "citext",
	reflect.TypeOf(time.Time{}): "timestamp with time zone",
	reflect.TypeOf(uuid.UUID{}): "uuid",
}
var GoTypeToMySQLType = map[reflect.Type]string{
	reflect.TypeOf(int(0)):      "int",
	reflect.TypeOf(int8(0)):     "tinyint",
	reflect.TypeOf(int16(0)):    "smallint",
	reflect.TypeOf(int32(0)):    "int",
	reflect.TypeOf(int64(0)):    "bigint",
	reflect.TypeOf(uint(0)):     "int",
	reflect.TypeOf(uint8(0)):    "tinyint",
	reflect.TypeOf(uint16(0)):   "smallint",
	reflect.TypeOf(uint32(0)):   "int",
	reflect.TypeOf(uint64(0)):   "bigint",
	reflect.TypeOf(float32(0)):  "float",
	reflect.TypeOf(float64(0)):  "double",
	reflect.TypeOf(bool(false)): "boolean",
	reflect.TypeOf(string("")):  "varchar",
	reflect.TypeOf(time.Time{}): "datetime",
	reflect.TypeOf(uuid.UUID{}): "char(36)",
}
var GoTypeToMssqlType = map[reflect.Type]string{
	reflect.TypeOf(int(0)):      "int",
	reflect.TypeOf(int8(0)):     "tinyint",
	reflect.TypeOf(int16(0)):    "smallint",
	reflect.TypeOf(int32(0)):    "int",
	reflect.TypeOf(int64(0)):    "bigint",
	reflect.TypeOf(uint(0)):     "int",
	reflect.TypeOf(uint8(0)):    "tinyint",
	reflect.TypeOf(uint16(0)):   "smallint",
	reflect.TypeOf(uint32(0)):   "int",
	reflect.TypeOf(uint64(0)):   "bigint",
	reflect.TypeOf(float32(0)):  "float",
	reflect.TypeOf(float64(0)):  "float",
	reflect.TypeOf(bool(false)): "bit",
	reflect.TypeOf(string("")):  "nvarchar",
	reflect.TypeOf(time.Time{}): "datetime2",
	reflect.TypeOf(uuid.UUID{}): "uniqueidentifier",
}
var DbMapTypes = map[string]map[reflect.Type]string{
	"postgres": GoTypeToPOSTGRESType,
	"mysql":    GoTypeToMySQLType,
	"mssql":    GoTypeToMssqlType,
}

func GetDbMapType(dbType string, typ reflect.Type) (*string, error) {
	if _, ok := DbMapTypes[dbType][typ]; !ok {
		return nil, fmt.Errorf("%s %s not support", dbType, typ.String())
	}
	if _, ok := DbMapTypes[dbType][typ]; !ok {
		return nil, fmt.Errorf("%s %s not support", dbType, typ.String())
	}
	ret := DbMapTypes[dbType][typ]
	return &ret, nil
}
func SortColumns(cols map[string]ColInfo) []ColInfo {
	var ret []ColInfo
	for _, col := range cols {

		ret = append(ret, col)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Order < ret[j].Order
	})
	return ret
}
