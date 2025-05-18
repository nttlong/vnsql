package info

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nttlong/vnsql/types"
	_ "github.com/nttlong/vnsql/types"
	_ "github.com/nttlong/vnsql/types/info/info_postgres"
	"google.golang.org/genproto/googleapis/type/decimal"
)

type ColInfo types.ColInfo
type TableInfo struct {
	types.TableInfo
}

//type RelationshipInfo types.RelationshipInfo

func (c *ColInfo) String() string {
	if c == nil {
		return "<nil>"
	}
	typ := reflect.TypeOf(c).Elem()
	ret := []string{}
	for i := 0; i < typ.NumField(); i++ {

		field := typ.Field(i)
		if field.Name == "FieldType" {
			continue
		}
		if field.Name == "Tag" {
			continue
		}
		val := reflect.ValueOf(c).Elem().Field(i).Interface()
		ret = append(ret, fmt.Sprintf("%s:%v ", field.Name, val))

	}
	return strings.Join(ret, "; ")
}
func GetColInfo(field reflect.StructField) *types.ColInfo {
	isNull := false
	isDbField := true
	if field.Type.Kind() == reflect.Slice {
		return nil

	}
	if field.Type.Kind() == reflect.Struct && !hashCheckIsDbFieldAble[field.Type] {
		tagOdStruct := field.Tag.Get("db")
		if tagOdStruct == "" {
			return nil
		}

	}

	if field.Type.Kind() == reflect.Ptr {
		// field.Type = field.Type.Elem()
		isNull = true
		if hashCheckIsDbFieldAble[field.Type.Elem()] {
			isDbField = true
		} else {
			isDbField = false
		}
	}

	tag := field.Tag.Get("db")
	if tag == "" && !isDbField {
		return nil
	}

	strTags := strings.ToLower(";" + tag + ";")

	for k, v := range replacerConstraint {
		for _, t := range v {
			strTags = strings.ReplaceAll(strTags, ";"+t+";", ";"+k+";")
			strTags = strings.ReplaceAll(strTags, ";"+t+":", ";"+k+":")
			strTags = strings.ReplaceAll(strTags, ";"+t+"(", ";"+k+"(")

		}

	}
	if strings.Contains(strTags, "fk:") && !isDbField {
		return nil
	}
	ret := types.ColInfo{
		Name:      field.Name,
		FieldType: field,
		Tag:       strTags,
		Len:       -1,
		AllowNull: isNull,
		FieldSt:   field,
	}
	tgs := strings.Split(strTags, ";")
	for _, tg := range tgs {
		if tg == "" {
			continue
		}
		if tg == "pk" {
			ret.IsPrimary = true
		}
		if strings.HasPrefix(tg, "uk") {
			ret.IsUnique = true
			if strings.Contains(tg, ":") {
				ret.IndexName = tg[3:]
			}
		}
		if strings.HasPrefix(tg, "idx") {
			ret.IsIndex = true
			if strings.Contains(tg, ":") {
				ret.IndexName = tg[4:]
			} else {
				ret.IndexName = field.Name
			}

		}
		if strings.HasPrefix(tg, "fk") {
			ret.IsIndex = true
			if strings.Contains(tg, ":") {
				ret.IndexName = tg[3:]
			}
		}
		if strings.HasPrefix(tg, "text") {
			if strings.Contains(tg, "(") && strings.Contains(tg, ")") {
				ret.FieldType.Type = reflect.TypeOf(string(""))
				strLen := strings.Split(tg, "(")[1]
				strLen = strings.Split(strLen, ")")[0]
				lenV, err := strconv.Atoi(strLen)
				if err != nil {
					panic(fmt.Errorf("invalid text length %s in tag %s", strLen, field.Tag.Get("db")))
				}

				ret.Len = lenV
			}
		}
		if strings.HasPrefix(tg, "size") {
			if strings.Contains(tg, ":") {
				strLen := strings.Split(tg, ":")[1]
				lenV, err := strconv.Atoi(strLen)
				if err != nil {
					panic(fmt.Errorf("invalid size length %s in tag %s", strLen, field.Tag.Get("db")))
				}

				ret.Len = lenV
			} else {
				panic(fmt.Errorf("invalid length %s", field.Tag.Get("db")))
			}
		}
		if strings.HasPrefix(tg, ";auto;") {
			ret.DefaultValue = "auto"
		}

		if strings.HasPrefix(tg, "df:") {
			if strings.Contains(tg, ":") {
				dfValue := strings.Split(tg, ":")[1]
				dfValue = strings.Split(dfValue, ";")[0]
				ret.DefaultValue = dfValue
			} else {
				panic(fmt.Errorf("invalid default value %s", field.Tag.Get("db")))

			}

		}

	}
	return &ret
}
func GetTableInfoByType(typ reflect.Type) (*types.TableInfo, error) {
	if v, ok := cachedTableInfo.Load(typ); ok {
		return v.(*types.TableInfo), nil
	}
	table, err := getTableInfoByType(typ)
	if err != nil {
		return nil, err
	}
	cachedTableInfo.Store(typ, table)
	return table, nil
}
func GetTableInfo(obj interface{}) (*types.TableInfo, error) {

	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Slice {
		typ = reflect.SliceOf(typ)

	}
	if typ.Kind() != reflect.Array {
		typ = typ.Elem()

	}
	return GetTableInfoByType(typ)
}

// func (c *TableInfo) GetSql(dbDriver string) []string {
// 	switch dbDriver {
// 	// case "mysql":
// 	// 	return getSqlOfTableInfoForMysql(&c)
// 	case "postgres":

// 		return info_postgres.GetSqlOfTableInfoForPostgres(c.TableInfo, nil)
// 	// case "sqlite3":
// 	// 	return getSqlOfTableInfoForSqlite3(&c)
// 	default:
// 		panic(fmt.Errorf("not support db driver %s", dbDriver))
// 	}
// }

// private var

var replacerConstraint = map[string][]string{
	"pk":   {"primary_key", "primarykey", "primary", "primary_key_constraint"},
	"fk":   {"foreign_key", "foreignkey", "foreign", "foreign_key_constraint"},
	"uk":   {"unique", "unique_key", "uniquekey", "unique_key_constraint"},
	"idx":  {"index", "index_key", "indexkey", "index_constraint"},
	"text": {"vachar", "varchar", "varchar2"},
	"size": {"length", "len"},
	"df":   {"default", "default_value", "default_value_constraint"},
	"auto": {"auto_increment", "autoincrement", "serial_key", "serialkey", "serial_key_constraint"},
}
var hashCheckIsDbFieldAble = map[reflect.Type]bool{
	reflect.TypeOf(int(0)):      true,
	reflect.TypeOf(int8(0)):     true,
	reflect.TypeOf(int16(0)):    true,
	reflect.TypeOf(int32(0)):    true,
	reflect.TypeOf(int64(0)):    true,
	reflect.TypeOf(uint(0)):     true,
	reflect.TypeOf(uint8(0)):    true,
	reflect.TypeOf(uint16(0)):   true,
	reflect.TypeOf(uint32(0)):   true,
	reflect.TypeOf(uint64(0)):   true,
	reflect.TypeOf(float32(0)):  true,
	reflect.TypeOf(float64(0)):  true,
	reflect.TypeOf(string("")):  true,
	reflect.TypeOf(bool(false)): true,
	reflect.TypeOf(time.Time{}): true,

	reflect.TypeOf(decimal.Decimal{}): true,
	reflect.TypeOf(uuid.UUID{}):       true,
}
var cachedTableInfo sync.Map

func getTableInfoByType(typ reflect.Type) (*types.TableInfo, error) {

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid type %s", typ.Name())
	}
	table := types.TableInfo{
		TableName:    typ.Name(),
		ColInfos:     []types.ColInfo{},
		Relationship: []*types.RelationshipInfo{},
	}
	remainFields := []reflect.StructField{}
	colsProc := map[string]int{} // map colum name to index in colInfos
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			embededTable, err := getTableInfoByType(field.Type)
			if err != nil {
				return nil, err
			}
			table.ColInfos = append(table.ColInfos, embededTable.ColInfos...)
			continue
		}
		colInfo := GetColInfo(field)

		if colInfo == nil {
			if !field.Anonymous {
				remainFields = append(remainFields, field)
			}
			continue
		}
		colInfo.IndexOnStruct = i
		colsProc[colInfo.Name] = i
		table.ColInfos = append(table.ColInfos, *colInfo)
	}
	/* check in remainFileds
	//sometime an entity may be inherite from another entity, so we need to check all fields in all parent entities
	// example
	// type A struct {
	// 	ID int `db:"pk"`
	// 	Code string `db:"size(100)"`
	// }
	// type B struct {
	// 	A
	//  ID string `db:"pk"` // this will be used instead of A.ID
	// 	Name string `db:"size(100)"`
	// }
	// new struct of table is looks like:
	// type newB struct {
	// 	ID string `db:"pk"` // use B.ID instead of A.ID
	/** 	Code string `db:"size(100)"` //use A.Code because Code was not found in B struct, so we use A.Code instead of B.Code
	/** 	Name string `db:"size(100)"`
	/** }
	/**
	*/
	for _, field := range remainFields {
		tag := field.Tag.Get("db")
		if tag == "" {
			if field.Type.Kind() == reflect.Ptr {
				field.Type = field.Type.Elem()
			}
			if field.Type.Kind() == reflect.Struct {
				extraCols := []types.ColInfo{}
				for i := 0; i < field.Type.NumField(); i++ {
					field2 := field.Type.Field(i)
					colInfo := GetColInfo(field2)
					if colInfo == nil {
						continue
					}
					extraCols = append(extraCols, *colInfo)
				}
				for _, col := range extraCols {
					if _, ok := colsProc[col.Name]; !ok {
						//skip extra col
						continue
					} else {
						table.ColInfos = append(table.ColInfos, col)
					}
				}

			}
		}
	}
	//check relationship in remainFields
	for _, field := range remainFields {
		tag := field.Tag.Get("db")
		strTags := strings.ToLower(";" + tag + ";")

		for k, v := range replacerConstraint {
			for _, t := range v {
				strTags = strings.ReplaceAll(strTags, ";"+t+";", ";"+k+";")
				strTags = strings.ReplaceAll(strTags, ";"+t+":", ";"+k+":")
				strTags = strings.ReplaceAll(strTags, ";"+t+"(", ";"+k+"(")

			}

		}
		if strings.Contains(strTags, "fk:") {
			strFk := strings.Split(strTags, ":")[1]
			strFk = strings.Split(strFk, ";")[0]
			fieldType := field.Type

			if fieldType.Kind() == reflect.Slice {
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
			}
			reltblInfo, err := GetTableInfoByType(fieldType)
			if err != nil {
				return nil, err
			}
			fromCols := []types.ColInfo{}
			for _, col := range table.ColInfos {
				if col.IsPrimary {
					fromCols = append(fromCols, col)
				}
			}
			toCols := []types.ColInfo{}
			strFkCompare := strings.ToLower("," + strFk + ",")
			for _, col := range reltblInfo.ColInfos {
				colCompare := strings.ToLower("," + col.Name + ",")
				if strings.Contains(strFkCompare, colCompare) {
					toCols = append(toCols, col)
				}
			}

			rel := types.RelationshipInfo{
				FromTable: table,
				ToTable:   *reltblInfo,
				FromCols:  fromCols,
				ToCols:    toCols,
			}
			table.Relationship = append(table.Relationship, &rel)

		} else if strings.Contains(strTags, ";fk(") {
			fmt.Println("not support fk with func yet")

		} else {

		}

	}
	table.MapCols = make(map[string]*types.ColInfo)
	table.AutoValueCols = make(map[string]*types.ColInfo)
	for i, col := range table.ColInfos {
		table.MapCols[col.Name] = &table.ColInfos[i]
		if col.DefaultValue != "" {
			table.AutoValueCols[col.Name] = &table.ColInfos[i]
		}
	}
	table.EntityType = typ

	return &table, nil
}
func GetTypeOfEntity(entity interface{}) reflect.Type {
	typ := reflect.TypeOf(entity)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return typ
}
