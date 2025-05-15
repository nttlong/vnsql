package infopostgres

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nttlong/vnsql/types"
	"google.golang.org/genproto/googleapis/type/decimal"
)

func GetSqlOfTableInfoForPostgres(table types.TableInfo) []string {
	ret := []string{}
	// create table
	sql := "CREATE TABLE IF NOT EXISTS \"" + table.TableName + "\" ("
	colsScript := []string{}
	scripAlterAddCols := []string{}

	//scriptSeqs := []string{}
	for _, col := range table.ColInfos {
		ft := col.FieldType.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}

		strCol := ""
		strAddCol := ""
		//check mapGoTypeToPosgresType
		if dbType, ok := mapGoTypeToPosgresType[ft]; ok {
			if col.IsPrimary {
				if col.DefaultValue == "auto" && col.FieldType.Type.Kind() == reflect.Int {
					strCol = fmt.Sprintf("\"%s\" SERIAL PRIMARY KEY", col.Name)
					colsScript = append(colsScript, strCol)
				} else {
					strCol = fmt.Sprintf("\"%s\" %s PRIMARY KEY", col.Name, dbType)
					colsScript = append(colsScript, strCol)
				}

			} else {

				if col.AllowNull {
					// strCol = fmt.Sprintf("\"%s\" %s NOT NULL", col.Name, dbType)
					//ALTER TABLE "Employee" ADD COLUMN IF NOT EXISTS email TEXT;
					strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table.TableName, col.Name, dbType)
					scripAlterAddCols = append(scripAlterAddCols, strAddCol)
				} else {
					// strCol = fmt.Sprintf("\"%s\" %s", col.Name, dbType)
					//ALTER TABLE "Employee" ADD COLUMN IF NOT EXISTS email TEXT;
					// df := ""
					// if _, ok := maoGoTyoToPostgresDefaultValue[ft]; ok {
					// 	df = maoGoTyoToPostgresDefaultValue[ft]
					// }
					if col.DefaultValue == "" {
						strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL", table.TableName, col.Name, dbType)
						scripAlterAddCols = append(scripAlterAddCols, strAddCol)
					} else {
						if dff, ok := mapDefaultValueFuncToPg[col.DefaultValue]; ok {
							if dff == "SERIAL" {
								defaultSeq := "\"" + table.TableName + "_" + col.Name + "_seq\""
								scriptSeq := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", defaultSeq)
								strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT nextval('%s')", table.TableName, col.Name, dbType, defaultSeq)
								scripAlterAddCols = append(scripAlterAddCols, scriptSeq)
								scripAlterAddCols = append(scripAlterAddCols, strAddCol)
							} else {
								strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT %s", table.TableName, col.Name, dbType, dff)
								scripAlterAddCols = append(scripAlterAddCols, strAddCol)
							}
						} else {
							strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT %s", table.TableName, col.Name, dbType, col.DefaultValue)
							scripAlterAddCols = append(scripAlterAddCols, strAddCol)
						}
					}

				}

			}

		} else {
			panic(fmt.Errorf("not support map %s in GO type to in postgres ", ft.Name()))
		}

	}

	sql += strings.Join(colsScript, ",") + ")"
	ret = append(ret, sql)
	// scan col if col has len>-1
	scriptAddLenConstraint := []string{}
	indexInfo := map[string][]string{}
	unIndexInfo := map[string][]string{}
	for _, col := range table.ColInfos {
		if col.Len > 0 {
			/**
						ALTER TABLE IF EXISTS public."Users"
			    ADD CONSTRAINT "Max_Name_50" CHECK (length("Name"::text) <= 50);
			*/
			sqlAddCheckLen := "ALTER TABLE IF EXISTS \"" + table.TableName + "\" ADD CONSTRAINT \"" + table.TableName + "_" + col.Name + "_" + strconv.Itoa(col.Len) + "\" CHECK (length(\"" + col.Name + "\"::text) <= " + strconv.Itoa(col.Len) + ")"
			scriptAddLenConstraint = append(scriptAddLenConstraint, sqlAddCheckLen)
		}
		if col.IsIndex {
			if col.IndexName == "" {
				col.IndexName = table.TableName + "_" + col.Name
			}
			if _, ok := indexInfo[col.IndexName]; !ok {
				indexInfo[col.IndexName] = []string{col.Name}
			} else {
				indexInfo[col.IndexName] = append(indexInfo[col.IndexName], col.Name)
			}

		}
		if col.IsUnique && !col.IsPrimary {
			if col.IndexName == "" {
				col.IndexName = table.TableName + "_" + col.Name
			}
			if _, ok := unIndexInfo[col.IndexName]; !ok {
				unIndexInfo[col.IndexName] = []string{}
			}
			unIndexInfo[col.IndexName] = append(unIndexInfo[col.IndexName], col.Name)
		}

	}
	scriptIndex := []string{}
	for index_name, cols := range indexInfo {
		strCosl := strings.Join(cols, "\",\"")
		sqlCreateIndex := "CREATE INDEX IF NOT EXISTS \"" + index_name + "\" ON \"" + table.TableName + "\" (\"" + strCosl + "\")"
		scriptIndex = append(scriptIndex, sqlCreateIndex)
	}
	scriptUnIndex := []string{}
	for index_name, cols := range unIndexInfo {
		strCosl := strings.Join(cols, "\",\"")
		sqlCreateIndex := "CREATE UNIQUE INDEX IF NOT EXISTS \"" + index_name + "\" ON \"" + table.TableName + "\" (\"" + strCosl + "\")"
		scriptUnIndex = append(scriptUnIndex, sqlCreateIndex)
	}
	ret = append(ret, scripAlterAddCols...)
	ret = append(ret, scriptAddLenConstraint...)
	ret = append(ret, scriptIndex...)
	ret = append(ret, scriptUnIndex...)
	scriptCreateRel := []string{}
	for _, rel := range table.Relationship {
		if ret != nil {
			sqlOfToTable := GetSqlOfTableInfoForPostgres(rel.ToTable)
			ret = append(ret, sqlOfToTable...)
			/*
						ALTER TABLE <tên_bảng_có_khóa_ngoại>
				ADD CONSTRAINT <tên_constraint>
				FOREIGN KEY (<cột_khóa_ngoại>)
				REFERENCES <tên_bảng_được_tham_chiếu> (<cột_khóa_chính>);
						**/
			keyCOls := ""

			for _, col := range rel.FromCols {
				keyCOls += "\"" + col.Name + "\","
			}
			keyCOls = strings.TrimSuffix(keyCOls, ",")
			fkCol := ""
			for _, col := range rel.ToCols {
				fkCol += "\"" + col.Name + "\","
			}
			fkCol = strings.TrimSuffix(fkCol, ",")
			relName := table.TableName + "_" + rel.ToTable.TableName + "_fk"
			strRel := "ALTER TABLE \"" + rel.ToTable.TableName + "\" ADD CONSTRAINT \"" + relName + "\" FOREIGN KEY (" + fkCol + ") REFERENCES \"" + rel.FromTable.TableName + "\" (" + keyCOls + ") MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION"
			scriptCreateRel = append(scriptCreateRel, strRel)

		}

	}
	ret = append(ret, scriptCreateRel...)

	return ret
}

//--- private regin -----------
// serilize tag info ex: db:"name,age"

var mapGoTypeToPosgresType = map[reflect.Type]string{
	reflect.TypeOf(int(0)):            "integer",
	reflect.TypeOf(int8(0)):           "smallint",
	reflect.TypeOf(int16(0)):          "smallint",
	reflect.TypeOf(int32(0)):          "integer",
	reflect.TypeOf(int64(0)):          "bigint",
	reflect.TypeOf(uint(0)):           "integer",
	reflect.TypeOf(uint8(0)):          "smallint",
	reflect.TypeOf(uint16(0)):         "integer",
	reflect.TypeOf(uint32(0)):         "bigint",
	reflect.TypeOf(uint64(0)):         "bigint",
	reflect.TypeOf(float32(0)):        "real",
	reflect.TypeOf(float64(0)):        "double precision",
	reflect.TypeOf(string("")):        "citext",
	reflect.TypeOf(bool(false)):       "boolean",
	reflect.TypeOf(time.Time{}):       "timestamp",
	reflect.TypeOf(decimal.Decimal{}): "numeric",
	reflect.TypeOf(uuid.UUID{}):       "uuid",
}
var mapDefaultValueFuncToPg = map[string]string{
	"now()":  "CURRENT_TIMESTAMP",
	"uuid()": "uuid_generate_v4()",
	"auto":   "SERIAL",
}
