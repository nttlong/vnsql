package executor_postgres

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"google.golang.org/genproto/googleapis/type/decimal"

	"github.com/nttlong/vnsql/types"
)

type Executor struct {
}

func (e *Executor) CreateInsertCommand(entity interface{}, tableInfo types.TableInfo) (*types.SqlWithParams, error) {

	// start := time.Now()

	sqlWithParams, err := e.createInsertCommand(entity, tableInfo)
	// fmt.Println("CreateInsertCommand time Nanoseconds: ", time.Since(start).Nanoseconds())
	if err != nil {
		return nil, err
	}

	return sqlWithParams, nil

}
func getAllFields(typ reflect.Type) []reflect.StructField {
	var fields []reflect.StructField
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			fields = append(fields, getAllFields(field.Type)...)
		} else {
			fields = append(fields, field)
		}
	}
	return fields
}
func (e *Executor) createInsertCommand(entity interface{}, tableInfo types.TableInfo) (*types.SqlWithParams, error) {
	var ret = types.SqlWithParams{
		Params: []interface{}{},
	}
	typ := reflect.TypeOf(entity)
	val := reflect.ValueOf(entity)

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		val = val.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("not support type %s", typ.String())
	}
	ret.Sql = "insert into "
	fields := []string{}
	valParams := []string{}
	// fields := getAllFields(typ)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			continue
		} else {
			if col, ok := tableInfo.MapCols[field.Name]; ok {
				fieldVal := val.Field(i)
				if fieldVal.IsZero() {
					if !col.AllowNull && col.DefaultValue != "" {
						continue
					}
					if !col.AllowNull && col.DefaultValue == "" {
						if val, ok := types.MapDefaultValueOfGoType[fieldVal.Type()]; ok {
							ret.Params = append(ret.Params, val)
							fields = append(fields, col.Name)
							valParams = append(valParams, "?")
						}
					}

				} else {
					ret.Params = append(ret.Params, fieldVal.Interface())
					fields = append(fields, col.Name)
					valParams = append(valParams, "?")
				}

			}
		}

	}
	ret.Sql += tableInfo.TableName + " (" + strings.Join(fields, ",") + ") values (" + strings.Join(valParams, ",") + ")"
	return &ret, nil
}
func (e *Executor) CreatePostgresDbIfNotExist(ctx *sql.DB, dbName string, tenantDns string) error {
	var exists bool
	err := ctx.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		sqlCreate := fmt.Sprintf("CREATE DATABASE %q", dbName) // Sử dụng %q để quote tên database an toàn
		_, err = ctx.Exec(sqlCreate)
		if err != nil {
			return err
		}

	}
	dbTenant, err := sql.Open("postgres", tenantDns)
	if err != nil {
		return err
	}
	defer dbTenant.Close()
	// enable extension citext if not exist
	_, err = dbTenant.Exec("CREATE EXTENSION IF NOT EXISTS citext")
	if err != nil {
		return err
	}
	e.GetTableInfoFormDb(dbTenant, dbName)
	return nil
}

var cacheGetTableInfoFormDb sync.Map

func (e *Executor) GetTableInfoFormDb(ctx *sql.DB, dbName string) (*types.TableMapping, error) {
	if v, ok := cacheGetTableInfoFormDb.Load(dbName); ok {
		return v.(*types.TableMapping), nil
	}
	tableMap, err := e.getTableInfoFormDb(ctx)
	if err != nil {
		return nil, err
	}
	cacheGetTableInfoFormDb.Store(dbName, tableMap)
	return tableMap, nil
}
func (e *Executor) getTableInfoFormDb(ctx *sql.DB) (*types.TableMapping, error) {

	sql := "SELECT table_name, column_name from information_schema.columns WHERE table_schema = 'public'"
	rows, err := ctx.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tableMap := types.TableMapping{}
	for rows.Next() {
		var tableName string
		var colName string
		err = rows.Scan(&tableName, &colName)
		if err != nil {
			return nil, err
		}
		keyTabelName := strings.ToLower(tableName)
		keyColName := strings.ToLower(colName)
		if _, ok := tableMap[keyTabelName]; !ok {
			tableMap[keyTabelName] = types.DbTbaleInfo{
				TableName: tableName,
				ColInfos: map[string]string{
					keyColName: colName,
				},
			}
		} else {
			tableMap[keyTabelName].ColInfos[keyColName] = colName
		}

	}

	return &tableMap, nil

}

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

func (e *Executor) CreateSqlMigrate(table types.TableInfo) []string {
	ret := []string{}
	// create table
	sql := "CREATE TABLE IF NOT EXISTS \"" + table.TableName + "\" ("
	colsScript := []string{}
	scripAlterAddCols := []string{}
	colsInfo := map[string]string{}

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
					if _, ok := colsInfo[strings.ToLower(col.Name)]; !ok {
						strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table.TableName, col.Name, dbType)

						scripAlterAddCols = append(scripAlterAddCols, strAddCol)
					}
				} else {
					// strCol = fmt.Sprintf("\"%s\" %s", col.Name, dbType)
					//ALTER TABLE "Employee" ADD COLUMN IF NOT EXISTS email TEXT;
					// df := ""
					// if _, ok := maoGoTyoToPostgresDefaultValue[ft]; ok {
					// 	df = maoGoTyoToPostgresDefaultValue[ft]
					// }
					if col.DefaultValue == "" {
						if _, ok := colsInfo[strings.ToLower(col.Name)]; !ok {
							strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL", table.TableName, col.Name, dbType)
							scripAlterAddCols = append(scripAlterAddCols, strAddCol)
						}
					} else {
						if dff, ok := mapDefaultValueFuncToPg[col.DefaultValue]; ok {
							if dff == "SERIAL" {
								defaultSeq := "\"" + table.TableName + "_" + col.Name + "_seq\""
								scriptSeq := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", defaultSeq)
								strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT nextval('%s')", table.TableName, col.Name, dbType, defaultSeq)
								scripAlterAddCols = append(scripAlterAddCols, scriptSeq)
								if _, ok := colsInfo[strings.ToLower(col.Name)]; !ok {
									scripAlterAddCols = append(scripAlterAddCols, strAddCol)
								}
							} else {
								strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT %s", table.TableName, col.Name, dbType, dff)
								if _, ok := colsInfo[strings.ToLower(col.Name)]; !ok {
									scripAlterAddCols = append(scripAlterAddCols, strAddCol)
								}
							}
						} else {
							strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT %s", table.TableName, col.Name, dbType, col.DefaultValue)
							if _, ok := colsInfo[strings.ToLower(col.Name)]; !ok {
								scripAlterAddCols = append(scripAlterAddCols, strAddCol)
							}
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
		// if tblFormMap != nil {
		// 	if _, ok := tblFormMap.ColInfos[col.Name]; ok {
		// 		continue
		// 	}
		// }
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
			sqlOfToTable := e.CreateSqlMigrate(rel.ToTable)
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
