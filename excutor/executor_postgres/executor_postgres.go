package executor_postgres

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	_ "github.com/lib/pq"

	"github.com/nttlong/vnsql/types"
)

type Executor struct {
}

var cacheSqlWithParams sync.Map

func (e *Executor) CreateInsertCommand(entity interface{}, tableInfo types.TableInfo) (*types.SqlWithParams, error) {
	if v, ok := cacheSqlWithParams.Load(tableInfo.TableName); ok {
		return v.(*types.SqlWithParams), nil
	}
	// start := time.Now()

	sqlWithParams, err := e.createInsertCommand(entity, tableInfo)
	// fmt.Println("CreateInsertCommand time Nanoseconds: ", time.Since(start).Nanoseconds())
	if err != nil {
		return nil, err
	}
	cacheSqlWithParams.Store(tableInfo.TableName, sqlWithParams)
	return sqlWithParams, nil

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
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
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
