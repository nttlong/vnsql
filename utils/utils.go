package utils

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql/compiler"
	"github.com/nttlong/vnsql/compiler/compilerpostgres"
	"github.com/nttlong/vnsql/excutor/excutorpostgres"
	_ "github.com/nttlong/vnsql/excutor/excutorpostgres"
	"github.com/nttlong/vnsql/types"
	_ "github.com/nttlong/vnsql/types"
	"github.com/nttlong/vnsql/types/info"
	_ "github.com/nttlong/vnsql/types/info"
	"github.com/nttlong/vnsql/types/info/infopostgres"
	_ "github.com/nttlong/vnsql/types/info/infopostgres"
)

type DbCfg struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	UseSSL   bool
}

func (cfg *DbCfg) GetDns(dbName string) string {
	if cfg.Driver == "postgres" {
		if dbName == "" {
			if cfg.UseSSL {

				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			} else {
				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			}
		} else {
			if cfg.UseSSL {

				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			} else {
				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			}
		}
	} else if cfg.Driver == "mysql" {
		if dbName == "" {
			return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
		} else {
			return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
		}
	} else {
		panic(fmt.Errorf("not support db driver %s", cfg.Driver))
	}
}

type DbContext struct {
	*sql.DB
	dbDriver string
	cfg      *DbCfg

	dns string
}
type TenentDbContext struct {
	*DbContext
	dbName string
}

func NewDbContext(cfg DbCfg) *DbContext {
	ret := DbContext{
		dbDriver: cfg.Driver,
		cfg:      &cfg,
		dns:      cfg.GetDns(""),
	}
	return &ret
}
func (ctx *DbContext) Open() error {
	db, err := sql.Open(ctx.dbDriver, ctx.dns)
	if err != nil {
		return err
	}
	ctx.DB = db
	return nil
}

var cachedTenentDb sync.Map
var cachMigrate sync.Map
var cachGetExcutor sync.Map

func GetExcutor(dbDriver string) types.IExecutor {
	if v, ok := cachGetExcutor.Load(dbDriver); ok {
		return v.(types.IExecutor)
	}
	if dbDriver == "postgres" {

		ret := &excutorpostgres.Executor{}
		cachGetExcutor.Store(dbDriver, ret)
		return ret
	}
	panic(fmt.Errorf("not support db driver %s", dbDriver))
}

func (ctx *TenentDbContext) Migrate(entity interface{}) (reflect.Type, error) {

	typ := reflect.TypeOf(entity)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	tableInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return nil, err
	}

	key := ctx.dbName + "-" + typ.String()
	if _, ok := cachMigrate.Load(key); ok {
		return typ, nil
	}
	sqlCreate := infopostgres.GetSqlOfTableInfoForPostgres(*tableInfo)
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open TenentDbContext first")
	}
	for _, sql := range sqlCreate {
		_, err = ctx.Exec(sql)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				continue
			}
			return nil, err
		}
	}
	cachMigrate.Store(key, true)
	return typ, nil
}
func (ctx *DbContext) CreateCtx(dbName string) (*TenentDbContext, error) {
	// check cache

	if ctx.cfg == nil {
		return nil, fmt.Errorf("db config is nil")
	}
	ctx.Open()
	defer ctx.Close()
	if ctx.dbDriver == "postgres" {
		if _, ok := cachedTenentDb.Load(dbName); !ok {

			err := GetExcutor(ctx.dbDriver).CreatePosgresDbIfNotExist(ctx.DB, dbName, ctx.cfg.GetDns(dbName))
			if err != nil {
				return nil, err
			}
			//set to cach
			cachedTenentDb.Store(dbName, true)
		}

	}
	ret := TenentDbContext{
		DbContext: &DbContext{
			dbDriver: ctx.cfg.Driver,
			cfg:      ctx.cfg,
			dns:      ctx.cfg.GetDns(dbName),
		},
		dbName: dbName,
	}
	return &ret, nil

}
func (ctx *TenentDbContext) Insert(entity interface{}) error {
	var walker *compiler.Walker
	if ctx.cfg.Driver == "postgres" {
		walker = compilerpostgres.Walker.Walker
	} else {
		panic(fmt.Errorf("not support db driver %s", ctx.cfg.Driver))
	}
	typ, err := ctx.Migrate(entity)
	if err != nil {
		return err
	}
	tblInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return err
	}
	dataInsert, err := GetExcutor(ctx.dbDriver).CreateInsertCommand(entity, *tblInfo)

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	if ctx.DB == nil {
		return fmt.Errorf("please open TenentDbContext first")
	}
	execSql, err := walker.Parse(dataInsert.Sql, nil)
	if walker.ResolverInsertSQL == nil {
		return fmt.Errorf("walker.ResolverInsertSQL is not set")
	}
	resolverInsertSQL := walker.ResolverInsertSQL

	execSql2, err := resolverInsertSQL(execSql, *tblInfo)
	if err != nil {
		return err
	}
	resultArray := []interface{}{}

	rw, err := ctx.Query(*execSql2, dataInsert.Params...)
	if err != nil {
		return err
	}
	defer rw.Close()
	colsVal, err := rw.Columns()
	for _, c := range colsVal {
		if ft, ok := tblInfo.AutoValueCols[c]; ok {
			val := reflect.New(ft.FieldType.Type).Interface()
			resultArray = append(resultArray, val)
		}

	}
	for rw.Next() {
		err = rw.Scan(resultArray...)
		if err != nil {
			return err
		}
		for i, x := range resultArray {
			fieldName := colsVal[i]
			fieldVal := reflect.ValueOf(entity).Elem().FieldByName(fieldName)
			fieldVal.Set(reflect.ValueOf(x).Elem())

		}
	}

	if err != nil {
		return err
	}
	// fmt.Println("insert time: ", time.Now().Sub(start).Milliseconds())
	return nil
}
