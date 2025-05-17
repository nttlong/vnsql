package utils

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql/compiler"
	"github.com/nttlong/vnsql/compiler/compiler_postgres"
	"github.com/nttlong/vnsql/excutor/executor_postgres"
	_ "github.com/nttlong/vnsql/excutor/executor_postgres"
	"github.com/nttlong/vnsql/types"
	_ "github.com/nttlong/vnsql/types"
	"github.com/nttlong/vnsql/types/info"
	_ "github.com/nttlong/vnsql/types/info"
	"github.com/nttlong/vnsql/types/info/info_postgres"
	_ "github.com/nttlong/vnsql/types/info/info_postgres"
)

// refactor code for maintainable code
type DbCfg struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	UseSSL   bool
}
type Model struct {
	typ        reflect.Type
	ctx        *TenantDbContext
	fiter      string
	filterArgs []interface{}
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

type CtxResult struct {
	sql.Result
}

func (r *CtxResult) String() string {
	lastInsertedId, errLastInsertedId := r.Result.LastInsertId()
	rowsAffected, errRowsAffected := r.RowsAffected()
	if errLastInsertedId != nil || errRowsAffected != nil {
		return fmt.Sprintf("last insert id: %v, rows affected: %v", errLastInsertedId, errRowsAffected)
	}
	return fmt.Sprintf("last insert id: %d, rows affected: %d", lastInsertedId, rowsAffected)

}

type DbContext struct {
	*sql.DB
	dbDriver string
	cfg      *DbCfg

	dns string
}
type TenantDbContext struct {
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

var cachedTenantDb sync.Map
var cacheMigrate sync.Map
var cacheGetExecutor sync.Map
var entitiesCache map[string]*types.TableInfo = make(map[string]*types.TableInfo)

func RegisterEntity(entity ...interface{}) error {
	for _, e := range entity {
		tblInfo, err := info.GetTableInfo(e)
		if err != nil {
			return err
		}
		entitiesCache[strings.ToLower(tblInfo.TableName)] = tblInfo
	}
	return nil
}

func GetExecutor(dbDriver string) types.IExecutor {
	if v, ok := cacheGetExecutor.Load(dbDriver); ok {
		return v.(types.IExecutor)
	}
	if dbDriver == "postgres" {

		ret := &executor_postgres.Executor{}
		cacheGetExecutor.Store(dbDriver, ret)
		return ret
	}
	panic(fmt.Errorf("not support db driver %s", dbDriver))
}
func (ctx *DbContext) GetCompiler() compiler.Compiler {
	if ctx.dbDriver == "postgres" {
		ret := *compiler_postgres.NewCompiler().Compiler
		ret.TableInfo = entitiesCache
		return ret
	} else {
		panic(fmt.Errorf("not support db driver %s", ctx.dbDriver))
	}
}
func (ctx *TenantDbContext) Migrate(entity interface{}) (reflect.Type, error) {

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
	tableInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return nil, err
	}

	key := ctx.dbName + "-" + typ.String()
	if _, ok := cacheMigrate.Load(key); ok {
		return typ, nil
	}
	sqlCreate := info_postgres.GetSqlOfTableInfoForPostgres(*tableInfo, nil)
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open TenentDbContext first")
	}
	for _, sql := range sqlCreate {
		_, err = ctx.DB.Exec(sql)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				continue
			}
			return nil, err
		}
	}
	cacheMigrate.Store(key, true)
	return typ, nil
}

func (ctx *DbContext) CreateCtx(dbName string) (*TenantDbContext, error) {
	// check cache

	if ctx.cfg == nil {
		return nil, fmt.Errorf("db config is nil")
	}
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open DbContext first")
	}
	defer ctx.Close()
	if ctx.dbDriver == "postgres" {
		if _, ok := cachedTenantDb.Load(dbName); !ok {

			err := GetExecutor(ctx.dbDriver).CreatePostgresDbIfNotExist(ctx.DB, dbName, ctx.cfg.GetDns(dbName))
			if err != nil {
				return nil, err
			}
			//cache all tablename and column name

			//set to cach
			cachedTenantDb.Store(dbName, true)
		}

	}
	migrateDb, err := sql.Open(ctx.dbDriver, ctx.cfg.GetDns(dbName))
	if err != nil {
		return nil, err
	}
	defer migrateDb.Close()
	tblMap, err := GetExecutor(ctx.dbDriver).GetTableInfoFormDb(migrateDb, dbName)
	if err != nil {
		return nil, err
	}
	tblInTableMap := *tblMap
	dbCompiler := ctx.GetCompiler()
	if _, ok := dbCompiler.GetDbDict(dbName); !ok {
		fx := compiler.DbDictionary{
			DbName: dbName,
			Tables: map[string]compiler.DbTableDictionaryItem{},
		}
		for k, v := range tblInTableMap {
			fx.Tables[k] = compiler.DbTableDictionaryItem{
				TableName: v.TableName,
				Cols:      v.ColInfos,
			}
		}
		dbCompiler.AddDbDict(dbName, fx)
	}
	for _, v := range entitiesCache {

		if tblInMap, ok := tblInTableMap[strings.ToLower(v.TableName)]; ok {
			tblInMap.EntityType = v.EntityType
		}
		sqlCreate := info_postgres.GetSqlOfTableInfoForPostgres(*v, tblMap)
		for _, sql := range sqlCreate {
			_, err = migrateDb.Exec(sql)
			if err != nil {
				if strings.Contains(err.Error(), "already exists") {
					continue
				}
				return nil, err
			}
			fmt.Println(sql)
		}

	}

	ret := TenantDbContext{
		DbContext: &DbContext{
			dbDriver: ctx.cfg.Driver,
			cfg:      ctx.cfg,
			dns:      ctx.cfg.GetDns(dbName),
		},
		dbName: dbName,
	}
	return &ret, nil

}
func (ctx *TenantDbContext) getCompiler() *compiler.Compiler {
	if ctx.cfg.Driver == "postgres" {
		return compiler_postgres.Compiler.Compiler
	} else {
		panic(fmt.Errorf("not support db driver %s", ctx.cfg.Driver))
	}
}
func (ctx *TenantDbContext) Insert(entity interface{}) error {
	var walker *compiler.Compiler
	if ctx.cfg.Driver == "postgres" {
		walker = compiler_postgres.Compiler.Compiler
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
	dataInsert, err := GetExecutor(ctx.dbDriver).CreateInsertCommand(entity, *tblInfo)

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	if ctx.DB == nil {
		return fmt.Errorf("please open TenantDbContext first")
	}
	// dbTableInfo, err := ctx.GetTableMappingFromDb()
	if err != nil {
		return err
	}
	execSql, err := walker.Parse(dataInsert.Sql)
	if err != nil {
		return err
	}
	// start := time.Now()
	if walker.OnParseInsertSQL == nil {
		return fmt.Errorf("compiler.Compiler.OnParseInsertSQL is not set")
	}
	if tblInfo.AutoValueColsName == nil {
		tblInfo.AutoValueColsName = []string{}
		for _, col := range tblInfo.ColInfos {
			if col.DefaultValue == "auto" {
				tblInfo.AutoValueColsName = append(tblInfo.AutoValueColsName, col.Name)
			}
		}

	}
	execSql2, err := walker.OnParseInsertSQL(walker, execSql, tblInfo.AutoValueColsName, []string{})
	if err != nil {
		return err
	}
	// resultArray := []interface{}{}

	rw, err := ctx.Query((*execSql2), dataInsert.Params...)
	if err != nil {
		return err
	}
	defer rw.Close()
	// colsVal, err := rw.Columns()
	// for _, c := range colsVal {
	// 	if ft, ok := tblInfo.AutoValueCols[c]; ok {
	// 		val := reflect.New(ft.FieldType.Type).Interface()
	// 		resultArray = append(resultArray, val)
	// 	}

	// }
	for rw.Next() {
		err := scanRowToStruct(rw, entity) // thay may cai vong lap o duoi ban ham nay chay OK
		if err != nil {
			return err
		}
		// err = rw.Scan(resultArray...)
		// if err != nil {
		// 	return err
		// }
		// for i, x := range resultArray {
		// 	fieldName := colsVal[i]
		// 	fieldVal := reflect.ValueOf(entity).Elem().FieldByName(fieldName)
		// 	fieldVal.Set(reflect.ValueOf(x).Elem())

		// }
	}

	if err != nil {
		return err
	}
	// fmt.Println("insert time: ", time.Now().Sub(start).Milliseconds())
	return nil
}
func (ctx *TenantDbContext) Exec(query string, args ...interface{}) (*CtxResult, error) {
	compiler := ctx.getCompiler()
	// tblMap, err := ctx.GetTableMappingFromDb()

	// if err != nil {
	// 	return nil, err
	// }

	execSql, err := compiler.Parse(query)
	if err != nil {
		return nil, err
	}
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open TenantDbContext first")
	}
	ret, err := ctx.DB.Exec(execSql, args...)
	if err != nil {
		return nil, err
	}
	return &CtxResult{
		Result: ret,
	}, nil
}
func getSqlSelect(typ reflect.Type) (*string, error) {
	tblInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return nil, err
	}
	field := []string{}
	for _, col := range tblInfo.ColInfos {
		field = append(field, col.Name)
	}
	selectFields := strings.Join(field, ",")
	ret := fmt.Sprintf("select %s from %s", selectFields, typ.Name())
	return &ret, nil

}
func scanRowToStruct(rows *sql.Rows, dest interface{}) error {
	destType := reflect.TypeOf(dest)
	destValue := reflect.ValueOf(dest)

	if destType.Kind() != reflect.Ptr || destValue.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer to a struct")
	}

	structType := destType.Elem()
	if structType.Kind() != reflect.Struct {
		return fmt.Errorf("destination must be a pointer to a struct")
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	scanArgs := make([]interface{}, len(columns))
	fields := make([]reflect.Value, len(columns))

	for i, col := range columns {
		field := destValue.Elem().FieldByName(col)
		// chac chan la tim duoc vi sau sql select duoc sinh ra tu cac field cua struct
		if field.IsValid() && field.CanSet() {
			fields[i] = field
			scanArgs[i] = field.Addr().Interface()
		} else {
			// Nếu không tìm thấy field phù hợp, vẫn cần một nơi để scan giá trị
			var dummy interface{}
			scanArgs[i] = &dummy
		}
	}

	err = rows.Scan(scanArgs...)
	if err != nil {
		return err
	}

	return nil
}
func (ctx *TenantDbContext) Find(entity interface{}, filterExr string, args ...interface{}) error {

	var walker *compiler.Compiler
	if ctx.cfg.Driver == "postgres" {
		walker = compiler_postgres.Compiler.Compiler
	} else {
		panic(fmt.Errorf("not support db driver %s", ctx.cfg.Driver))
	}

	typ, err := ctx.Migrate(entity)
	if err != nil {
		return err
	}

	if ctx.DB == nil {
		return fmt.Errorf("please open TenentDbContext first")
	}
	if filterExr == "" {
		return fmt.Errorf("filter is nil")
	}
	// sql := "select * from " + typ.Name() + " where " + filterExr
	sqlSelect, err := getSqlSelect(typ)
	if err != nil {
		return err
	}
	sql := *sqlSelect + " where " + filterExr // cau sql chua chay duoc tren datbase server thuc
	// tblMap, err := getTableMap(typ)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	/**
	Ham walker.Parse chuyen doi cau sql sang dung voi cau sql thuc te chay tren database
	*/
	execSQl, err := walker.Parse(sql)

	if err != nil {
		return err
	}

	rows, err := ctx.Query(execSQl, args...)
	if err != nil {
		return err
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	defer rows.Close()
	// entityType := reflect.TypeOf(entity)
	//entityType := reflect.TypeOf(entity)
	entityValue := reflect.ValueOf(entity)
	//fmt.Println(entityType.Kind())

	// if entityType.Kind() != reflect.Ptr || entityType.Elem().Kind() != reflect.Slice {

	// 	return fmt.Errorf("entity must be a pointer to a slice %s example []*Vector", entityType.Kind())
	// }
	sliceValue := entityValue
	sliceValue = entityValue.Elem()
	// fmt.Println(sliceValue.Kind())
	// Tạo một phần tử mới (zero value của kiểu phần tử)

	// Hoặc nếu bạn có một giá trị cụ thể muốn append:
	// newElementValue := reflect.ValueOf(yourNewStruct)
	// if newElementValue.Type() != elementType {
	// 	fmt.Println("Type of new element does not match the slice element type")
	// 	return
	// }
	// newElement = newElementValue

	// Append phần tử mới vào slice
	// start := time.Now()
	for rows.Next() {

		//rEntity := reflect.New(typ).Interface()
		newElement := reflect.New(typ).Interface()

		err := scanRowToStruct(rows, newElement)
		if err != nil {
			return err
		}

		sliceValue = reflect.Append(sliceValue, reflect.ValueOf(newElement))

	}

	entityValue.Elem().Set(sliceValue)
	// fmt.Println("find time: ", time.Now().Sub(start).Milliseconds())
	return nil

}
func (ctx *TenantDbContext) Delete(entity interface{}) error {

	panic("not implement")
}

func (ctx *TenantDbContext) Model(entity interface{}) *Model {
	ctx.Migrate(entity)
	typ := reflect.TypeOf(entity)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return &Model{
		typ: typ,
		ctx: ctx,
	}
}
func (m *Model) Filter(filterExr string, args ...interface{}) *Model {
	m.filterArgs = args
	m.fiter = filterExr
	return m
}

var tabbMap sync.Map

func getTableMap(typ reflect.Type) (*compiler.TableMap, error) {
	if v, ok := tabbMap.Load(typ); ok {
		return v.(*compiler.TableMap), nil
	}
	tblInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return nil, err
	}

	tableMap := compiler.TableMap{}
	for _, col := range tblInfo.ColInfos {
		tableMap[strings.ToLower(col.Name)] = col.Name
	}
	tabbMap.Store(typ, &tableMap)
	return &tableMap, nil
}

func (m *Model) Update(updateExr string, args ...interface{}) (*CtxResult, error) {
	var walker *compiler.Compiler
	if m.ctx.cfg.Driver == "postgres" {
		walker = compiler_postgres.Compiler.Compiler
	} else {
		panic(fmt.Errorf("not support db driver %s", m.ctx.cfg.Driver))
	}
	if m.ctx.DB == nil {
		return nil, fmt.Errorf("please open TenentDbContext first")
	}
	if m.fiter == "" {
		return nil, fmt.Errorf("filter is nil")
	}
	sql := fmt.Sprintf("update %s set %s where %s", m.typ.Name(), updateExr, m.fiter)
	// tableMap, err := getTableMap(m.typ)
	// if err != nil {
	// 	return nil, err
	// }
	execSQl, err := walker.Parse(sql)
	if err != nil {
		return nil, err
	}
	sqlArg := append(m.filterArgs, args...)
	ret, err := m.ctx.Exec(execSQl, sqlArg...)
	if err != nil {
		return nil, err
	}
	return &CtxResult{
		Result: ret,
	}, nil
}

// get all table and its column from database
// return a map[tableName]map[columnName]columnType
func (ctx *TenantDbContext) GetTableMappingFromDb() (*types.TableMapping, error) {
	executor := GetExecutor(ctx.dbDriver)
	return executor.GetTableInfoFormDb(ctx.DB, ctx.dbName)
}
