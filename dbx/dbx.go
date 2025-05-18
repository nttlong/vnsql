package dbx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql/compiler"
	"github.com/nttlong/vnsql/excutor/executor_postgres"
	"github.com/nttlong/vnsql/types"
	"github.com/nttlong/vnsql/types/info"
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

type DBX struct {
	cfg *DbCfg
	dns string
	*sql.DB
	Executor types.IExecutor
}
type DBXTenant struct {
	DBX
	tenant   string
	compiler *compiler.Compiler
}

var cacheCompiler sync.Map

func newCompiler(dbDriverName string, dbname string) *compiler.Compiler {
	key := dbDriverName + "-" + dbname
	if dbDriverName == "postgres" {
		//check from cache
		if cached, ok := cacheCompiler.Load(key); ok {
			return cached.(*compiler.Compiler)
		}
		ret := &compiler.Compiler{
			TableDict: map[string]compiler.DbTableDictionaryItem{},
			FieldDict: map[string]string{},
			Quote: compiler.QuoteIdentifier{
				Left:  "\"",
				Right: "\"",
			},
		}
		//set cache
		cacheCompiler.Store(key, ret)
		return ret

	}
	panic(fmt.Errorf("not support db driver %s", dbDriverName))
}
func NewDBX(cfg *DbCfg) DBX {

	dns := cfg.GetDns("")

	return DBX{
		cfg:      cfg,
		dns:      dns,
		Executor: getExecutor(cfg.Driver),
	}

}

func (dbx *DBX) Open() error {
	db, err := sql.Open(dbx.cfg.Driver, dbx.dns)
	if err != nil {
		return err
	}
	dbx.DB = db
	return nil
}

var red = "\033[31m"
var reset = "\033[0m"
var cacheHasCreateDb sync.Map

func (dbx *DBX) GetTenant(tenant string) (*DBXTenant, error) {

	ret := &DBXTenant{
		DBX:    *dbx,
		tenant: tenant,
	}
	ret.dns = dbx.cfg.GetDns(tenant)
	ret.compiler = &compiler.Compiler{}
	if _, ok := cacheHasCreateDb.Load(tenant); !ok {
		dbx.Open()
		defer dbx.Close()
		err := ret.Executor.CreatePostgresDbIfNotExist(dbx.DB, tenant, ret.dns)
		if err != nil {
			return nil, err
		}
		cacheHasCreateDb.Store(tenant, true)
	}

	registerType := GetAllRegisterEntitiesTypes()
	if len(registerType) == 0 {
		fmt.Println(red + "warning:no entity register./nPlease call dbx.RegisterEntities() to register all entities." + reset)
	}
	ret.Open()
	defer ret.Close()
	for _, t := range registerType {
		e := reflect.New(t).Interface()
		_, err := ret.Migrate(e)
		if err != nil {
			return nil, err
		}
	}
	ret.compiler = newCompiler(dbx.cfg.Driver, tenant)
	ret.compiler.LoadDbDictionary(ret.DB)

	return ret, nil
}

var cacheMigrate sync.Map

func (ctx *DBXTenant) Migrate(entity interface{}) (reflect.Type, error) {

	typ := info.GetTypeOfEntity(entity)
	tableInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return nil, err
	}

	key := ctx.tenant + "-" + typ.String()
	if _, ok := cacheMigrate.Load(key); ok {
		return typ, nil
	}
	sqlCreate := ctx.Executor.CreateSqlMigrate(*tableInfo)
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open DBXTenant first")
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
func (ctx *DBXTenant) Exec(sql string, args ...interface{}) (sql.Result, error) {
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open DBXTenant first")
	}
	sqlExec, err := ctx.compiler.Parse(sql)
	if err != nil {
		return nil, err
	}
	return ctx.DB.Exec(sqlExec, args...)
}
func (ctx *DBXTenant) Insert(entity interface{}) error {

	typ, err := ctx.Migrate(entity)
	if err != nil {
		return err
	}
	tblInfo, err := info.GetTableInfoByType(typ)
	if err != nil {
		return err
	}
	dataInsert, err := ctx.Executor.CreateInsertCommand(entity, *tblInfo)

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
	execSql, err := ctx.compiler.Parse(dataInsert.Sql)
	if err != nil {
		return err
	}
	// start := time.Now()
	// if walker.OnParseInsertSQL == nil {
	// 	return fmt.Errorf("compiler.Compiler.OnParseInsertSQL is not set")
	// }
	// if tblInfo.AutoValueColsName == nil {
	// 	tblInfo.AutoValueColsName = []string{}
	// 	for _, col := range tblInfo.ColInfos {
	// 		if col.DefaultValue == "auto" {
	// 			tblInfo.AutoValueColsName = append(tblInfo.AutoValueColsName, col.Name)
	// 		}
	// 	}

	// }
	if tblInfo.AutoValueCols != nil && len(tblInfo.AutoValueColsName) == 0 {
		for _, col := range tblInfo.AutoValueCols {
			tblInfo.AutoValueColsName = append(tblInfo.AutoValueColsName, col.Name)
		}

	}
	execSql2, err := ctx.compiler.ParseInsertSQL(execSql, tblInfo.AutoValueColsName, []string{})
	//.OnParseInsertSQL(walker, execSql, tblInfo.AutoValueColsName, []string{})
	if err != nil {
		return err
	}
	// resultArray := []interface{}{}

	rw, err := ctx.Query((*execSql2), dataInsert.Params...)
	if err != nil {
		fmt.Println(red+" err: ", *execSql2+"\n"+err.Error()+reset)
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

// ---------------------------
var cacheGetExecutor sync.Map

func getExecutor(dbDriver string) types.IExecutor {
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

var cachedTableInfo sync.Map

func RegisterEntities(entity ...interface{}) error {
	for _, e := range entity {
		typ := info.GetTypeOfEntity(e)
		tblInfo, err := info.GetTableInfoByType(typ)
		if err != nil {
			return err
		}
		//set cache
		cachedTableInfo.Store(typ, tblInfo)
	}
	return nil
}
func GetAllRegisterEntitiesTypes() []reflect.Type {
	var ret []reflect.Type
	cachedTableInfo.Range(func(key, value interface{}) bool {
		ret = append(ret, key.(reflect.Type))
		return true
	})
	return ret
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
