package isql

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/nttlong/vnsql/internal/xdb/common"
	"github.com/nttlong/vnsql/internal/xdb/parser"
)

type IMigrate interface {
	SetSqlStore(store ISqlStore)
	GetColumnsInfo(entity interface{}) (*common.TableInfo, error)
	GetIndexInfo(entity interface{}) (map[string][]common.ColInfo, error)
	GetSqlCreateTable(entity interface{}) ([]string, error)
	GetSqlCreateTables(entitises ...[]interface{}) ([]string, error)
	DoMigrate(entity interface{}) error
	GetError() error
}
type ISql interface {
	SetConfig(cfg DbCfg)
	GetConfig() DbCfg
	GetSqlStore(dbName string) ISqlStore
	GetMigrate(dbName string) IMigrate

	PingDb() error
	OpenDb(dbName string) (*sql.DB, error)
}
type ISqlStore interface {
	SetSql(isql ISql)
	GetSql() ISql
	GetError() error
	GetDbName() string
	GetCtx() (*DBContext, error)
}
type IDbContext interface {
	GetSqlStore() ISqlStore
}

type DbCfg struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	UseSSL   bool
}

func (cfg *DbCfg) LoadYamlFile(path string) error {
	// TODO: implement this
	panic("not implemented")
}
func (cfg *DbCfg) Dns(dbName string) string {
	if dbName == "" {
		if !cfg.UseSSL {
			return fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password)
		} else {
			return fmt.Sprintf("host=%s port=%d user=%s password=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password)
		}
	} else {
		if !cfg.UseSSL {
			return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password, dbName)
		} else {
			return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password, dbName)
		}
	}
}

type DBContext struct {
	*sql.DB
	StatementWalker parser.Walker
	Migrator        IMigrate
	SqlStore        ISqlStore
}
type QuerySelector struct {
	Context *DBContext

	EntityType reflect.Type
	TableName  string
	Err        error
}

func (db *DBContext) Selector(entity interface{}, args ...interface{}) *QuerySelector {
	tblName := ""
	entityType := common.GetEntityType(entity)
	if tablename, _, ok := common.IsDataTables(entity); ok {
		tblName = tablename

	} else if len(args) == 0 {
		panic("tablename or entity must be provided")
	}
	if len(args) > 0 {
		tblName = args[0].(string)
	}
	return &QuerySelector{
		Context:    db,
		EntityType: entityType,
		TableName:  tblName,
	}
}
func (db *DBContext) Exec(query string, args ...interface{}) (sql.Result, error) {
	eQuery, err := db.StatementWalker.Parse(query, nil)
	if err != nil {
		return nil, err
	}
	return db.DB.Exec(eQuery, args...)
}
func (db *DBContext) Query(query string, args ...interface{}) (*sql.Rows, error) {
	eQuery, err := db.StatementWalker.Parse(query, nil)
	if err != nil {
		return nil, err
	}
	return db.DB.Query(eQuery, args...)
}
func (db *DBContext) IsTable(entity interface{}) {

}

// func (db *DBContext) Find(entity interface{}, args ...interface{}) error {
// 	var rows *sql.Rows = nil
// 	if tablename, typeTest, ok := common.IsDataTables(entity); ok {
// 		err := db.Migrator.DoMigrate(entity)
// 		if err != nil {
// 			return err
// 		}

// 		if len(args) == 0 {
// 			sql := "SELECT * FROM " + tablename
// 			eSQL, err := db.StatementWalker.Parse(sql, nil)
// 			if err != nil {
// 				return err
// 			}
// 			rows, err = db.Query(eSQL)
// 			if err != nil {
// 				return err
// 			}
// 			fmt.Print(typeTest)

// 		}

// 	}
// 	fmt.Print(rows)
// 	// tglInfo, err := common.GetTableInfo(db.SqlStore.GetSql().GetConfig().Driver, entity, nil)
// 	return nil
// }
