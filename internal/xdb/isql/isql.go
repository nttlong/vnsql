package isql

import (
	"database/sql"
	"fmt"

	"github.com/nttlong/vnsql/internal/xdb/common"
)

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
}
type IMigrate interface {
	SetSqlStore(store ISqlStore)
	GetColumnsInfo(entity interface{}) (*common.TableInfo, error)
	GetIndexInfo(entity interface{}) (map[string][]common.ColInfo, error)
	GetSqlCreateTable(entity interface{}) ([]string, error)
	GetSqlCreateTables(entitises ...[]interface{}) ([]string, error)
	DoMigrate(entity interface{}) error
	GetError() error
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
