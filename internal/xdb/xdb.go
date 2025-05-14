package xdb

import (
	"fmt"

	_ "github.com/nttlong/vnsql/internal/xdb/common"
	"github.com/nttlong/vnsql/internal/xdb/isql"
	_ "github.com/nttlong/vnsql/internal/xdb/isql"
	"github.com/nttlong/vnsql/internal/xdb/postgres"
)

type Cfg struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	UseSSL   bool
}

func NewCfg(cfg Cfg) isql.DbCfg {
	return isql.DbCfg{
		Driver: cfg.Driver,
		Host:   cfg.Host,
		Port:   cfg.Port,
	}
}

func NewSql(driver string) (isql.ISql, error) {
	switch driver {
	case "mysql":
		return nil, fmt.Errorf("mysql not supported yet")
	case "postgres":
		return postgres.New(), nil
	default:
		return nil, fmt.Errorf("%s not supported yet", driver)
	}
}
