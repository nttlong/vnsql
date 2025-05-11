package xdb

import (
	"fmt"

	_ "github.com/nttlong/vnsql/internal/xdb/common"
	"github.com/nttlong/vnsql/internal/xdb/isql"
	_ "github.com/nttlong/vnsql/internal/xdb/isql"
	"github.com/nttlong/vnsql/internal/xdb/postgres"
)

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
