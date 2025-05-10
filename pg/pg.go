package pg

import (
	"database/sql"
	_ "database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql"
	_ "github.com/nttlong/vnsql"
)

type PG struct {
}
type Connector struct {
	Cfg        vnsql.DbConfig
	cnnStrNoDb string
}

func (c *Connector) GetConfig() vnsql.DbConfig {
	return c.Cfg
}
func (c *Connector) PingDb() error {

	db, err := sql.Open("postgres", c.cnnStrNoDb)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.QueryRow("SELECT 1").Err()
	if err != nil {
		return err
	}
	return nil
}
func (c *Connector) SetConfig(cfg vnsql.DbConfig) error {
	if cfg.Host == "" || cfg.Port == 0 || cfg.User == "" || cfg.Password == "" {
		return fmt.Errorf("invalid database configuration")
	}
	c.Cfg = cfg
	return nil
}

var cacheConnector sync.Map // Sử dụng sync.Map
