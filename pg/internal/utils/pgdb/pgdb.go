package pgdb

import (
	"database/sql"
	_ "database/sql"

	_ "github.com/lib/pq"
)

type PgDB struct {
	db *sql.DB
}
type PgDBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}
