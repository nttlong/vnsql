package pg

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql/pg/internal/utils"
	_ "github.com/nttlong/vnsql/pg/internal/utils"
)

func GenratePostgresCreateTableIfNotExists(tblInfo utils.TableInfo) string {
	var sql string
	sql = "CREATE TABLE IF NOT EXISTS \"" + tblInfo.Name + "\" (\n"
	for _, col := range tblInfo.Columns {
		sql += "\"" + col.Name + "\" " + col.DbType + " "
		if col.IsPk {
			sql += "PRIMARY KEY "
		}
		if col.IsUnique {
			sql += "UNIQUE "
		}
		if !col.IsNullable {
			sql += "NOT NULL "
		}
		sql += ",\n"
	}
	sql = sql[:len(sql)-2] + "\n)"
	return sql

}

type DbCfg struct {
	Host     string
	Port     int
	User     string
	Password string

	NoSSL bool
}

func (cfg *DbCfg) Dsn() string {

	if cfg.NoSSL {
		return fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	} else {
		return fmt.Sprintf("host=%s port=%d user=%s password=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password)
	}

}
func (cfg *DbCfg) DnsDb(dbName string) string {
	if cfg.NoSSL {
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.Host, cfg.Port, cfg.User, cfg.Password, dbName)
	} else {
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password, dbName)
	}
}

// ExecSQl execute sql command in postgres
func (cfg *DbCfg) ExecSQl(SqlCommand string) error {
	dns := cfg.Dsn()
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return err
	}
	defer db.Close()
	// begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	// execute sql command
	_, err = tx.Exec(SqlCommand)
	if err != nil {
		tx.Rollback()
		return err
	}
	// commit transaction
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil

}
func (cfg *DbCfg) ExecSqlWithDbName(dbName string, SqlCommands []string) error {
	dns := cfg.DnsDb(dbName)
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return err
	}
	defer db.Close()
	// begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	// execute sql command
	for _, SqlCommand := range SqlCommands {
		_, err = tx.Exec(SqlCommand)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	// commit transaction
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

type PgColumnInfo struct {
	NotNull bool
	Default string
	Name    string
}
type PgDB struct {
	sql.DB
	DbCfg
}

func (cfg *DbCfg) GetTableInfo(dbName string, tableName string) ([]*PgColumnInfo, error) {
	var cols []*PgColumnInfo = make([]*PgColumnInfo, 0)
	SqlCommand := "SELECT column_name, data_type, is_nullable, column_default, ordinal_position FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + tableName + "'"
	dns := cfg.DnsDb(dbName)
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(SqlCommand)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var col PgColumnInfo
		var colName string
		var colType string
		var colNullable string
		var colDefault sql.NullString
		var colOrder int
		err = rows.Scan(&colName, &colType, &colNullable, &colDefault, &colOrder)
		if err != nil {
			return nil, err
		}
		if colNullable == "NO" {
			col.NotNull = true
		}
		if colDefault.Valid {
			col.Default = colDefault.String
		}
		col.Name = colName
		cols = append(cols, &col)
	}
	return cols, nil
}

func (cfg *DbCfg) CreatDbIfNotExist(dbName string) error {
	dns := cfg.Dsn()
	// create database if not exist

	SqlCommand := "CREATE DATABASE  \"" + dbName + "\""
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return err
	}
	defer db.Close()
	// exec no transaction sql command
	_, err = db.Exec(SqlCommand)
	// enanble citext extension

	if err != nil {
		return err
	}
	SqlCommand = "CREATE EXTENSION IF NOT EXISTS citext"
	dnsWidthdb := cfg.DnsDb(dbName)
	db2, err := sql.Open("postgres", dnsWidthdb)
	if err != nil {
		return err
	}
	defer db2.Close()
	// exec no transaction sql command
	_, err = db2.Exec(SqlCommand)
	if err != nil {
		return err
	}
	return nil
}

var mapCoTypePgDefaultValue = map[reflect.Type]string{
	reflect.TypeOf(int64(0)):    "0",
	reflect.TypeOf(int(0)):      "0",
	reflect.TypeOf(int8(0)):     "0",
	reflect.TypeOf(int16(0)):    "0",
	reflect.TypeOf(int32(0)):    "0",
	reflect.TypeOf(uint(0)):     "0",
	reflect.TypeOf(uint8(0)):    "0",
	reflect.TypeOf(uint16(0)):   "0",
	reflect.TypeOf(uint32(0)):   "0",
	reflect.TypeOf(uint64(0)):   "0",
	reflect.TypeOf(float32(0)):  "0",
	reflect.TypeOf(float64(0)):  "0",
	reflect.TypeOf(string("")):  "''",
	reflect.TypeOf(bool(false)): "false",
	reflect.TypeOf(time.Time{}): "now()",
}

func (cfg *DbCfg) SyncMissColumn(dbName string, tableName string, colInfos []utils.ColInfo) error {
	// get table info
	tableInfo, err := cfg.GetTableInfo(dbName, tableName)

	if err != nil {
		return err
	}
	// add miss column
	var sqlList = []string{}
	for _, colInfo := range colInfos {
		isMiss := true
		for _, tableCol := range tableInfo {
			if colInfo.Name == tableCol.Name {
				isMiss = false
				break
			}
		}
		if isMiss {
			sql := "ALTER TABLE \"" + tableName + "\" ADD COLUMN \"" + colInfo.Name + "\" " + colInfo.DbType + " "
			if !colInfo.IsNullable {
				sql += "NOT NULL "
			}
			// get default value in mapCoTypePgDefaultValue
			defaultValue, ok := mapCoTypePgDefaultValue[colInfo.Typ]
			if ok && !colInfo.IsNullable {
				if colInfo.DefaultVal == "" {
					sql += "DEFAULT " + defaultValue + " "
				} else {
					sql += "DEFAULT " + colInfo.DefaultVal + " "
				}

			}

			if colInfo.IsUnique {
				sql += "UNIQUE "
			}
			sql += ";"
			sqlList = append(sqlList, sql)

		}
	}
	err = cfg.ExecSqlWithDbName(dbName, sqlList)
	if err != nil {
		return err
	}
	return nil
}
func onResolveCol(col *utils.ColInfo) error {
	// do something
	return nil
}
func (cfg *DbCfg) DoMigration(dbName string, entity interface{}) error {
	// get table info
	tblInfo, err := utils.GetTableInfo("postgres", entity, onResolveCol)
	if err != nil {
		return err
	}

	sql := GenratePostgresCreateTableIfNotExists(*tblInfo)
	err = cfg.ExecSqlWithDbName(dbName, []string{sql})
	if err != nil {
		return err
	}
	err = cfg.SyncMissColumn(dbName, tblInfo.Name, tblInfo.Columns)
	if err != nil {
		return err
	}
	return nil

}

func (cfg *DbCfg) GetAllIndex(dbName string, tableName string) (map[string][]string, error) {
	var indexMap = make(map[string][]string)

	//create sql command get index and column name of table by join information_schema.indexes and information_schema.columns
	SqlCommand := `SELECT 
    pi.indexname,
    pa.attname AS column_name
		FROM 
			pg_indexes pi
		JOIN 
			pg_class pc ON pi.indexname = pc.relname AND pc.relkind = 'i'
		JOIN 
			pg_index pind ON pc.oid = pind.indexrelid
		JOIN 
			pg_class pct ON pct.relname = pi.tablename AND pct.relkind = 'r'
		JOIN 
			pg_attribute pa ON pa.attrelid = pct.oid AND pa.attnum = ANY(pind.indkey)
		WHERE 
			pi.schemaname = 'public'` + " AND pi.tablename = '" + tableName + "'" + ` ORDER BY pi.indexname, pa.attnum`

	dns := cfg.DnsDb(dbName)
	db, err := sql.Open("postgres", dns)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(SqlCommand)
	fmt.Print(SqlCommand)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var indexName string
		var colName string
		err = rows.Scan(&indexName, &colName)
		if err != nil {
			return nil, err
		}
		if _, ok := indexMap[indexName]; !ok {
			indexMap[indexName] = []string{colName}
		} else {
			indexMap[indexName] = append(indexMap[indexName], colName)
		}
	}
	return indexMap, nil
}
