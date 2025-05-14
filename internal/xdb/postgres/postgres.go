package postgres

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nttlong/vnsql/internal/xdb/common"
	"github.com/nttlong/vnsql/internal/xdb/isql"
	"github.com/nttlong/vnsql/internal/xdb/parser"
	pgParser "github.com/nttlong/vnsql/internal/xdb/parser/postgres"
)

type Postgres struct {
	cfg isql.DbCfg
}
type SqlStore struct {
	sql    isql.ISql
	dbName string
	err    error
}
type Migrate struct {
	sqlStore isql.ISqlStore
}

func (st *SqlStore) SetSql(isql isql.ISql) {
	st.sql = isql

}
func (st *SqlStore) GetSql() isql.ISql {
	return st.sql

}
func (st *SqlStore) GetError() error {
	return st.err
}
func (st *SqlStore) GetDbName() string {
	return st.dbName
}

var walker = parser.Walker{
	Resolver: pgParser.PostgresResoler,
}

func (st *SqlStore) GetCtx() (*isql.DBContext, error) {
	cfg := st.GetSql().GetConfig()
	dns := cfg.Dns(st.dbName)
	retDb, err := sql.Open(cfg.Driver, dns)
	if err != nil {
		return nil, err
	}
	return &isql.DBContext{
		DB:              retDb,
		StatementWalker: walker,
		Migrator:        st.GetSql().GetMigrate(st.dbName),
		SqlStore:        st,
	}, nil

}

func (m *Migrate) SetSqlStore(store isql.ISqlStore) {
	m.sqlStore = store
	m.sqlStore.SetSql(m.GetSql())

}
func (m *Migrate) GetSql() isql.ISql {
	return m.sqlStore.GetSql()

}
func (m *Migrate) GetIndexInfo(entity interface{}) (map[string][]common.ColInfo, error) {
	tbl, err := m.GetColumnsInfo(entity)
	if err != nil {
		return nil, err
	}
	return common.GetAllIndexInColsInfo(tbl.Columns), nil
}
func (m *Migrate) GetColumnsInfo(entity interface{}) (*common.TableInfo, error) {
	if m.sqlStore.GetError() != nil {
		return nil, m.sqlStore.GetError()
	}
	cfg := m.sqlStore.GetSql().GetConfig()

	return common.GetTableInfo(cfg.Driver, entity, OnResolveColInfo)
}
func (m *Migrate) GetError() error {
	return m.sqlStore.GetError()
}
func (m *Migrate) GetSqlCreateTable(entity interface{}) ([]string, error) {
	tbl, err := m.GetColumnsInfo(entity)
	ret := []string{}
	if err != nil {
		return []string{}, err
	}
	for _, subTalbe := range tbl.RelationTables {
		subSQl, err := m.GetSqlCreateTable(subTalbe.Entity)
		if err != nil {
			return []string{}, err
		}
		ret = append(ret, subSQl...)
	}
	retCreateTable := "CREATE TABLE \"" + tbl.Name + "\" (\n"
	for _, col := range tbl.Columns {
		if col.Name == "" {
			continue
		}
		retCreateTable += "\"" + col.Name + "\" " + col.DbType + " "
		if col.IsPk {
			retCreateTable += "PRIMARY KEY "
		}
		if col.IsUnique {
			retCreateTable += "UNIQUE "
		}
		if !col.IsNullable {
			retCreateTable += "NOT NULL "
		}
		if col.DefaultVal != "" {
			retCreateTable += "DEFAULT " + col.DefaultVal
		}
		retCreateTable += ",\n"
	}
	retCreateTable = retCreateTable[:len(retCreateTable)-2] + "\n)"
	ret = append(ret, retCreateTable)
	// get all columms of table in database
	dbName := m.sqlStore.GetDbName()
	db, err := m.sqlStore.GetSql().OpenDb(dbName)
	if err != nil {
		return []string{}, err
	}
	cols, err := getTableInfo(db, tbl.Name)
	if err != nil {
		return []string{}, err
	}
	// check if all columns in table in database
	if len(cols) > 0 {
		for _, col := range tbl.Columns {
			exist := false
			for _, colDb := range cols {
				if colDb.Name == col.Name {
					exist = true
					break
				}
			}
			if !exist {
				sqlAddColumn := "ALTER TABLE \"" + tbl.Name + "\" ADD COLUMN \"" + col.Name + "\" " + col.DbType
				if col.DefaultVal != "" {
					sqlAddColumn += " DEFAULT " + col.DefaultVal
				}
				ret = append(ret, sqlAddColumn)
			}
		}
	}
	indexInfo := common.GetAllIndexInColsInfo(tbl.Columns)
	for indexName, indexCols := range indexInfo {
		sqlCreateIndex := "CREATE INDEX \"" + indexName + "\" ON \"" + tbl.Name + "\" ("
		for _, indexCol := range indexCols {
			sqlCreateIndex += "\"" + indexCol.Name + "\", "
		}
		sqlCreateIndex = sqlCreateIndex[:len(sqlCreateIndex)-2] + ")"
		ret = append(ret, sqlCreateIndex)
	}
	// create sql check len of text in postgres
	for _, col := range tbl.Columns {
		/**
		ALTER TABLE your_table_name
		ADD CONSTRAINT your_constraint_name CHECK (LENGTH(your_citext_column) <= your_desired_length);
		*/
		if col.Size > -1 {
			constraintName := fmt.Sprintf("Max_%s_%d", col.Name, col.Size)
			sqlCheckLen := "ALTER TABLE \"" + tbl.Name + "\" ADD CONSTRAINT \"" + constraintName + "\" CHECK (LENGTH(\"" + col.Name + "\") <= " + fmt.Sprintf("%d", col.Size) + ")"
			ret = append(ret, sqlCheckLen)
		}
	}
	// create foreing key
	for _, tbl := range tbl.RelationTables {
		pks := []string{}
		for _, col := range tbl.Columns {
			if col.IsPk {
				pks = append(pks, col.Name)
			}
		}
		/*
					ALTER TABLE child_table_name
			ADD CONSTRAINT fk_constraint_name
			FOREIGN KEY (child_column_name)
			REFERENCES parent_table_name (parent_column_name)
			ON UPDATE CASCADE;
					**/
		constraintName := fmt.Sprintf("FK_%s_%s", tbl.Name, tbl.ForeingKey[0])

		fk := "\"" + strings.Join(tbl.ForeingKey, "\",\"") + "\""
		pk := "\"" + strings.Join(pks, "\", \"") + "\""
		sqlCreateForeingKey := "ALTER TABLE \"" + tbl.Name + "\" ADD CONSTRAINT \"" + constraintName + "\" FOREIGN KEY (" + fk + ") REFERENCES \"" + tbl.ForeingTable + "\" (" + pk + ")"
		//sqlCreateForeingKey := "ALTER TABLE \"" + tbl.Name + "\" ADD FOREIGN KEY (\"" + tbl.ForeingKey[0] + "\") REFERENCES \"" + tbl.ForeingTable + "\" (\"" + pks[0] + "\")"
		ret = append(ret, sqlCreateForeingKey)
	}

	return ret, nil
}
func (m *Migrate) GetSqlCreateTables(entitises ...[]interface{}) ([]string, error) {
	ret := []string{}
	for _, entity := range entitises {
		sqls, err := m.GetSqlCreateTable(entity)
		if err != nil {
			return []string{}, err
		}
		ret = append(ret, sqls...)
	}
	return ret, nil
}

var isMigrated sync.Map

func (m *Migrate) DoMigrate(entity interface{}) error {
	typ := common.GetEntityType(entity)
	key := m.sqlStore.GetDbName() + "_" + typ.String()
	if _, ok := isMigrated.Load(key); ok {
		return nil
	}
	entityTest := reflect.New(typ).Interface()
	sqls, err := m.GetSqlCreateTable(entityTest)
	if err != nil {
		return err
	}
	db, err := m.sqlStore.GetSql().OpenDb(m.sqlStore.GetDbName())
	if err != nil {
		return err
	}
	defer db.Close()
	for _, sql := range sqls {
		_, err = db.Exec(sql)
		if err != nil {
			if pgErr, ok := err.(*pq.Error); ok {
				message := pgErr.Message

				if strings.Contains(message, "already exists") {
					continue
				}
				sqlState := pgErr.SQLState()

				detail := pgErr.Detail
				hint := pgErr.Hint
				where := pgErr.Where
				return fmt.Errorf("sql:%s, sqlState:%s, detail:%s, message:%s, hint:%s, where:%s", sql, sqlState, detail, message, hint, where)

			}
			strErr := err.Error()
			if !strings.Contains(strErr, "already exists") {
				print(sql)
				return err
			}
		}

	}
	isMigrated.Store(key, true)
	return nil

}
func (p *Postgres) SetConfig(cfg isql.DbCfg) {
	p.cfg = cfg
	p.cfg.Driver = "postgres"
}
func (p *Postgres) GetConfig() isql.DbCfg {
	return p.cfg
}
func (p *Postgres) OpenDb(dbName string) (*sql.DB, error) {
	cfg := p.GetConfig()
	dns := cfg.Dns(dbName)
	sql, err := sql.Open(cfg.Driver, dns)
	if err != nil {
		return nil, err
	}
	return sql, nil
}

var cacheSqlStore sync.Map

func (p *Postgres) GetSqlStore(dbName string) isql.ISqlStore {
	// check cache
	if val, ok := cacheSqlStore.Load(dbName); ok {
		return val.(*SqlStore)
	}
	// create new SqlStore
	sqlStore := p.createSqlStore(dbName)
	// save to cache
	cacheSqlStore.Store(dbName, sqlStore)
	return sqlStore
}

func (p *Postgres) createSqlStore(dbName string) *SqlStore {
	sqlCreateDatabase := "CREATE DATABASE  \"" + dbName + "\" "
	sqlEnableCitex := "CREATE EXTENSION IF NOT EXISTS citext"

	db, err := p.OpenDb("")
	if err != nil {
		return &SqlStore{
			sql:    p,
			dbName: dbName,
			err:    err,
		}
	}
	defer db.Close()
	_, err = db.Exec(sqlCreateDatabase)
	if err != nil {
		errStr := err.Error()
		fmt.Print(errStr)
		expectErr := "pq: database \"" + dbName + "\" already exists"
		fmt.Print(expectErr)

		if errStr != expectErr {
			return &SqlStore{
				sql:    p,
				dbName: dbName,
				err:    err,
			}
		}
	}
	dbCreate, err := p.OpenDb(dbName)
	if err != nil {
		return &SqlStore{
			sql:    p,
			dbName: dbName,
			err:    err,
		}
	}
	defer dbCreate.Close()
	_, err = dbCreate.Exec(sqlEnableCitex)
	if err != nil {
		return &SqlStore{
			sql:    p,
			dbName: dbName,
			err:    err,
		}
	}

	return &SqlStore{
		sql:    p,
		dbName: dbName,
		err:    err,
	}
}

var cacheMigrate sync.Map

func (p *Postgres) GetMigrate(dbName string) isql.IMigrate {
	//check cache
	if val, ok := cacheMigrate.Load(dbName); ok {
		return val.(*Migrate)
	}
	// create new Migrate
	// save to cache
	// cacheMigrate.Store(dbName, ret)
	ret := &Migrate{
		sqlStore: p.GetSqlStore(dbName),
	}
	//set to cache
	cacheMigrate.Store(dbName, ret)
	return ret

}
func (p *Postgres) GetColumnsInfoOfType(typ reflect.Type) (*common.TableInfo, error) {
	return common.GetTableInfoByType(p.cfg.Driver, typ, OnResolveColInfo)

}
func (p *Postgres) PingDb() error {

	cfg := p.GetConfig()
	dns := cfg.Dns("")
	sql, err := sql.Open(cfg.Driver, dns)
	if err != nil {
		return err
	}
	err = sql.Ping()
	if err != nil {
		return err
	}
	return nil
}
func New() isql.ISql {
	return &Postgres{}
}

// ================================================
func OnResolveColInfo(col *common.ColInfo) error {
	return nil
}

type PgColumnInfo struct {
	NotNull bool
	Default string
	Name    string
}

func getTableInfo(db *sql.DB, tableName string) ([]*PgColumnInfo, error) {
	var cols []*PgColumnInfo = make([]*PgColumnInfo, 0)
	SqlCommand := "SELECT column_name, data_type, is_nullable, column_default, ordinal_position FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + tableName + "'"

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
