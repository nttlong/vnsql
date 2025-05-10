package vnsql

import (
	"sync"
)

type DbConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	IsUseSSL bool
}
type IConnector interface {
	PingDb() error
	SetConfig(cfg DbConfig) error
	GetConfig() DbConfig
	GetStorage(dbName string) IStorage
}
type IStorage interface {
	SetConnector(conn IConnector)
	GetConnector() IConnector

	Connect() error
	GetDbName() string
}

//=================================

type MigrateHelper struct {
}

var migrateBase MigrateHelper
var one sync.Once

func NewHelper() MigrateHelper {
	one.Do(func() {
		migrateBase = MigrateHelper{}
	})
	return migrateBase
}

var tableInfoCache = sync.Map{}
