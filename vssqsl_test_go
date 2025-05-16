package vnsql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nttlong/vnsql/utils"
	_ "github.com/nttlong/vnsql/utils"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type JobsProfile struct {
	ProfileId   int `db:"pk"`
	FromTime    time.Time
	ToTime      *time.Time
	Description string
	Point       float32 `db:"default:14.0"`
	OrderId     int32   `db:"df:auto"`
}
type BaseInfo struct {
	CreatedOn time.Time
	UpdatedOn time.Time
}
type Employee struct {
	BaseInfo

	Id        int    `db:"pk"`
	Name      string `db:"idx"`
	Code      string `db:"unique;varchar(10)"`
	Col1      *string
	Guild     uuid.UUID
	Guild1    *uuid.UUID
	TimeCol   time.Time
	TimeCol1  *time.Time
	ColIndex1 int            `db:"index:idx1"`
	ColIndex2 *time.Time     `db:"index:idx1"`
	COlUUID   *uuid.UUID     `db:"index:idx1"`
	Profile   []*JobsProfile `db:"foreignkey:ProfileId"`
	JobTime   time.Time
	StartTimr time.Time
	DeptId    *int
}
type Dept struct {
	BaseInfo
	Id                int         `db:"pk;df:auto"`
	Name              string      `db:"idx"`
	Code              string      `db:"unique;varchar(10)"`
	Emps              []*Employee `db:"foreignkey:DeptId"`
	CreateOn          time.Time   `db:"default:now()"`
	Description       string      `db:"df:''"`
	CreatedOn         time.Time   `db:"default:now()"`
	UpdatedOn         *time.Time
	CreatedBy         string     `db:"default:'system';idx"`
	UpdatedBy         *string    `db:"idx"`
	EstablishmentDate time.Time  `db:"idx;df:now()"`
	DissolutionDate   *time.Time `db:"idx"`
	SecretKey         uuid.UUID  `db:"default:gen_random_uuid()"`
}
type Dept2 struct {
	Id                int        `gorm:"column:Id;primaryKey;autoIncrement"`
	Name              string     `gorm:"column:Name;type:citext;not null"`
	Code              string     `gorm:"column:Code;type:citext;not null;size:10"`
	CreateOn          time.Time  `gorm:"column:CreateOn;type:timestamp without time zone;not null;default:CURRENT_TIMESTAMP"`
	Description       string     `gorm:"column:Description;type:citext;not null;default:''"`
	CreatedOn         time.Time  `gorm:"column:CreatedOn;type:timestamp without time zone;not null;default:CURRENT_TIMESTAMP"`
	UpdatedOn         *time.Time `gorm:"column:UpdatedOn;type:timestamp without time zone"`
	CreatedBy         string     `gorm:"column:CreatedBy;type:citext;not null;default:'system'"`
	UpdatedBy         *string    `gorm:"column:UpdatedBy;type:citext"`
	EstablishmentDate time.Time  `gorm:"column:EstablishmentDate;type:timestamp without time zone;not null;default:CURRENT_TIMESTAMP"`
	DissolutionDate   *time.Time `gorm:"column:DissolutionDate;type:timestamp without time zone"`
	SecretKey         uuid.UUID  `gorm:"column:SecretKey;type:uuid;not null;default:gen_random_uuid()"`
}

func TestUtisl(t *testing.T) {
	cfg := utils.DbCfg{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		UseSSL:   false,
	}
	ctx := utils.NewDbContext(cfg)

	err := ctx.Open()
	if err != nil {
		panic(err)
	}
	defer ctx.Close()
	err = ctx.Ping()
	if err != nil {
		panic(err)
	}
	tanetDb, err := ctx.CreateCtx("db_001124")
	if err != nil {
		panic(err)
	}
	tanetDb.Open()
	defer tanetDb.Close()
	et, err := tanetDb.Migrate(&Dept{})
	if err != nil {
		panic(err)
	}
	// tanetDb.Migrate(&Employee{})
	fmt.Print(et)
	totalElasped := int32(0)
	for i := 10000; i < 20000; i++ {
		start := time.Now()
		dep := Dept{
			Name: "test" + fmt.Sprintf("%04d", i),
			Code: "test" + fmt.Sprintf("%04d", i),
		}
		err = tanetDb.Insert(&dep)
		if err != nil {
			fmt.Print(err)
		}
		n := int32(time.Since(start).Milliseconds())
		totalElasped += n
		fmt.Printf("insert %d, time: %d\n", i, n)
	}
	avgTime := totalElasped / 10000
	fmt.Printf("avg time: %d ms\n", avgTime)

}
func TestUpdate(t *testing.T) {
	cfg := utils.DbCfg{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		UseSSL:   false,
	}
	ctx := utils.NewDbContext(cfg)

	err := ctx.Open()
	if err != nil {
		panic(err)
	}
	defer ctx.Close()
	err = ctx.Ping()
	assert.NoError(t, err)
	tanetDb, err := ctx.CreateCtx("db_001124")
	assert.NoError(t, err)
	tanetDb.Open()
	rs, err := tanetDb.Model(&Dept{}).Filter("code=?", "A0001").Update("name=?", "test_update")

	assert.NoError(t, err)
	fmt.Print(rs)
	rs, err = tanetDb.Model(&Dept{}).Filter("id>=1 and id<=10").Update("createdOn=?", time.Now())
	assert.NoError(t, err)
	fmt.Print(rs)

}
func TestFind(t *testing.T) {
	cfg := utils.DbCfg{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		UseSSL:   false,
	}
	ctx := utils.NewDbContext(cfg)

	err := ctx.Open()
	if err != nil {
		panic(err)
	}
	defer ctx.Close()
	err = ctx.Ping()
	assert.NoError(t, err)
	tanetDb, err := ctx.CreateCtx("db_001124")
	assert.NoError(t, err)
	tanetDb.Open()
	avgTime := int64(0)
	for i := 0; i < 1000; i++ {
		dep := []*Dept{}
		start := time.Now()
		err = tanetDb.Find(&dep, "id<=?", 10000)
		if err != nil {
			fmt.Println(err)
		}
		assert.NoError(t, err)
		n := time.Since(start).Milliseconds()
		avgTime += n
		fmt.Println(fmt.Sprintf("elapse time: %d total rows %d", n, len(dep)))
		assert.NoError(t, err)
		// assert.Equal(t, "test0010", dep[0].Code)
	}
	fmt.Print(fmt.Sprintf("avg elapse time: %d", avgTime/1000))

}

func TestGorm(t *testing.T) {
	cfg := utils.DbCfg{
		Driver:   "postgres",
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "123456",
		UseSSL:   false,
	}
	dns := cfg.GetDns("db_001124")
	// You can use this struct with GORM like this:
	db, err := gorm.Open(postgres.Open(dns), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema

	// if err != nil {
	// 	panic("failed to connect database")
	// }

	db.AutoMigrate(&Dept2{})
	totalTime := int64(0)
	for i := 0; i < 10000; i++ {
		start := time.Now()
		dd := time.Now().Add(time.Hour * 24)
		dep := Dept2{
			Name:              "test",
			Code:              "test" + fmt.Sprintf("%04d", i),
			CreatedBy:         "admin",
			EstablishmentDate: time.Now(),
			DissolutionDate:   &dd,
		}
		db.Create(&dep)
		n := time.Since(start).Abs().Milliseconds()
		totalTime += n
		if err != nil {
			fmt.Println(fmt.Sprintf("elapse time: %04d %s", n, err))
		} else {
			fmt.Println(fmt.Sprintf("elapse time: %04d", n))
		}
	}
	avgTime := totalTime / 10000
	fmt.Println(fmt.Sprintf("avg elapse time: %04d", avgTime))
}
