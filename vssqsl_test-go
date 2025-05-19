package vnsql_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nttlong/vnsql/types/info"
	"github.com/nttlong/vnsql/utils"
	_ "github.com/nttlong/vnsql/utils"
	"github.com/stretchr/testify/assert"
)

type UserInfo struct {
	Id       string  `db:"pk;varchar(36)"`
	Username string  `db:"unique;idx;varchar(100)"`
	Password string  `db:"varchar(100)"`
	Email    *string `db:"unique;idx;varchar(100)"`
	Phone    *string `db:"varchar(20)"`
	BaseInfo
	Employee *EmployeeInfo `db:"foreignkey:AccountId"`
}

type JobsProfile struct {
	ProfileId   int `db:"pk"`
	FromTime    time.Time
	ToTime      *time.Time
	Description string
	Point       float32 `db:"default:14.0"`
	OrderId     int32   `db:"df:auto"`
}
type BaseInfo struct {
	CreatedOn   time.Time `db:"default:now()"`
	CreatedBy   string    `db:"default:'system';idx;varchar(100)"`
	UpdatedBy   *string   `db:"idx;varchar(100)"`
	UpdatedOn   *time.Time
	Description *string
}
type PersonalInfo struct {
	Id        int    `db:"pk;df:auto"`
	FirstName string `db:"varchar(100);idx"`
	LastName  string `db:"varchar(100);idx"`
	Birthday  time.Time
	Gender    string        `db:"varchar(10)"`
	Email     string        `db:"unique;varchar(100)"`
	Phone     string        `db:"varchar(20)"`
	Employee  *EmployeeInfo `db:"foreignkey:Id"`
	LegalInfo *LegalInfo    `db:"foreignkey:Id"`
	BaseInfo
}
type IDCardInfo struct {
	IdCard  string `db:"unique;varchar(50)"`
	IssueOn time.Time
	IssueBy string `db:"varchar(100)"`
}
type LegalInfo struct {
	Id int `db:"pk;"`
	IDCardInfo
	OfficialResidence string `db:"varchar(300)"`
}

//	type CandidateInfo struct {
//		Persion   *PersonalInfo `db:"foreignkey:Id"`
//		LegalInfo *LegalInfo    `db:"foreignkey:Id"`
//		Id        int           `db:"pk"`
//		Title     string        `db:"varchar(100)"`
//	}
type EmployeeInfo struct {
	BaseInfo

	Id   int    `db:"pk;"`
	Code string `db:"unique;varchar(10)"`

	Profile []*JobsProfile `db:"foreignkey:ProfileId"`

	DeptId *int

	JointDate time.Time  `db:"idx"`
	QuitDate  *time.Time `db:"idx"`
	AccountId *string    `db:"unique;idx;varchar(36)"`
}
type Dept struct {
	BaseInfo
	Id              int             `db:"pk;df:auto"`
	Name            string          `db:"idx"`
	Code            string          `db:"unique;varchar(10)"`
	Emps            []*EmployeeInfo `db:"foreignkey:DeptId"`
	DecisionDate    *time.Time      `db:"idx"`
	DecisionNo      *string         `db:"idx''"`
	DissolutionDate *time.Time      `db:"idx"`
	ParentId        *int            `db:"idx"`
	LevelCode       string          `db:"varchar(512)"`
	ManagerId       *int
}
type MySTruct struct {
}

// Chi la test
func (t *MySTruct) SayHello() {

}
func TestUtisl(t *testing.T) {
	st := MySTruct{}
	st.SayHello()
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
func TestGeTableMapFromDB(t *testing.T) {
	utils.RegisterEntity(&Dept{}, &EmployeeInfo{}, &JobsProfile{}, &PersonalInfo{}, &UserInfo{})
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
	defer tanetDb.Close()
	tblInffo, err := tanetDb.GetTableMappingFromDb()
	assert.NoError(t, err)
	fmt.Println(tblInffo)
	tanetDb.Open()
}

func TestGetEmbededFronmReflectType(t *testing.T) {
	type B struct {
	}
	type A struct {
		B
		b *B
	}
	info.GetTableInfoByType(reflect.TypeOf(A{}))
	rt := reflect.TypeOf(A{})
	for i := 0; i < rt.NumField(); i++ {
		if rt.Field(i).Anonymous { // cho nay la nhung embedded struct dung kg
			fmt.Println(rt.Field(i).Name)
		}
		fmt.Println(rt.Field(i).Name)
	}
	// fmt.Println(rt.Field(0).Type.Elem().Name())
	// fmt.Println(rt.Field(0).Type.Elem().Field(0).Type.Name())
}
func TestCreatePersonal(t *testing.T) {
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
	tenantDb, err := ctx.CreateCtx("db_001124")
	assert.NoError(t, err)
	err = tenantDb.Open()
	if err != nil {
		panic(err)
	}
	defer tenantDb.Close()
	p := PersonalInfo{
		FirstName: "test",
		LastName:  "test",
		Birthday:  time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		Gender:    "male",
		Email:     "test@test.com5",
		Phone:     "0987654321",
	}
	err = tenantDb.Insert(&p)
	assert.NoError(t, err)
	fmt.Println(p.Id)
	r, err := tenantDb.Exec("delete from personalInfo where personalInfo.id<=?", 20)
	assert.NoError(t, err)
	fmt.Println(r.RowsAffected())

}
