package entities_example

import (
	"time"

	"google.golang.org/genproto/googleapis/type/decimal"
)

type Departments struct {
	Id          int         `db:"pk;df:auto"`
	Code        string      `db:"nvarchar(50);unique"`
	Name        string      `db:"nvarchar(50);idx"`
	ManagerId   int         `db:"fk(Employees.EmployeeId)"`
	Emps        []Employees `db:"fk:DepartmentId"`
	ParentId    *int        `db:"fk(Departments.DepartmentId)"`
	CreatedOn   time.Time   `db:"df:now();idx"`
	CreatedBy   string      `db:"nvarchar(50);idx"`
	UpdatedOn   *time.Time  `db:"idx"`
	UpdatedBy   *string     `db:"idx"`
	Description *string
}
type Employees struct {
	EmployeeId   int    `db:"pk;df:auto"`
	FirstName    string `db:"nvarchar(50);idx"`
	LastName     string `db:"nvarchar(50);idx"`
	Gender       bool
	PersonId     int    `db:"fk(Persons.PersonId)"`
	Title        string `db:"nvarchar(50)"`
	BasicSalary  decimal.Decimal
	DepartmentId int        `db:"fk(Departments.DepartmentId)"`
	CreatedOn    time.Time  `db:"df:now();idx"`
	CreatedBy    string     `db:"nvarchar(50);idx"`
	UpdatedOn    *time.Time `db:"idx"`
	UpdatedBy    *string    `db:"idx"`
	Description  *string
}
