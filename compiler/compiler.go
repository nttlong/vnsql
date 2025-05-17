package compiler

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xwb1989/sqlparser"
)

type NodeType int
type SqlTypeEnum int

const (
	Unknown SqlTypeEnum = iota
	Insert
	Update
	Delete
	Select
)

func (s SqlTypeEnum) String() string {
	names := [...]string{
		"Unknown",
		"Insert",
		"Update",
		"Delete",
		"Select",
	}
	if s < Insert || s > Select {
		return "Unknown"
	}
	return names[s]
}

const (
	Selector NodeType = iota
	Value
	Binary
	Unary
	Field
	Alias
	Function
	Params
	OrderBy
	Where
	Join
	TableName
	OffsetAndLimit
	Using
)

// Optional: Add a String() method for better readability
func (n NodeType) String() string {
	names := [...]string{
		"Selector",
		"Binary",
		"Field",
		"Alias",
		"Function",
		"Params",
		"OrderBy",
		"Where",
		"Join",
	}

	if n < Selector || n > Field {
		return "Unknown"
	}

	return names[n]
}

type UsingNodeOnDelete struct {
	TableName   string
	Where       string
	TargetTable string
	ReNewSQL    string
}
type ParseContext struct {
	SqlNodes  []sqlparser.SQLNode
	TableName string
	Alias     string
	SqlType   SqlTypeEnum
}

type TableMap map[string]string
type DbDllCommand struct {
	DbName      *string
	CommandText *string
}
type Node struct {
	Nt     NodeType
	V      string // Value of the node
	C      []Node // Children of the node
	Offset string
	Limit  string
	Un     *UsingNodeOnDelete
}

// type DbDmlCmds []*DbDmlCommand

// func (dd *DBDDLCommand) String() string {
// 	db := "owner"
// 	if dd.DbName != nil {
// 		db = *dd.DbName

// 	}
// 	return fmt.Sprintf("%s in db %s", *dd.CommandText, db)
// }

// func (dd DBDDLCmds) String() string {
// 	var sb strings.Builder
// 	for _, d := range dd {
// 		sb.WriteString(d.String())
// 		sb.WriteString(";")
// 	}
// 	return sb.String()
// }

//---------------------------------

// SQLParseInfo is the result of parsing a SQL statement
type SQLParseInfo struct {
	SQL    string
	Params []interface{}
}
type DbTableDictionaryItem struct {
	TableName string
	Cols      map[string]string
}

//	type DbDictionary struct {
//		DbName string
//		Tables map[string]DbTableDictionaryItem
//	}
type QuoteIdentifier struct {
	Left  string
	Right string
}
type Compiler struct {
	TableDict map[string]DbTableDictionaryItem
	FieldDict map[string]string
	Quote     QuoteIdentifier

	// Some RDBMS need special parse for insert sql

}

func (q *QuoteIdentifier) Quote(s ...string) string {
	if len(s) == 0 {
		return ""
	}

	rets := []string{}
	for _, v := range s {
		rets = append(rets, q.Left+v+q.Right)

	}
	return strings.Join(rets, ".")

}

//	func (w Compiler) GetDbDict(dbName string) (DbDictionary, bool) {
//		if dbDict, ok := w.DbDict[strings.ToLower(dbName)]; ok {
//			return dbDict, true
//		}
//		return DbDictionary{}, false
//	}
//
//	func (w Compiler) AddDbDict(dbName string, dict DbDictionary) {
//		w.DbDict[strings.ToLower(dbName)] = dict
//	}
func (w Compiler) ParseInsertSQL(sql string, autoValueCols []string, returnColAfterInsert []string) (*string, error) {
	return nil, fmt.Errorf("not support yet")
}

func (w Compiler) Parse(sql string) (string, error) {
	if cached, ok := cacheSqlParse.Load(sql); ok {
		return cached.(SQLParseInfo).SQL, nil
	}
	sql, err := w.parse(sql)
	if err != nil {
		return "", err
	}

	sql = strings.TrimLeft(sql, " ")
	sql = strings.TrimRight(sql, " ")
	sql = strings.Replace(sql, "  ", " ", -1)
	cacheSqlParse.Store(sql, SQLParseInfo{SQL: sql, Params: nil})
	return sql, nil
}

// --------------PRIVATE-----------------
var cacheSqlParse sync.Map
var paramPrefix []string = []string{"@", ":"}

func isParam(s string) (string, bool) {
	if len(s) < 2 {
		return "", false
	}
	for _, p := range paramPrefix {
		if strings.HasPrefix(s, p) {
			return s[1:], true
		}
	}
	return "", false
}
func (n *Node) IsNumber() (interface{}, bool) {
	s := n.V
	if ret, err := strconv.ParseFloat(s, 64); err == nil {
		return ret, true
	}
	if ret, err := strconv.ParseFloat(s, 64); err == nil {
		return ret, true
	}
	if ret, err := strconv.ParseInt(s, 64, 64); err == nil {
		return ret, true
	}
	return nil, false
}
func (n *Node) IsDate() (*time.Time, bool) {
	if ret, err := time.Parse("2006-01-02", n.V); err == nil {
		return &ret, true
	}
	if ret, err := time.Parse("2006-01-02 15:04:05", n.V); err == nil {
		return &ret, true
	}
	return nil, false
}
func (n *Node) IsBool() (bool, bool) {
	if ret, err := strconv.ParseBool(n.V); err == nil {
		return ret, true
	}
	return false, false
}
func (w Compiler) parse(sql string) (string, error) {
	parseCtx := ParseContext{
		SqlNodes:  []sqlparser.SQLNode{},
		TableName: "",
		Alias:     "",
	}
	sql = " " + sql
	stm, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	if _, ok := stm.(*sqlparser.DBDDL); ok {
		panic(fmt.Sprintf("not support ddl: %s. Please call Walker.ParseDBDLL instead ", sql))

	}
	sql = " " + sql
	if !strings.Contains(strings.ToLower(sql), " from ") &&
		!strings.Contains(strings.ToLower(sql), " insert ") &&
		!strings.Contains(strings.ToLower(sql), " update ") &&
		!strings.Contains(strings.ToLower(sql), " delete ") {
		if fx, ok := stm.(*sqlparser.Select); ok {
			return w.walkOnSelectOnly(fx, &parseCtx)
		}
		return "", fmt.Errorf("not support sql: %s", sql)

	}
	ret, err := w.walkOnStatement(stm, &parseCtx)
	if err != nil {
		return "", err
	}

	return ret, nil
}
func (w Compiler) walkOnSelectOnly(stmt *sqlparser.Select, ctx *ParseContext) (string, error) {
	selector := []string{}
	for _, sel := range stmt.SelectExprs {

		s, err := w.walkSQLNode(sel, ctx)
		if err != nil {
			return "", err
		}
		selector = append(selector, s)
	}
	return "SELECT " + strings.Join(selector, ", "), nil
}
func (w Compiler) walkSQLNode(node sqlparser.SQLNode, ctx *ParseContext) (string, error) {

	if fx, ok := node.(*sqlparser.StarExpr); ok {

		if !fx.TableName.IsEmpty() {
			n, err := w.OnParse(Node{Nt: TableName, V: fx.TableName.Name.String()})
			if err != nil {
				return "", err
			}
			return n.V + ".*", nil

		}
		if ctx.TableName != "" {
			return ctx.TableName + ".*", nil
		}
		return "*", nil
	}
	if fx, ok := node.(*sqlparser.AliasedExpr); ok {
		if fx.As.IsEmpty() {
			return w.walkSQLNode(fx.Expr, ctx)

		}

		nAlias, err := w.OnParse(Node{Nt: Alias, V: fx.As.String()})

		if err != nil {
			return "", err
		}
		n, err := w.walkSQLNode(fx.Expr, ctx)
		if err != nil {
			return "", err
		}
		return n + " AS " + nAlias.V, nil
	}
	if fx, ok := node.(*sqlparser.SQLVal); ok {
		strVal := string(fx.Val)
		if pName, ok := isParam(strVal); ok {
			n, err := w.OnParse(Node{Nt: Params, V: pName})
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			n, err := w.OnParse(Node{Nt: Value, V: string(fx.Val)})
			if err != nil {
				return "", err
			}
			return n.V, nil
		}

	}
	if fx, ok := node.(*sqlparser.ColName); ok {
		return w.walkOnColName(fx, ctx)

	}
	if fx, ok := node.(*sqlparser.ParenExpr); ok {

		return w.walkSQLParen(fx, ctx)

	}
	if fx, ok := node.(*sqlparser.BinaryExpr); ok {

		return w.walkOnBinaryExpr(fx, ctx)

	}
	if fx, ok := node.(*sqlparser.FuncExpr); ok {
		return w.walkOnFuncExpr(fx, ctx)
	}
	if fx, ok := node.(*sqlparser.JoinTableExpr); ok {
		return w.walkOnJoinTable(fx, ctx)
	}
	if fx, ok := node.(sqlparser.TableExprs); ok {

		return w.walkOnTable(fx, ctx)

	}
	if fx, ok := node.(sqlparser.TableName); ok {
		if fx.Name.IsEmpty() {
			return "", nil
		}
		strAlias := ""
		if !fx.Qualifier.IsEmpty() {
			strAliasGet, err := w.walkSQLNode(fx.Qualifier, ctx)
			if err != nil {
				return "", err
			}
			strAlias = strAliasGet

		}
		ctx.SqlNodes = append(ctx.SqlNodes, fx)
		n, err := w.OnParse(Node{Nt: TableName, V: fx.Name.String()})
		if err != nil {
			return "", err
		}
		if strAlias != "" {
			return strAlias + "." + n.V, nil
		}
		return n.V, nil

	}
	if fx, ok := node.(*sqlparser.Where); ok {
		return w.walkOnWhere(fx, ctx)

	}
	if fx, ok := node.(*sqlparser.ComparisonExpr); ok {
		strLeft, err := w.walkSQLNode(fx.Left, ctx)
		if err != nil {
			return "", err
		}
		strRight, err := w.walkSQLNode(fx.Right, ctx)
		if err != nil {
			return "", err
		}
		return strLeft + " " + fx.Operator + " " + strRight, nil
	}
	if fx, ok := node.(*sqlparser.AliasedTableExpr); ok {
		if fx.As.IsEmpty() {
			return w.walkSQLNode(fx.Expr, ctx)
		} else {
			nAlias, err := w.OnParse(Node{Nt: Alias, V: fx.As.String()})
			if err != nil {
				return "", err
			}
			n, err := w.walkSQLNode(fx.Expr, ctx)
			if err != nil {
				return "", err
			}
			return n + " AS " + nAlias.V, nil

		}
	}
	if fx, ok := node.(*sqlparser.JoinTableExpr); ok {
		return w.walkOnJoinTable(fx, ctx)
	}
	if fx, ok := node.(sqlparser.JoinCondition); ok {
		retStr, err := w.walkSQLNode(fx.On, ctx)
		if err != nil {
			return "", err
		}
		retStrUsing, err := w.walkSQLNode(fx.Using, ctx)
		if err != nil {
			return "", err
		}
		if retStrUsing != "" {
			return retStr + " USING " + retStrUsing, nil
		}
		return retStr, nil
	}
	if fx, ok := node.(sqlparser.Columns); ok {
		ret := []string{}
		for _, col := range fx {
			s, err := w.walkSQLNode(col, ctx)
			if err != nil {
				return "", err
			}
			ret = append(ret, s)
		}
		return strings.Join(ret, ", "), nil
	}
	if fx, ok := node.(sqlparser.GroupBy); ok {
		return w.walkOnGroupBy(fx, ctx)
	}
	if fx, ok := node.(*sqlparser.Select); ok {
		return w.walkOnSelect(fx, ctx)
	}
	if fx, ok := node.(*sqlparser.OrderBy); ok {
		return w.walkOnOrderBy(fx, ctx)
	}
	if fx, ok := node.(sqlparser.OrderBy); ok {
		return w.walkOnOrderBy(&fx, ctx)
	}
	if fx, ok := node.(*sqlparser.Subquery); ok {
		return w.walkOnSubquery(*fx, ctx)

	}
	if fx, ok := node.(sqlparser.ColIdent); ok {
		return w.walkOnColIdent(fx, ctx)

	}
	if fx, ok := node.(*sqlparser.Limit); ok {
		if fx.Offset == nil && fx.Rowcount == nil {
			return "", fmt.Errorf("syntax error")
		}
		if fx.Offset == nil && fx.Rowcount != nil {
			rc, err := w.walkSQLNode(fx.Rowcount, ctx)
			if err != nil {
				return "", err
			}
			n, err := w.OnParse(Node{Nt: OffsetAndLimit, Limit: rc})
			if err != nil {
				return "", err
			}
			if n.V == "" {
				errMsg := fmt.Errorf("It looks like you forget handle Nt value OffsetAndLimit in Resolver function")
				return "", errMsg
			}
			return "LIMIT " + n.V, nil
		}

		if fx.Offset != nil && fx.Rowcount == nil {
			rc, err := w.walkSQLNode(fx.Offset, ctx)
			if err != nil {
				return "", err
			}
			n, err := w.OnParse(Node{Nt: OffsetAndLimit, Offset: rc})
			if err != nil {
				return "", err
			}
			if n.V == "" {
				errMsg := fmt.Errorf("It looks like you forget handle Nt value OffsetAndLimit in Resolver function")
				return "", errMsg
			}
			return "LIMIT " + n.V, nil
		}
		if fx.Offset != nil && fx.Rowcount != nil {
			ofs, err := w.walkSQLNode(fx.Offset, ctx)
			if err != nil {
				return "", err
			}
			rc, err := w.walkSQLNode(fx.Rowcount, ctx)
			if err != nil {
				return "", err
			}
			n, err := w.OnParse(Node{Nt: OffsetAndLimit, Offset: ofs, Limit: rc})
			if err != nil {
				return "", err
			}
			if n.V == "" {
				errMsg := fmt.Errorf("It looks like you forget handle Nt value OffsetAndLimit in Resolver function")
				return "", errMsg
			}
			return n.V, nil
		}

	}
	if fx, ok := node.(*sqlparser.AndExpr); ok {
		strL, err := w.walkSQLNode(fx.Left, ctx)
		if err != nil {
			return "", err
		}
		strR, err := w.walkSQLNode(fx.Right, ctx)
		if err != nil {
			return "", err
		}
		return strL + " AND " + strR, nil
	}
	if fx, ok := node.(*sqlparser.OrExpr); ok {
		strL, err := w.walkSQLNode(fx.Left, ctx)
		if err != nil {
			return "", err
		}
		strR, err := w.walkSQLNode(fx.Right, ctx)
		if err != nil {
			return "", err
		}
		return strL + " OR " + strR, nil
	}
	if fx, ok := node.(*sqlparser.NotExpr); ok {
		strL, err := w.walkSQLNode(fx.Expr, ctx)
		if err != nil {
			return "", err
		}
		return "NOT " + strL, nil

	}
	if fx, ok := node.(sqlparser.TableNames); ok {
		ret := []string{}
		for _, tbl := range fx {
			strTbl, err := w.walkSQLNode(tbl, ctx)
			if err != nil {
				return "", err
			}
			ret = append(ret, strTbl)
		}
		return strings.Join(ret, ", "), nil
	}
	if fx, ok := node.(*sqlparser.CaseExpr); ok {
		return w.walkOnCaseExpr(fx, ctx)
	}
	if fx, ok := node.(sqlparser.BoolVal); ok {
		str := "true"
		if !fx {
			str = "false"
		}
		n, err := w.OnParse(Node{Nt: Value, V: str})
		if err != nil {
			return "", err
		}
		return n.V, nil
	}
	if _, ok := node.(*sqlparser.NullVal); ok {
		return "NULL", nil
	}

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLNode", reflect.TypeOf(node)))

}
func (w Compiler) walkOnCaseExpr(expr *sqlparser.CaseExpr, ctx *ParseContext) (string, error) {
	ret := []string{}
	for _, when := range expr.Whens {
		whenStr, err := w.walkSQLNode(when.Cond, ctx)
		if err != nil {
			return "", err
		}
		thenStr, err := w.walkSQLNode(when.Val, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHEN "+whenStr+" THEN "+thenStr)
	}
	if expr.Else != nil {
		elseStr, err := w.walkSQLNode(expr.Else, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, "ELSE "+elseStr)
	}
	return "CASE " + strings.Join(ret, " ") + " END", nil
}
func (w Compiler) walkOnStatement(stmt sqlparser.Statement, ctx *ParseContext) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Union:
		return w.walkOnUnion(stmt, ctx)
	case *sqlparser.Select:
		return w.walkOnSelect(stmt, ctx)
	case *sqlparser.Insert:
		return w.walkOnInsert(stmt, ctx)
	case *sqlparser.Update:
		return w.walkOnUpdate(stmt, ctx)
	case *sqlparser.Delete:
		return w.walkOnDelete(stmt, ctx)
	default:
		panic(fmt.Sprintf("unsupported statement type %T", stmt))
	}

}
func (w Compiler) walkOnUnion(stmt *sqlparser.Union, ctx *ParseContext) (string, error) {
	left, err := w.walkSQLNode(stmt.Left, ctx)
	if err != nil {
		return "", err
	}
	right, err := w.walkSQLNode(stmt.Right, ctx)
	if err != nil {
		return "", err
	}
	return left + " " + stmt.Type + " " + right, nil
}

func (w Compiler) walkOnSelect(stmt *sqlparser.Select, ctx *ParseContext) (string, error) {
	ret := []string{}

	strFrom := ""
	strSelect := ""

	if stmt.From != nil {
		from, err := w.walkSQLNode(stmt.From, ctx)
		if err != nil {
			return "", err
		}
		strFrom = "FROM " + from

	}
	for _, sel := range stmt.SelectExprs {

		s, err := w.walkSQLNode(sel, ctx)
		if err != nil {
			return "", err
		}
		strSelect = "SELECT " + s
	}
	ret = append(ret, strSelect, strFrom)

	if stmt.GroupBy != nil {
		groupBy, err := w.walkSQLNode(stmt.GroupBy, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, "GROUP BY "+groupBy)

	}
	if stmt.Having != nil {
		groupBy, err := w.walkSQLNode(stmt.Having, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, "HAVING "+groupBy)
	}
	if stmt.Where != nil {
		where, err := w.walkSQLNode(stmt.Where, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHERE "+where)
	}

	if stmt.OrderBy != nil {
		orderBy, err := w.walkSQLNode(stmt.OrderBy, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, "ORDER BY "+orderBy)
	}

	if stmt.Limit != nil {
		limit, err := w.walkSQLNode(stmt.Limit, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, limit)
	}

	return strings.Join(ret, " "), nil

}
func (w Compiler) walkOnInsert(stmt *sqlparser.Insert, ctx *ParseContext) (string, error) {
	ctx.SqlType = Insert
	tableName, err := w.walkSQLNode(stmt.Table, ctx)
	if err != nil {
		return "", err
	}
	cols := []string{}

	for _, col := range stmt.Columns {
		colName, err := w.walkSQLNode(col, ctx)
		if err != nil {
			return "", err
		}
		cols = append(cols, colName)
	}

	if fx, ok := stmt.Rows.(*sqlparser.Select); ok {
		ctx.SqlType = Select
		sqlSelect, err := w.walkOnSelect(fx, ctx)
		if err != nil {
			return "", err
		}
		return "INSERT INTO " + tableName + " (" + strings.Join(cols, ", ") + ") " + sqlSelect, nil
	}
	if fx, ok := stmt.Rows.(sqlparser.Values); ok {
		values := []string{}
		for _, row := range fx {
			rowStr := []string{}
			for _, val := range row {
				valStr, err := w.walkSQLNode(val, ctx)
				if err != nil {
					return "", err
				}
				rowStr = append(rowStr, valStr)
			}
			values = append(values, "("+strings.Join(rowStr, ", ")+")")
		}
		return "INSERT INTO " + tableName + " (" + strings.Join(cols, ", ") + ") VALUES " + strings.Join(values, ", "), nil
	}
	panic(fmt.Sprintf("unsupported type %s in parser.walkOnInsert", reflect.TypeOf(stmt.Rows)))
}

func (w Compiler) walkOnUpdate(stmt *sqlparser.Update, ctx *ParseContext) (string, error) {
	ctx.SqlType = Update
	tableName, err := w.walkSQLNode(stmt.TableExprs, ctx)
	if err != nil {
		return "", err
	}
	ret := []string{}
	for _, col := range stmt.Exprs {
		colName, err := w.walkSQLNode(col.Name, ctx)
		if err != nil {
			return "", err
		}
		colValue, err := w.walkSQLNode(col.Expr, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, colName+" = "+colValue)
	}
	ctx.SqlType = Unknown
	where := ""
	if stmt.Where != nil {
		where, err = w.walkSQLNode(stmt.Where, ctx)
		if err != nil {
			return "", err
		}

	}
	return "UPDATE " + tableName + " SET " + strings.Join(ret, ", ") + " WHERE " + where, nil

}
func (w Compiler) walkOnDelete(stmt *sqlparser.Delete, ctx *ParseContext) (string, error) {

	tableName, err := w.walkSQLNode(stmt.Targets, ctx)
	if err != nil {
		return "", err
	}
	tableNameUsing, err := w.walkSQLNode(stmt.TableExprs, ctx)
	if err != nil {
		return "", err
	}

	strWhere, err := w.walkSQLNode(stmt.Where, ctx)

	if err != nil {
		return "", err
	}
	n, err := w.OnParse(Node{
		Nt: Using, Un: &UsingNodeOnDelete{
			TableName:   tableNameUsing,
			Where:       strWhere,
			TargetTable: tableName,
		},
	})
	if err != nil {
		return "", err
	}
	if n.Un != nil && n.Un.ReNewSQL != "" {
		return n.Un.ReNewSQL, nil
	}
	if tableName == "" {
		return "DELETE FROM " + tableNameUsing + " WHERE " + strWhere, nil
	}
	return "DELETE FROM " + tableName + " USING " + tableNameUsing + " WHERE " + strWhere, nil
}
func (w Compiler) walkOnJoinTable(expr *sqlparser.JoinTableExpr, ctx *ParseContext) (string, error) {
	strLeft, err := w.walkSQLNode(expr.LeftExpr, ctx)
	if err != nil {
		return "", err
	}

	strRight, err := w.walkSQLNode(expr.RightExpr, ctx)
	if err != nil {
		return "", err
	}
	var leftExpr *sqlparser.AliasedTableExpr = nil
	var rightExpr *sqlparser.AliasedTableExpr = nil
	if l, ok := expr.LeftExpr.(*sqlparser.AliasedTableExpr); ok && !l.As.IsEmpty() {
		leftExpr = l
	}
	if r, ok := expr.RightExpr.(*sqlparser.AliasedTableExpr); ok {
		rightExpr = r
	}
	ctx.SqlNodes = []sqlparser.SQLNode{leftExpr, rightExpr}

	strConditional, err := w.walkSQLNode(expr.Condition, ctx)

	if err != nil {
		return "", err
	}
	ret := strLeft + " " + expr.Join + " " + strRight + " ON " + strConditional
	return ret, nil
}
func (w Compiler) walkOnOrderBy(expr *sqlparser.OrderBy, ctx *ParseContext) (string, error) {
	ret := []string{}
	for _, order := range *expr {
		str, err := w.walkSQLNode(order.Expr, ctx)
		if err != nil {
			return "", err
		}
		if order.Direction == sqlparser.AscScr {
			ret = append(ret, str+" ASC")
		} else {
			ret = append(ret, str+" DESC")
		}
	}
	return strings.Join(ret, ", "), nil
}

// func (w Compiler) ParseDBDLL(sql string) (DBDDLCmds, error) {
// 	if w.OnCreateDb == nil {
// 		return nil, fmt.Errorf("OnCreateDb is nil, please set it by using Walker.SetOnCreateDb")
// 	}
// 	ddl, err := sqlparser.Parse(sql)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if fx, ok := ddl.(*sqlparser.DBDDL); ok {
// 		fn := w.OnCreateDb
// 		dbName := fx.DBName
// 		return fn(dbName)
// 	} else {
// 		return nil, fmt.Errorf("not support ddl in Walker.ParseDBDLL : %s", sql)
// 	}

// }

func (w Compiler) walkOnSubquery(stmt sqlparser.Subquery, ctx *ParseContext) (string, error) {
	subquery, err := w.walkOnStatement(stmt.Select, ctx)
	if err != nil {
		return "", err
	}
	return "(" + subquery + ")", nil
}
func (w Compiler) walkOnGroupBy(stmt sqlparser.GroupBy, ctx *ParseContext) (string, error) {
	ret := []string{}
	for _, expr := range stmt {
		s, err := w.walkSQLNode(expr, ctx)
		if err != nil {
			return "", err
		}
		ret = append(ret, s)
	}
	return strings.Join(ret, ", "), nil
}
func (w Compiler) walkOnWhere(stmt *sqlparser.Where, ctx *ParseContext) (string, error) {

	if stmt.Expr != nil {
		where, err := w.walkSQLNode(stmt.Expr, ctx)
		if err != nil {
			return "", err
		}
		return where, nil
	}
	return "", fmt.Errorf("syntax error")
}
func (w Compiler) walkOnTable(expr sqlparser.TableExprs, ctx *ParseContext) (string, error) {
	ret := []string{}
	for _, expr := range expr {
		if tbl, ok := expr.(*sqlparser.AliasedTableExpr); ok {

			strTableName, err := w.walkSQLNode(tbl.Expr, ctx)
			ctx.SqlNodes = append(ctx.SqlNodes, tbl.Expr)
			if err != nil {
				return "", err
			}

			if !tbl.As.IsEmpty() {
				n, err := w.OnParse(Node{Nt: Alias, V: tbl.As.String()})
				if err != nil {
					return "", err
				}
				strTableName = strTableName + " AS " + n.V
			}

			ret = append(ret, strTableName)
			continue
		}
		if tbl, ok := expr.(*sqlparser.JoinTableExpr); ok {
			strJoin, err := w.walkOnJoinTable(tbl, ctx)
			if err != nil {
				return "", err
			}
			ret = append(ret, strJoin)
			continue
		}
		panic(fmt.Sprintf("unsupported type %s in parser.walkOnTable", reflect.TypeOf(expr)))
	}
	return strings.Join(ret, ", "), nil
}
func (w Compiler) walkOnFuncExpr(expr *sqlparser.FuncExpr, ctx *ParseContext) (string, error) {
	funcName := expr.Name.String()
	n, err := w.OnParse(Node{Nt: Function, V: funcName})
	if err != nil {
		return "", err
	}
	params := []string{}
	for _, p := range expr.Exprs {
		s, err := w.walkSQLNode(p, ctx)
		if err != nil {
			return "", err
		}
		params = append(params, s)
	}
	return n.V + "(" + strings.Join(params, ", ") + ")", nil
}
func (w Compiler) walkOnColName(expr *sqlparser.ColName, ctx *ParseContext) (string, error) {
	for _, x := range ctx.SqlNodes {
		if !expr.Qualifier.IsEmpty() {
			if fx, ok := x.(*sqlparser.AliasedTableExpr); ok {

				if fx.As.String() == expr.Qualifier.Name.String() {
					return w.Quote.Quote(fx.As.String(), expr.Name.String()), nil
				}

			}
		}

		if fx, ok := x.(sqlparser.TableName); ok {
			tableName := fx.Name.String()
			if expr.Qualifier.Name.String() == "" {
				returnFieldName := tableName + "." + expr.Name.String()
				n, err := w.OnParse(Node{Nt: Field, V: returnFieldName})
				if err != nil {
					return "", err
				}
				return n.V, nil

			}
		}
		if fx, ok := x.(*sqlparser.TableName); ok {
			tableName := fx.Name.String()
			if tableName == expr.Qualifier.Name.String() {
				return w.Quote.Left + tableName + w.Quote.Right + "." + w.Quote.Left + expr.Name.String() + w.Quote.Right, nil
			}
		}

	}
	fieldName := expr.Name.String()
	fieldName = strings.TrimLeft(fieldName, " ")

	if strings.HasPrefix(fieldName, "@") {
		n, err := w.OnParse(

			Node{
				Nt: Params, V: fieldName[1:]},
		)
		if err != nil {
			return "", err
		}
		return n.V, nil
	} else {

		tblName := ""
		if !expr.Qualifier.IsEmpty() {
			tblName = expr.Qualifier.Name.String()
			n, err := w.OnParse(Node{Nt: Field, V: tblName + "." + fieldName})

			if err != nil {
				return "", err
			}
			return n.V, nil
		}
		if ctx.TableName != "" {

			n, err := w.OnParse(Node{Nt: Field, V: ctx.TableName + "." + fieldName})
			if err != nil {
				return "", err
			}
			return n.V, nil

		}

		n, err := w.OnParse(Node{Nt: Field, V: fieldName})

		if err != nil {
			return "", err
		}
		tableName, err := w.walkSQLNode(expr.Qualifier, ctx)
		if err != nil {
			return "", err
		}
		if tableName != "" {
			return tableName + "." + n.V, nil
		}
		return n.V, nil
	}
}

func (w Compiler) walkSQLParen(expr *sqlparser.ParenExpr, ctx *ParseContext) (string, error) {

	cExpr := expr.Expr
	if fx, ok := cExpr.(*sqlparser.BinaryExpr); ok {
		strExpr, err := w.walkOnBinaryExpr(fx, ctx)
		if err != nil {
			return "", err
		}
		return "(" + strExpr + ")", nil

	}

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLParen", reflect.TypeOf(cExpr)))
}
func (w Compiler) walkOnBinaryExpr(expr *sqlparser.BinaryExpr, ctx *ParseContext) (string, error) {
	op, err := w.OnParse(Node{Nt: Unary, V: expr.Operator})
	if err != nil {
		return "", err
	}
	left, err := w.walkSQLNode(expr.Left, ctx)
	if err != nil {
		return "", err
	}
	right, err := w.walkSQLNode(expr.Right, ctx)
	if err != nil {
		return "", err
	}
	return left + " " + op.V + " " + right, nil
}
func (w Compiler) walkOnColIdent(expr sqlparser.ColIdent, ctx *ParseContext) (string, error) {
	for _, x := range ctx.SqlNodes {
		if fx, ok := x.(*sqlparser.TableName); ok {
			n, err := w.OnParse(Node{Nt: Field, V: fx.Name.String() + "." + expr.String()})
			if err != nil {
				return "", err
			}
			if strings.Contains(n.V, ".") {

				return strings.Split(n.V, ".")[1], nil

			}
			return n.V, nil
		}
		if fx, ok := x.(sqlparser.TableName); ok {
			n, err := w.OnParse(Node{Nt: Field, V: fx.Name.String() + "." + expr.String()})
			if err != nil {
				return "", err
			}
			if ctx.SqlType == Insert {
				if strings.Contains(n.V, ".") {

					return strings.Split(n.V, ".")[1], nil

				}
				return n.V, nil
			}
			return n.V, nil
		}

	}
	n, err := w.OnParse(Node{Nt: Field, V: expr.String()})
	if err != nil {
		return "", err
	}
	return n.V, nil
}

func (w Compiler) OnParse(node Node) (Node, error) {
	if node.Nt == Value {
		if v, ok := node.IsBool(); ok {
			if v {
				node.V = "TRUE"
			} else {
				node.V = "FALSE"
			}
		}
		if _, ok := node.IsDate(); ok {
			return node, nil
		}
		if _, ok := node.IsNumber(); ok {
			return node, nil
		}
		//escape "'" in node.V
		node.V = "'" + strings.Replace(node.V, "'", "''", -1) + "'"
		return node, nil
	}
	if node.Nt == TableName {
		tableNameLower := strings.ToLower(node.V)
		if matchTableName, ok := w.TableDict[tableNameLower]; ok {
			node.V = w.Quote.Left + matchTableName.TableName + w.Quote.Right
			return node, nil
		}
	}
	if node.Nt == Alias {
		node.V = w.Quote.Left + node.V + w.Quote.Right
		return node, nil
	}
	if node.Nt == Field {
		fieldNameLower := strings.ToLower(node.V)

		if matchField, ok := w.FieldDict[fieldNameLower]; ok {

			if strings.Contains(matchField, ".") {
				tableName := strings.Split(matchField, ".")[0]
				fieldName := strings.Split(matchField, ".")[1]
				node.V = w.Quote.Left + tableName + w.Quote.Right + "." + w.Quote.Left + fieldName + w.Quote.Right
				return node, nil
			}
			node.V = w.Quote.Left + matchField + w.Quote.Right
			return node, nil
		} else {
			if strings.Contains(node.V, ".") {
				tableName := strings.Split(node.V, ".")[0]
				fieldName := strings.Split(node.V, ".")[1]
				node.V = w.Quote.Left + tableName + w.Quote.Right + "." + w.Quote.Left + fieldName + w.Quote.Right
				return node, nil
			}
			node.V = w.Quote.Left + node.V + w.Quote.Right
			return node, nil
		}

	}
	if node.Nt == Params {
		node.V = "$" + node.V[1:]
	}
	if node.Nt == Function {
		return w.OnParseFunction(node)

	}
	return node, nil

}
func (w Compiler) OnParseFunction(node Node) (Node, error) {
	if node.V == "now" {
		node.V = "NOW()"
	}
	if strings.ToLower(node.V) == "len" {
		node.V = "LENGTH"
	}
	return node, nil

}
func (w Compiler) LoadDbDictionary(db *sql.DB) error {
	// decalre sql get table and columns in postgres
	sqlGetTableAndColumns := "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, column_name"
	rows, err := db.Query(sqlGetTableAndColumns)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var fieldName string
		err = rows.Scan(&tableName, &fieldName)
		if err != nil {
			return err
		}
		tableNameLower := strings.ToLower(tableName)
		fieldNameLower := strings.ToLower(fieldName)
		if _, ok := w.TableDict[tableNameLower]; !ok {
			w.TableDict[tableNameLower] = DbTableDictionaryItem{
				TableName: tableName,
				Cols:      map[string]string{},
			}
		}
		if _, ok := w.FieldDict[fieldNameLower]; !ok {
			w.FieldDict[tableNameLower+"."+fieldNameLower] = tableName + "." + fieldName
		}
	}
	return nil
}
