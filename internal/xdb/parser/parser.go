package parser

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/xwb1989/sqlparser"
	_ "github.com/xwb1989/sqlparser"
	"vitess.io/vitess/go/mysql/decimal"
)

type NodeType int

const (
	Selector NodeType = iota
	Value
	Binary
	Unanry
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

// Node represents a SQL node (field, table, params, or function)
type UsingNodeOnDelete struct {
	TableName   string
	Where       string
	TargetTable string
	ReNewSQL    string
}
type Node struct {
	Nt     NodeType
	V      string // Value of the node
	C      []Node // Children of the node
	Offset string
	Limit  string
	Un     *UsingNodeOnDelete
}
type SQLParseInfo struct {
	SQL    string
	Params []interface{}
}
type TableMap map[string]map[string]string
type DBDDLCommand struct {
	DbName      *string
	CommandText *string
}

func (dd *DBDDLCommand) String() string {
	db := "owner"
	if dd.DbName != nil {
		db = *dd.DbName

	}
	return fmt.Sprintf("%s in db %s", *dd.CommandText, db)
}

type DBDDLCmds []*DBDDLCommand

func (dd DBDDLCmds) String() string {
	var sb strings.Builder
	for _, d := range dd {
		sb.WriteString(d.String())
		sb.WriteString(";")
	}
	return sb.String()
}

type Walker struct {
	Resolver          func(node Node, tblMap *TableMap) (Node, error)
	OnCreateDb        *func(dbName string) (DBDDLCmds, error)
	ResolverInsertSQL *func(sql string, tablIfo TableInfo) (*string, error)
}

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
func (n *Node) IsNumber(s string) (interface{}, bool) {
	if ret, err := strconv.ParseFloat(s, 64); err == nil {
		return ret, true
	}
	if ret, err := strconv.ParseFloat(s, 64); err == nil {
		return ret, true
	}
	return nil, false
}
func (n *Node) IsDate(s string) (*time.Time, bool) {
	if ret, err := time.Parse("2006-01-02", s); err == nil {
		return &ret, true
	}
	if ret, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return &ret, true
	}
	return nil, false
}
func (n *Node) IsBool(s string) (bool, bool) {
	if ret, err := strconv.ParseBool(s); err == nil {
		return ret, true
	}
	return false, false
}
func (w *Walker) ParseDBDLL(sql string) (DBDDLCmds, error) {
	if w.OnCreateDb == nil {
		return nil, fmt.Errorf("OnCreateDb is nil, please set it by using Walker.SetOnCreateDb")
	}
	ddl, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, err
	}
	if fx, ok := ddl.(*sqlparser.DBDDL); ok {
		fn := *w.OnCreateDb
		dbName := fx.DBName
		return fn(dbName)
	} else {
		return nil, fmt.Errorf("not support ddl in Walker.ParseDBDLL : %s", sql)
	}

}

func (w *Walker) Parse(sql string, tblMap *TableMap) (string, error) {
	sql = " " + sql
	stm, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	if _, ok := stm.(*sqlparser.DBDDL); ok {
		panic(fmt.Sprintf("not support ddl: %s. Please call Walker.ParseDBDLL instead ", sql))

	}
	if !strings.Contains(strings.ToLower(sql), " from ") &&
		!strings.Contains(strings.ToLower(sql), " insert ") &&
		!strings.Contains(strings.ToLower(sql), " update ") &&
		!strings.Contains(strings.ToLower(sql), " delete ") {
		if fx, ok := stm.(*sqlparser.Select); ok {
			return w.walkOnSelectOnly(fx, tblMap)
		}
		return "", fmt.Errorf("not support sql: %s", sql)

	}
	return w.walkOnStatement(stm, tblMap)
}
func (w *Walker) walkOnStatement(stmt sqlparser.Statement, tblMap *TableMap) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Union:
		return w.walkOnUnion(stmt, tblMap)
	case *sqlparser.Select:
		return w.walkOnSelect(stmt, tblMap)
	case *sqlparser.Insert:
		return w.walkOnInsert(stmt, tblMap)
	case *sqlparser.Update:
		return w.walkOnUpdate(stmt, tblMap)
	case *sqlparser.Delete:
		return w.walkOnDelete(stmt, tblMap)
	default:
		panic(fmt.Sprintf("unsupported statement type %T", stmt))
	}

}
func (w *Walker) walkSQLNode(node sqlparser.SQLNode, tblMap *TableMap) (string, error) {

	if fx, ok := node.(*sqlparser.StarExpr); ok {

		if !fx.TableName.IsEmpty() {
			n, err := w.Resolver(Node{Nt: TableName, V: fx.TableName.Name.String()}, tblMap)
			if err != nil {
				return "", err
			}
			return n.V + ".*", nil

		}
		return "*", nil
	}
	if fx, ok := node.(*sqlparser.AliasedExpr); ok {
		if fx.As.IsEmpty() {
			return w.walkSQLNode(fx.Expr, tblMap)

		}

		nAlias, err := w.Resolver(Node{Nt: Alias, V: fx.As.String()}, tblMap)

		if err != nil {
			return "", err
		}
		n, err := w.walkSQLNode(fx.Expr, tblMap)
		if err != nil {
			return "", err
		}
		return n + " AS " + nAlias.V, nil
	}
	if fx, ok := node.(*sqlparser.SQLVal); ok {
		strVal := string(fx.Val)
		if pName, ok := isParam(strVal); ok {
			n, err := w.Resolver(Node{Nt: Params, V: pName}, tblMap)
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			n, err := w.Resolver(Node{Nt: Value, V: string(fx.Val)}, tblMap)
			if err != nil {
				return "", err
			}
			return n.V, nil
		}

	}
	if fx, ok := node.(*sqlparser.ColName); ok {
		fieldName := fx.Name.String()
		fieldName = strings.TrimLeft(fieldName, " ")

		if strings.HasPrefix(fieldName, "@") {
			n, err := w.Resolver(Node{Nt: Params, V: fieldName[1:]}, tblMap)
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			n, err := w.Resolver(Node{Nt: Field, V: fieldName}, tblMap)
			if err != nil {
				return "", err
			}
			tableName, err := w.walkSQLNode(fx.Qualifier, tblMap)
			if err != nil {
				return "", err
			}
			if tableName != "" {
				return tableName + "." + n.V, nil
			}
			return n.V, nil
		}

	}
	if fx, ok := node.(*sqlparser.ParenExpr); ok {

		return w.walkSQLParen(fx, tblMap)

	}
	if fx, ok := node.(*sqlparser.BinaryExpr); ok {

		return w.walkOnBinaryExpr(fx, tblMap)

	}
	if fx, ok := node.(*sqlparser.FuncExpr); ok {
		return w.walkOnFuncExpr(fx, tblMap)
	}
	if fx, ok := node.(*sqlparser.JoinTableExpr); ok {
		return w.walkOnJoinTable(fx, tblMap)
	}
	if fx, ok := node.(sqlparser.TableExprs); ok {

		return w.walkOnTable(fx, tblMap)
	}
	if fx, ok := node.(sqlparser.TableName); ok {
		if fx.Name.IsEmpty() {
			return "", nil
		}
		strAlias := ""
		if !fx.Qualifier.IsEmpty() {
			strAliasGet, err := w.walkSQLNode(fx.Qualifier, tblMap)
			if err != nil {
				return "", err
			}
			strAlias = strAliasGet

		}
		n, err := w.Resolver(Node{Nt: TableName, V: fx.Name.String()}, tblMap)
		if err != nil {
			return "", err
		}
		if strAlias != "" {
			return strAlias + "." + n.V, nil
		}
		return n.V, nil

	}
	if fx, ok := node.(*sqlparser.Where); ok {
		return w.walkOnWhere(fx, tblMap)

	}
	if fx, ok := node.(*sqlparser.ComparisonExpr); ok {
		strLeft, err := w.walkSQLNode(fx.Left, tblMap)
		if err != nil {
			return "", err
		}
		strRight, err := w.walkSQLNode(fx.Right, tblMap)
		if err != nil {
			return "", err
		}
		return strLeft + " " + fx.Operator + " " + strRight, nil
	}
	if fx, ok := node.(*sqlparser.AliasedTableExpr); ok {

		return w.walkSQLNode(fx.Expr, tblMap)
	}
	if fx, ok := node.(*sqlparser.JoinTableExpr); ok {
		return w.walkOnJoinTable(fx, tblMap)
	}
	if fx, ok := node.(sqlparser.JoinCondition); ok {
		retStr, err := w.walkSQLNode(fx.On, tblMap)
		if err != nil {
			return "", err
		}
		retStrUsing, err := w.walkSQLNode(fx.Using, tblMap)
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
			s, err := w.walkSQLNode(col, tblMap)
			if err != nil {
				return "", err
			}
			ret = append(ret, s)
		}
		return strings.Join(ret, ", "), nil
	}
	if fx, ok := node.(sqlparser.GroupBy); ok {
		return w.walkOnGroupBy(fx, tblMap)
	}
	if fx, ok := node.(*sqlparser.Select); ok {
		return w.walkOnSelect(fx, tblMap)
	}
	if fx, ok := node.(*sqlparser.OrderBy); ok {
		return w.walkOnOrderBy(fx, tblMap)
	}
	if fx, ok := node.(sqlparser.OrderBy); ok {
		return w.walkOnOrderBy(&fx, tblMap)
	}
	if fx, ok := node.(*sqlparser.Subquery); ok {
		return w.walkOnSubquery(*fx, tblMap)

	}
	if fx, ok := node.(sqlparser.ColIdent); ok {
		n, err := w.Resolver(Node{Nt: Field, V: fx.String()}, tblMap)
		if err != nil {
			return "", err
		}
		return n.V, nil

	}
	if fx, ok := node.(*sqlparser.Limit); ok {
		if fx.Offset == nil && fx.Rowcount == nil {
			return "", fmt.Errorf("syntax error")
		}
		if fx.Offset == nil && fx.Rowcount != nil {
			rc, err := w.walkSQLNode(fx.Rowcount, tblMap)
			if err != nil {
				return "", err
			}
			n, err := w.Resolver(Node{Nt: OffsetAndLimit, Limit: rc}, tblMap)
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
			rc, err := w.walkSQLNode(fx.Offset, tblMap)
			if err != nil {
				return "", err
			}
			n, err := w.Resolver(Node{Nt: OffsetAndLimit, Offset: rc}, tblMap)
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
			ofs, err := w.walkSQLNode(fx.Offset, tblMap)
			if err != nil {
				return "", err
			}
			rc, err := w.walkSQLNode(fx.Rowcount, tblMap)
			if err != nil {
				return "", err
			}
			n, err := w.Resolver(Node{Nt: OffsetAndLimit, Offset: ofs, Limit: rc}, tblMap)
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
		strL, err := w.walkSQLNode(fx.Left, tblMap)
		if err != nil {
			return "", err
		}
		strR, err := w.walkSQLNode(fx.Right, tblMap)
		if err != nil {
			return "", err
		}
		return strL + " AND " + strR, nil
	}
	if fx, ok := node.(*sqlparser.OrExpr); ok {
		strL, err := w.walkSQLNode(fx.Left, tblMap)
		if err != nil {
			return "", err
		}
		strR, err := w.walkSQLNode(fx.Right, tblMap)
		if err != nil {
			return "", err
		}
		return strL + " OR " + strR, nil
	}
	if fx, ok := node.(*sqlparser.NotExpr); ok {
		strL, err := w.walkSQLNode(fx.Expr, tblMap)
		if err != nil {
			return "", err
		}
		return "NOT " + strL, nil

	}
	if fx, ok := node.(sqlparser.TableNames); ok {
		ret := []string{}
		for _, tbl := range fx {
			strTbl, err := w.walkSQLNode(tbl, tblMap)
			if err != nil {
				return "", err
			}
			ret = append(ret, strTbl)
		}
		return strings.Join(ret, ", "), nil
	}
	if fx, ok := node.(*sqlparser.CaseExpr); ok {
		return w.walkOnCaseExpr(fx, tblMap)
	}
	if fx, ok := node.(sqlparser.BoolVal); ok {
		str := "true"
		if !fx {
			str = "false"
		}
		n, err := w.Resolver(Node{Nt: Value, V: str}, tblMap)
		if err != nil {
			return "", err
		}
		return n.V, nil
	}

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLNode", reflect.TypeOf(node)))

}
func (w *Walker) walkOnCaseExpr(expr *sqlparser.CaseExpr, tblMap *TableMap) (string, error) {
	ret := []string{}
	for _, when := range expr.Whens {
		whenStr, err := w.walkSQLNode(when.Cond, tblMap)
		if err != nil {
			return "", err
		}
		thenStr, err := w.walkSQLNode(when.Val, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHEN "+whenStr+" THEN "+thenStr)
	}
	if expr.Else != nil {
		elseStr, err := w.walkSQLNode(expr.Else, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "ELSE "+elseStr)
	}
	return "CASE " + strings.Join(ret, " ") + " END", nil
}
func (w *Walker) walkOnSubquery(stmt sqlparser.Subquery, tblMap *TableMap) (string, error) {
	subquery, err := w.walkOnStatement(stmt.Select, tblMap)
	if err != nil {
		return "", err
	}
	return "(" + subquery + ")", nil
}
func (w *Walker) walkOnGroupBy(stmt sqlparser.GroupBy, tblMap *TableMap) (string, error) {
	ret := []string{}
	for _, expr := range stmt {
		s, err := w.walkSQLNode(expr, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, s)
	}
	return strings.Join(ret, ", "), nil
}
func (w *Walker) walkOnWhere(stmt *sqlparser.Where, tblMap *TableMap) (string, error) {

	if stmt.Expr != nil {
		where, err := w.walkSQLNode(stmt.Expr, tblMap)
		if err != nil {
			return "", err
		}
		return where, nil
	}
	return "", fmt.Errorf("sybnax error")
}
func (w *Walker) walkOnTable(expr sqlparser.TableExprs, tblMap *TableMap) (string, error) {
	ret := []string{}
	for _, expr := range expr {
		if tbl, ok := expr.(*sqlparser.AliasedTableExpr); ok {

			strTableName, err := w.walkSQLNode(tbl.Expr, tblMap)
			if err != nil {
				return "", err
			}

			if !tbl.As.IsEmpty() {
				n, err := w.Resolver(Node{Nt: Alias, V: tbl.As.String()}, tblMap)
				if err != nil {
					return "", err
				}
				strTableName = strTableName + " AS " + n.V
			}

			ret = append(ret, strTableName)
			continue
		}
		if tbl, ok := expr.(*sqlparser.JoinTableExpr); ok {
			strJoin, err := w.walkOnJoinTable(tbl, tblMap)
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
func (w *Walker) walkOnFuncExpr(expr *sqlparser.FuncExpr, tblMap *TableMap) (string, error) {
	funcName := expr.Name.String()
	n, err := w.Resolver(Node{Nt: Function, V: funcName}, tblMap)
	if err != nil {
		return "", err
	}
	params := []string{}
	for _, p := range expr.Exprs {
		s, err := w.walkSQLNode(p, tblMap)
		if err != nil {
			return "", err
		}
		params = append(params, s)
	}
	return n.V + "(" + strings.Join(params, ", ") + ")", nil
}
func (w *Walker) walkSQLParen(expr *sqlparser.ParenExpr, tblMap *TableMap) (string, error) {

	cExpr := expr.Expr
	if fx, ok := cExpr.(*sqlparser.BinaryExpr); ok {
		strExpr, err := w.walkOnBinaryExpr(fx, tblMap)
		if err != nil {
			return "", err
		}
		return "(" + strExpr + ")", nil

	}

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLParen", reflect.TypeOf(cExpr)))
}
func (w *Walker) walkOnBinaryExpr(expr *sqlparser.BinaryExpr, tblMap *TableMap) (string, error) {
	op, err := w.Resolver(Node{Nt: Unanry, V: expr.Operator}, tblMap)
	if err != nil {
		return "", err
	}
	left, err := w.walkSQLNode(expr.Left, tblMap)
	if err != nil {
		return "", err
	}
	right, err := w.walkSQLNode(expr.Right, tblMap)
	if err != nil {
		return "", err
	}
	return left + " " + op.V + " " + right, nil
}
func (w *Walker) walkOnSelectOnly(stmt *sqlparser.Select, tblMap *TableMap) (string, error) {
	selector := []string{}
	for _, sel := range stmt.SelectExprs {

		s, err := w.walkSQLNode(sel, tblMap)
		if err != nil {
			return "", err
		}
		selector = append(selector, s)
	}
	return "SELECT " + strings.Join(selector, ", "), nil
}
func (w *Walker) walkOnSelect(stmt *sqlparser.Select, tblMap *TableMap) (string, error) {
	ret := []string{}
	selector := []string{}
	for _, sel := range stmt.SelectExprs {

		s, err := w.walkSQLNode(sel, tblMap)
		if err != nil {
			return "", err
		}
		selector = append(selector, s)
	}
	ret = append(ret, strings.Join(selector, ", "))

	if stmt.From != nil {
		from, err := w.walkSQLNode(stmt.From, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "FROM "+from)
	}
	if stmt.GroupBy != nil {
		groupBy, err := w.walkSQLNode(stmt.GroupBy, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "GROUP BY "+groupBy)

	}
	if stmt.Having != nil {
		groupBy, err := w.walkSQLNode(stmt.Having, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "HAVING "+groupBy)
	}
	if stmt.Where != nil {
		where, err := w.walkSQLNode(stmt.Where, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHERE "+where)
	}

	if stmt.OrderBy != nil {
		orderBy, err := w.walkSQLNode(stmt.OrderBy, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "ORDER BY "+orderBy)
	}

	if stmt.Limit != nil {
		limit, err := w.walkSQLNode(stmt.Limit, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, limit)
	}

	return "SELECT " + strings.Join(ret, " "), nil

}
func (w *Walker) walkOnInsert(stmt *sqlparser.Insert, tblMap *TableMap) (string, error) {

	tableName, err := w.walkSQLNode(stmt.Table, tblMap)
	if err != nil {
		return "", err
	}
	cols := []string{}

	for _, col := range stmt.Columns {
		colName, err := w.walkSQLNode(col, tblMap)
		if err != nil {
			return "", err
		}
		cols = append(cols, colName)
	}
	if fx, ok := stmt.Rows.(*sqlparser.Select); ok {
		sqlSelect, err := w.walkOnSelect(fx, tblMap)
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
				valStr, err := w.walkSQLNode(val, tblMap)
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
func (w *Walker) walkOnUnion(stmt *sqlparser.Union, tblMap *TableMap) (string, error) {
	left, err := w.walkSQLNode(stmt.Left, tblMap)
	if err != nil {
		return "", err
	}
	right, err := w.walkSQLNode(stmt.Right, tblMap)
	if err != nil {
		return "", err
	}
	return left + " " + stmt.Type + " " + right, nil
}
func (w *Walker) walkOnUpdate(stmt *sqlparser.Update, tblMap *TableMap) (string, error) {
	tableName, err := w.walkSQLNode(stmt.TableExprs, tblMap)
	if err != nil {
		return "", err
	}
	ret := []string{}
	for _, col := range stmt.Exprs {
		colName, err := w.walkSQLNode(col.Name, tblMap)
		if err != nil {
			return "", err
		}
		colValue, err := w.walkSQLNode(col.Expr, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, colName+" = "+colValue)
	}
	if stmt.Where != nil {
		where, err := w.walkSQLNode(stmt.Where, tblMap)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHERE "+where)
	}
	return "UPDATE " + tableName + " SET " + strings.Join(ret, ", "), nil

}
func (w *Walker) walkOnDelete(stmt *sqlparser.Delete, tblMap *TableMap) (string, error) {
	tableName, err := w.walkSQLNode(stmt.Targets, tblMap)
	if err != nil {
		return "", err
	}
	tableNameUsing, err := w.walkSQLNode(stmt.TableExprs, tblMap)
	if err != nil {
		return "", err
	}

	strWhere, err := w.walkSQLNode(stmt.Where, tblMap)

	if err != nil {
		return "", err
	}
	n, err := w.Resolver(Node{
		Nt: Using, Un: &UsingNodeOnDelete{
			TableName:   tableNameUsing,
			Where:       strWhere,
			TargetTable: tableName,
		},
	}, tblMap)
	if err != nil {
		return "", err
	}
	if n.Un != nil && n.Un.ReNewSQL != "" {
		return n.Un.ReNewSQL, nil
	}

	return "DELETE FROM " + tableName + " USING " + tableNameUsing + " WHERE " + strWhere, nil
}
func (w *Walker) walkOnJoinTable(expr *sqlparser.JoinTableExpr, tblMap *TableMap) (string, error) {
	strLeft, err := w.walkSQLNode(expr.LeftExpr, tblMap)
	if err != nil {
		return "", err
	}

	strRight, err := w.walkSQLNode(expr.RightExpr, tblMap)
	if err != nil {
		return "", err
	}
	strConditional, err := w.walkSQLNode(expr.Condition, tblMap)
	if err != nil {
		return "", err
	}
	ret := strLeft + " " + expr.Join + " " + strRight + " ON " + strConditional
	return ret, nil
}
func (w *Walker) walkOnOrderBy(expr *sqlparser.OrderBy, tblMap *TableMap) (string, error) {
	ret := []string{}
	for _, order := range *expr {
		str, err := w.walkSQLNode(order.Expr, tblMap)
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

type ColInfo struct {
	Name          string
	FieldType     reflect.StructField
	Tag           string
	IndexName     string
	IsPrimary     bool
	IsUnique      bool
	IsIndex       bool
	Len           int
	AllowNull     bool
	DefaultValue  string
	IndexOnStruct int
}

func (c *ColInfo) String() string {
	if c == nil {
		return "<nil>"
	}
	typ := reflect.TypeOf(c).Elem()
	ret := []string{}
	for i := 0; i < typ.NumField(); i++ {

		field := typ.Field(i)
		if field.Name == "FieldType" {
			continue
		}
		if field.Name == "Tag" {
			continue
		}
		val := reflect.ValueOf(c).Elem().Field(i).Interface()
		ret = append(ret, fmt.Sprintf("%s:%v ", field.Name, val))

	}
	return strings.Join(ret, "; ")
}

type RelationshipInfo struct {
	FromTable TableInfo
	ToTable   TableInfo
	FromCols  []ColInfo
	ToCols    []ColInfo
}
type TableInfo struct {
	TableName     string
	ColInfos      []ColInfo
	Relationship  []*RelationshipInfo
	MapCols       map[string]*ColInfo
	AutoValueCols map[string]*ColInfo
}

var replacerConstraint = map[string][]string{
	"pk":   {"primary_key", "primarykey", "primary", "primary_key_constraint"},
	"fk":   {"foreign_key", "foreignkey", "foreign", "foreign_key_constraint"},
	"uk":   {"unique", "unique_key", "uniquekey", "unique_key_constraint"},
	"idx":  {"index", "index_key", "indexkey", "index_constraint"},
	"text": {"vachar", "varchar", "varchar2"},
	"size": {"length", "len"},
	"df":   {"default", "default_value", "default_value_constraint"},
	"auto": {"auto_increment", "autoincrement", "serial_key", "serialkey", "serial_key_constraint"},
}
var hashCheckIsDbFieldAble = map[reflect.Type]bool{
	reflect.TypeOf(int(0)):      true,
	reflect.TypeOf(int8(0)):     true,
	reflect.TypeOf(int16(0)):    true,
	reflect.TypeOf(int32(0)):    true,
	reflect.TypeOf(int64(0)):    true,
	reflect.TypeOf(uint(0)):     true,
	reflect.TypeOf(uint8(0)):    true,
	reflect.TypeOf(uint16(0)):   true,
	reflect.TypeOf(uint32(0)):   true,
	reflect.TypeOf(uint64(0)):   true,
	reflect.TypeOf(float32(0)):  true,
	reflect.TypeOf(float64(0)):  true,
	reflect.TypeOf(string("")):  true,
	reflect.TypeOf(bool(false)): true,
	reflect.TypeOf(time.Time{}): true,

	reflect.TypeOf(decimal.Decimal{}): true,
	reflect.TypeOf(uuid.UUID{}):       true,
}
var mapDefaultValueFuncToPg = map[string]string{
	"now()":  "CURRENT_TIMESTAMP",
	"uuid()": "uuid_generate_v4()",
	"auto":   "SERIAL",
}

// serilize tag info ex: db:"name,age"

func GetColInfo(field reflect.StructField) *ColInfo {
	isNull := false
	isDbField := true
	if field.Type.Kind() == reflect.Slice {
		return nil

	}
	if field.Type.Kind() == reflect.Struct && !hashCheckIsDbFieldAble[field.Type] {
		tagOdStruct := field.Tag.Get("db")
		if tagOdStruct == "" {
			return nil
		}

	}

	if field.Type.Kind() == reflect.Ptr {
		// field.Type = field.Type.Elem()
		isNull = true
		if hashCheckIsDbFieldAble[field.Type.Elem()] {
			isDbField = true
		} else {
			isDbField = false
		}
	}

	tag := field.Tag.Get("db")
	if tag == "" && !isDbField {
		return nil
	}

	strTags := strings.ToLower(";" + tag + ";")

	for k, v := range replacerConstraint {
		for _, t := range v {
			strTags = strings.ReplaceAll(strTags, ";"+t+";", ";"+k+";")
			strTags = strings.ReplaceAll(strTags, ";"+t+":", ";"+k+":")
			strTags = strings.ReplaceAll(strTags, ";"+t+"(", ";"+k+"(")

		}

	}
	if strings.Contains(strTags, "fk:") && !isDbField {
		return nil
	}
	ret := ColInfo{
		Name:      field.Name,
		FieldType: field,
		Tag:       strTags,
		Len:       -1,
		AllowNull: isNull,
	}
	tgs := strings.Split(strTags, ";")
	for _, tg := range tgs {
		if tg == "" {
			continue
		}
		if tg == "pk" {
			ret.IsPrimary = true
		}
		if strings.HasPrefix(tg, "uk") {
			ret.IsUnique = true
			if strings.Contains(tg, ":") {
				ret.IndexName = tg[3:]
			}
		}
		if strings.HasPrefix(tg, "idx") {
			ret.IsIndex = true
			if strings.Contains(tg, ":") {
				ret.IndexName = tg[4:]
			} else {
				ret.IndexName = field.Name
			}

		}
		if strings.HasPrefix(tg, "fk") {
			ret.IsIndex = true
			if strings.Contains(tg, ":") {
				ret.IndexName = tg[3:]
			}
		}
		if strings.HasPrefix(tg, "text") {
			if strings.Contains(tg, "(") && strings.Contains(tg, ")") {
				ret.FieldType.Type = reflect.TypeOf(string(""))
				strLen := strings.Split(tg, "(")[1]
				strLen = strings.Split(strLen, ")")[0]
				lenV, err := strconv.Atoi(strLen)
				if err != nil {
					panic(fmt.Errorf("invalid text length %s in tag %s", strLen, field.Tag.Get("db")))
				}

				ret.Len = lenV
			}
		}
		if strings.HasPrefix(tg, "size") {
			if strings.Contains(tg, ":") {
				strLen := strings.Split(tg, ":")[1]
				lenV, err := strconv.Atoi(strLen)
				if err != nil {
					panic(fmt.Errorf("invalid size length %s in tag %s", strLen, field.Tag.Get("db")))
				}

				ret.Len = lenV
			} else {
				panic(fmt.Errorf("invalid length %s", field.Tag.Get("db")))
			}
		}
		if strings.HasPrefix(tg, "df:") {
			if strings.Contains(tg, ":") {
				dfValue := strings.Split(tg, ":")[1]
				dfValue = strings.Split(dfValue, ";")[0]
				ret.DefaultValue = dfValue
			} else {
				panic(fmt.Errorf("invalid default value %s", field.Tag.Get("db")))

			}

		}

	}
	return &ret
}

var cachedTableInfo sync.Map

func getTableInfoByType(typ reflect.Type) (*TableInfo, error) {

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid type %s", typ.Name())
	}
	table := TableInfo{
		TableName:    typ.Name(),
		ColInfos:     []ColInfo{},
		Relationship: []*RelationshipInfo{},
	}
	remainFields := []reflect.StructField{}
	colsProc := map[string]int{} // map colum name to index in colInfos
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		colInfo := GetColInfo(field)

		if colInfo == nil {
			remainFields = append(remainFields, field)
			continue
		}
		colInfo.IndexOnStruct = i
		colsProc[colInfo.Name] = i
		table.ColInfos = append(table.ColInfos, *colInfo)
	}
	/* check in remainFileds
	//sometime an entity may be inherite from another entity, so we need to check all fields in all parent entities
	// example
	// type A struct {
	// 	ID int `db:"pk"`
	// 	Code string `db:"size(100)"`
	// }
	// type B struct {
	// 	A
	//  ID string `db:"pk"` // this will be used instead of A.ID
	// 	Name string `db:"size(100)"`
	// }
	// new struct of table is looks like:
	// type newB struct {
	// 	ID string `db:"pk"` // use B.ID instead of A.ID
	/** 	Code string `db:"size(100)"` //use A.Code because Code was not found in B struct, so we use A.Code instead of B.Code
	/** 	Name string `db:"size(100)"`
	/** }
	/**
	*/
	for _, field := range remainFields {
		tag := field.Tag.Get("db")
		if tag == "" {
			if field.Type.Kind() == reflect.Ptr {
				field.Type = field.Type.Elem()
			}
			if field.Type.Kind() == reflect.Struct {
				extraCols := []ColInfo{}
				for i := 0; i < field.Type.NumField(); i++ {
					field2 := field.Type.Field(i)
					colInfo := GetColInfo(field2)
					if colInfo == nil {
						continue
					}
					extraCols = append(extraCols, *colInfo)
				}
				for _, col := range extraCols {
					if _, ok := colsProc[col.Name]; !ok {
						//skip extra col
						continue
					} else {
						table.ColInfos = append(table.ColInfos, col)
					}
				}

			}
		}
	}
	// check relationship in remainFields
	for _, field := range remainFields {
		tag := field.Tag.Get("db")
		strTags := strings.ToLower(";" + tag + ";")

		for k, v := range replacerConstraint {
			for _, t := range v {
				strTags = strings.ReplaceAll(strTags, ";"+t+";", ";"+k+";")
				strTags = strings.ReplaceAll(strTags, ";"+t+":", ";"+k+":")
				strTags = strings.ReplaceAll(strTags, ";"+t+"(", ";"+k+"(")

			}

		}
		if strings.Contains(strTags, "fk:") {
			strFk := strings.Split(strTags, ":")[1]
			strFk = strings.Split(strFk, ";")[0]
			fieldType := field.Type

			if fieldType.Kind() == reflect.Slice {
				fieldType = fieldType.Elem()
			}
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
			}
			reltblInfo, err := GetTableInfoByType(fieldType)
			if err != nil {
				return nil, err
			}
			fromCols := []ColInfo{}
			for _, col := range table.ColInfos {
				if col.IsPrimary {
					fromCols = append(fromCols, col)
				}
			}
			toCols := []ColInfo{}
			strFkCompare := strings.ToLower("," + strFk + ",")
			for _, col := range reltblInfo.ColInfos {
				colCompare := strings.ToLower("," + col.Name + ",")
				if strings.Contains(strFkCompare, colCompare) {
					toCols = append(toCols, col)
				}
			}

			rel := RelationshipInfo{
				FromTable: table,
				ToTable:   *reltblInfo,
				FromCols:  fromCols,
				ToCols:    toCols,
			}
			table.Relationship = append(table.Relationship, &rel)

		} else {

		}

	}
	table.MapCols = make(map[string]*ColInfo)
	table.AutoValueCols = make(map[string]*ColInfo)
	for i, col := range table.ColInfos {
		table.MapCols[col.Name] = &table.ColInfos[i]
		if col.DefaultValue != "" {
			table.AutoValueCols[col.Name] = &table.ColInfos[i]
		}
	}

	return &table, nil
}
func GetTableInfoByType(typ reflect.Type) (*TableInfo, error) {
	if v, ok := cachedTableInfo.Load(typ); ok {
		return v.(*TableInfo), nil
	}
	table, err := getTableInfoByType(typ)
	if err != nil {
		return nil, err
	}
	cachedTableInfo.Store(typ, table)
	return table, nil
}
func GetTableInfo(obj interface{}) (*TableInfo, error) {

	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Slice {
		typ = reflect.SliceOf(typ)

	}
	if typ.Kind() != reflect.Array {
		typ = typ.Elem()

	}
	return GetTableInfoByType(typ)
}

func (c *TableInfo) GetSql(dbDriver string) []string {
	switch dbDriver {
	// case "mysql":
	// 	return getSqlOfTableInfoForMysql(&c)
	case "postgres":
		return getSqlOfTableInfoForPostgres(*c)
	// case "sqlite3":
	// 	return getSqlOfTableInfoForSqlite3(&c)
	default:
		panic(fmt.Errorf("not support db driver %s", dbDriver))
	}
}

var mapGoTypeToPosgresType = map[reflect.Type]string{
	reflect.TypeOf(int(0)):            "integer",
	reflect.TypeOf(int8(0)):           "smallint",
	reflect.TypeOf(int16(0)):          "smallint",
	reflect.TypeOf(int32(0)):          "integer",
	reflect.TypeOf(int64(0)):          "bigint",
	reflect.TypeOf(uint(0)):           "integer",
	reflect.TypeOf(uint8(0)):          "smallint",
	reflect.TypeOf(uint16(0)):         "integer",
	reflect.TypeOf(uint32(0)):         "bigint",
	reflect.TypeOf(uint64(0)):         "bigint",
	reflect.TypeOf(float32(0)):        "real",
	reflect.TypeOf(float64(0)):        "double precision",
	reflect.TypeOf(string("")):        "citext",
	reflect.TypeOf(bool(false)):       "boolean",
	reflect.TypeOf(time.Time{}):       "timestamp",
	reflect.TypeOf(decimal.Decimal{}): "numeric",
	reflect.TypeOf(uuid.UUID{}):       "uuid",
}

func getSqlOfTableInfoForPostgres(table TableInfo) []string {
	ret := []string{}
	// create table
	sql := "CREATE TABLE IF NOT EXISTS \"" + table.TableName + "\" ("
	colsScript := []string{}
	scripAlterAddCols := []string{}

	//scriptSeqs := []string{}
	for _, col := range table.ColInfos {
		ft := col.FieldType.Type
		if ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}

		strCol := ""
		strAddCol := ""
		//check mapGoTypeToPosgresType
		if dbType, ok := mapGoTypeToPosgresType[ft]; ok {
			if col.IsPrimary {
				if col.DefaultValue == "auto" && col.FieldType.Type.Kind() == reflect.Int {
					strCol = fmt.Sprintf("\"%s\" SERIAL PRIMARY KEY", col.Name)
					colsScript = append(colsScript, strCol)
				} else {
					strCol = fmt.Sprintf("\"%s\" %s PRIMARY KEY", col.Name, dbType)
					colsScript = append(colsScript, strCol)
				}

			} else {

				if col.AllowNull {
					// strCol = fmt.Sprintf("\"%s\" %s NOT NULL", col.Name, dbType)
					//ALTER TABLE "Employee" ADD COLUMN IF NOT EXISTS email TEXT;
					strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table.TableName, col.Name, dbType)
					scripAlterAddCols = append(scripAlterAddCols, strAddCol)
				} else {
					// strCol = fmt.Sprintf("\"%s\" %s", col.Name, dbType)
					//ALTER TABLE "Employee" ADD COLUMN IF NOT EXISTS email TEXT;
					// df := ""
					// if _, ok := maoGoTyoToPostgresDefaultValue[ft]; ok {
					// 	df = maoGoTyoToPostgresDefaultValue[ft]
					// }
					if col.DefaultValue == "" {
						strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL", table.TableName, col.Name, dbType)
						scripAlterAddCols = append(scripAlterAddCols, strAddCol)
					} else {
						if dff, ok := mapDefaultValueFuncToPg[col.DefaultValue]; ok {
							if dff == "SERIAL" {
								defaultSeq := "\"" + table.TableName + "_" + col.Name + "_seq\""
								scriptSeq := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s", defaultSeq)
								strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT nextval('%s')", table.TableName, col.Name, dbType, defaultSeq)
								scripAlterAddCols = append(scripAlterAddCols, scriptSeq)
								scripAlterAddCols = append(scripAlterAddCols, strAddCol)
							} else {
								strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT %s", table.TableName, col.Name, dbType, dff)
								scripAlterAddCols = append(scripAlterAddCols, strAddCol)
							}
						} else {
							strAddCol = fmt.Sprintf("ALTER TABLE IF EXISTS \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s NOT NULL DEFAULT %s", table.TableName, col.Name, dbType, col.DefaultValue)
							scripAlterAddCols = append(scripAlterAddCols, strAddCol)
						}
					}

				}

			}

		} else {
			panic(fmt.Errorf("not support map %s in GO type to in postgres ", ft.Name()))
		}

	}

	sql += strings.Join(colsScript, ",") + ")"
	ret = append(ret, sql)
	// scan col if col has len>-1
	scriptAddLenConstraint := []string{}
	indexInfo := map[string][]string{}
	unIndexInfo := map[string][]string{}
	for _, col := range table.ColInfos {
		if col.Len > 0 {
			/**
						ALTER TABLE IF EXISTS public."Users"
			    ADD CONSTRAINT "Max_Name_50" CHECK (length("Name"::text) <= 50);
			*/
			sqlAddCheckLen := "ALTER TABLE IF EXISTS \"" + table.TableName + "\" ADD CONSTRAINT \"" + table.TableName + "_" + col.Name + "_" + strconv.Itoa(col.Len) + "\" CHECK (length(\"" + col.Name + "\"::text) <= " + strconv.Itoa(col.Len) + ")"
			scriptAddLenConstraint = append(scriptAddLenConstraint, sqlAddCheckLen)
		}
		if col.IsIndex {
			if col.IndexName == "" {
				col.IndexName = table.TableName + "_" + col.Name
			}
			if _, ok := indexInfo[col.IndexName]; !ok {
				indexInfo[col.IndexName] = []string{col.Name}
			} else {
				indexInfo[col.IndexName] = append(indexInfo[col.IndexName], col.Name)
			}

		}
		if col.IsUnique && !col.IsPrimary {
			if col.IndexName == "" {
				col.IndexName = table.TableName + "_" + col.Name
			}
			if _, ok := unIndexInfo[col.IndexName]; !ok {
				unIndexInfo[col.IndexName] = []string{}
			}
			unIndexInfo[col.IndexName] = append(unIndexInfo[col.IndexName], col.Name)
		}

	}
	scriptIndex := []string{}
	for index_name, cols := range indexInfo {
		strCosl := strings.Join(cols, "\",\"")
		sqlCreateIndex := "CREATE INDEX IF NOT EXISTS \"" + index_name + "\" ON \"" + table.TableName + "\" (\"" + strCosl + "\")"
		scriptIndex = append(scriptIndex, sqlCreateIndex)
	}
	scriptUnIndex := []string{}
	for index_name, cols := range unIndexInfo {
		strCosl := strings.Join(cols, "\",\"")
		sqlCreateIndex := "CREATE UNIQUE INDEX IF NOT EXISTS \"" + index_name + "\" ON \"" + table.TableName + "\" (\"" + strCosl + "\")"
		scriptUnIndex = append(scriptUnIndex, sqlCreateIndex)
	}
	ret = append(ret, scripAlterAddCols...)
	ret = append(ret, scriptAddLenConstraint...)
	ret = append(ret, scriptIndex...)
	ret = append(ret, scriptUnIndex...)
	scriptCreateRel := []string{}
	for _, rel := range table.Relationship {
		if ret != nil {
			ret = append(ret, rel.ToTable.GetSql("postgres")...)
			/*
						ALTER TABLE <tên_bảng_có_khóa_ngoại>
				ADD CONSTRAINT <tên_constraint>
				FOREIGN KEY (<cột_khóa_ngoại>)
				REFERENCES <tên_bảng_được_tham_chiếu> (<cột_khóa_chính>);
						**/
			keyCOls := ""

			for _, col := range rel.FromCols {
				keyCOls += "\"" + col.Name + "\","
			}
			keyCOls = strings.TrimSuffix(keyCOls, ",")
			fkCol := ""
			for _, col := range rel.ToCols {
				fkCol += "\"" + col.Name + "\","
			}
			fkCol = strings.TrimSuffix(fkCol, ",")
			relName := table.TableName + "_" + rel.ToTable.TableName + "_fk"
			strRel := "ALTER TABLE \"" + rel.ToTable.TableName + "\" ADD CONSTRAINT \"" + relName + "\" FOREIGN KEY (" + fkCol + ") REFERENCES \"" + rel.FromTable.TableName + "\" (" + keyCOls + ") MATCH SIMPLE ON UPDATE CASCADE ON DELETE NO ACTION"
			scriptCreateRel = append(scriptCreateRel, strRel)

		}

	}
	ret = append(ret, scriptCreateRel...)

	return ret
}

//==========================================================
//==========================================================

type DbCfg struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	UseSSL   bool
}

func (cfg *DbCfg) GetDns(dbName string) string {
	if cfg.Driver == "postgres" {
		if dbName == "" {
			if cfg.UseSSL {

				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			} else {
				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			}
		} else {
			if cfg.UseSSL {

				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			} else {
				return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
			}
		}
	} else if cfg.Driver == "mysql" {
		if dbName == "" {
			return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
		} else {
			return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Password, cfg.Host, cfg.Port, dbName)
		}
	} else {
		panic(fmt.Errorf("not support db driver %s", cfg.Driver))
	}
}

type DbContext struct {
	*sql.DB
	dbDriver string
	cfg      *DbCfg

	dns string
}
type TenentDbContext struct {
	*DbContext
	dbName string
}

func NewDbContext(cfg DbCfg) *DbContext {
	ret := DbContext{
		dbDriver: cfg.Driver,
		cfg:      &cfg,
		dns:      cfg.GetDns(""),
	}
	return &ret
}
func (ctx *DbContext) Open() error {
	db, err := sql.Open(ctx.dbDriver, ctx.dns)
	if err != nil {
		return err
	}
	ctx.DB = db
	return nil
}

var cachedTenentDb sync.Map

func (ctx *DbContext) CreateCtx(dbName string) (*TenentDbContext, error) {
	// check cache

	if ctx.cfg == nil {
		return nil, fmt.Errorf("db config is nil")
	}
	ctx.Open()
	defer ctx.Close()
	if ctx.dbDriver == "postgres" {
		if _, ok := cachedTenentDb.Load(dbName); !ok {

			err := createPosgresDbIfNotExist(ctx, dbName)
			if err != nil {
				return nil, err
			}
			//set to cach
			cachedTenentDb.Store(dbName, true)
		}

	}
	ret := TenentDbContext{
		DbContext: &DbContext{
			dbDriver: ctx.cfg.Driver,
			cfg:      ctx.cfg,
			dns:      ctx.cfg.GetDns(dbName),
		},
		dbName: dbName,
	}
	return &ret, nil

}
func createPosgresDbIfNotExist(ctx *DbContext, dbName string) error {
	var exists bool
	err := ctx.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		sqlCreate := fmt.Sprintf("CREATE DATABASE %q", dbName) // Sử dụng %q để quote tên database an toàn
		_, err = ctx.Exec(sqlCreate)
		if err != nil {
			return err
		}

	}
	dbTenant, err := sql.Open(ctx.dbDriver, ctx.cfg.GetDns(dbName))
	if err != nil {
		return err
	}
	defer dbTenant.Close()
	// enable extension citext if not exist
	_, err = dbTenant.Exec("CREATE EXTENSION IF NOT EXISTS citext")
	if err != nil {
		return err
	}
	return nil
}
func postgresResolver(node Node, tblMap *TableMap) (Node, error) {
	if node.Nt == TableName || node.Nt == Field {
		node.V = "\"" + node.V + "\""
		return node, nil
	}
	if node.Nt == Params {
		node.V = "$" + node.V[1:]
	}
	return node, nil

}
func postgresResolverInsertSQL(sql string, tbl TableInfo) (*string, error) {
	returnCols := []string{}
	for _, col := range tbl.AutoValueCols {

		returnCols = append(returnCols, col.Name)
	}
	if len(returnCols) > 0 {
		sql += " returning " + "\"" + strings.Join(returnCols, "\",\"") + "\""
	}

	return &sql, nil
}

var postgresResolverInsertSQLfn = postgresResolverInsertSQL
var postgresWalker = &Walker{
	Resolver:          postgresResolver,
	ResolverInsertSQL: &postgresResolverInsertSQLfn,
}
var cacheSqlInsert sync.Map

type sqlDDL struct {
	Sql      string
	TablInfo *TableInfo
}

func (ctx *TenentDbContext) SqlInsert1(typ reflect.Type) (*sqlDDL, error) {

	var walker *Walker
	retData := &sqlDDL{}
	if ctx.dbDriver == "postgres" {
		walker = postgresWalker
	} else {
		panic(fmt.Errorf("not support db driver %s", ctx.dbDriver))
	}

	if v, ok := cacheSqlInsert.Load(typ); ok {
		return v.(*sqlDDL), nil
	}
	tableInfo, err := GetTableInfoByType(typ)
	retData.TablInfo = tableInfo

	if err != nil {
		return nil, err
	}
	ret := " insert into " + tableInfo.TableName + " ("
	strFieldInsert := ""
	strValue := ""
	for i, col := range tableInfo.ColInfos {
		if col.DefaultValue == "auto" {
			continue
		}
		if i > 0 {
			strFieldInsert += ","
			strValue += ","
		}
		strFieldInsert += col.Name
		strValue += "?"

	}
	ret += strFieldInsert + ") values (" + strValue + ")"
	retSQL, err := walker.Parse(ret, nil)
	if err != nil {
		return nil, err
	}
	retData.Sql = retSQL
	//set to cache
	cacheSqlInsert.Store(typ, retData)

	return retData, nil
}

var cachMigrate sync.Map

func (ctx *TenentDbContext) Migrate(dbName string, entity interface{}) (reflect.Type, error) {

	typ := reflect.TypeOf(entity)
	if typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	tableInfo, err := GetTableInfoByType(typ)
	if err != nil {
		return nil, err
	}
	key := dbName + "-" + typ.String()
	if _, ok := cachMigrate.Load(key); ok {
		return typ, nil
	}
	sqlCreate := tableInfo.GetSql(ctx.dbDriver)
	if ctx.DB == nil {
		return nil, fmt.Errorf("please open TenentDbContext first")
	}
	for _, sql := range sqlCreate {
		_, err = ctx.Exec(sql)
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				continue
			}
			return nil, err
		}
	}
	cachMigrate.Store(key, true)
	return typ, nil
}

type sqlWithParams struct {
	Sql    string
	Params []interface{}
}

var mapDefaulValueOfGoType = map[reflect.Type]interface{}{
	reflect.TypeOf(int(0)):      0,
	reflect.TypeOf(int8(0)):     0,
	reflect.TypeOf(int16(0)):    0,
	reflect.TypeOf(int32(0)):    0,
	reflect.TypeOf(int64(0)):    0,
	reflect.TypeOf(uint(0)):     0,
	reflect.TypeOf(uint8(0)):    0,
	reflect.TypeOf(uint16(0)):   0,
	reflect.TypeOf(uint32(0)):   0,
	reflect.TypeOf(uint64(0)):   0,
	reflect.TypeOf(float32(0)):  0,
	reflect.TypeOf(float64(0)):  0,
	reflect.TypeOf(bool(false)): false,
	reflect.TypeOf(string("")):  "",
	reflect.TypeOf(time.Time{}): time.Time{},
	reflect.TypeOf(uuid.UUID{}): uuid.UUID{},
}

func (ctx *TenentDbContext) toArray(entity interface{}, tableInfo TableInfo) (*sqlWithParams, error) {
	var ret = sqlWithParams{
		Params: []interface{}{},
	}
	typ := reflect.TypeOf(entity)
	val := reflect.ValueOf(entity)

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		val = val.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("not support type %s", typ.String())
	}
	ret.Sql = "insert into "
	fields := []string{}
	valParams := []string{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if col, ok := tableInfo.MapCols[field.Name]; ok {
			fieldVal := val.Field(i)
			if fieldVal.IsZero() {
				if !col.AllowNull && col.DefaultValue != "" {
					continue
				}
				if !col.AllowNull && col.DefaultValue == "" {
					if val, ok := mapDefaulValueOfGoType[fieldVal.Type()]; ok {
						ret.Params = append(ret.Params, val)
						fields = append(fields, col.Name)
						valParams = append(valParams, "?")
					}
				}

			} else {
				ret.Params = append(ret.Params, fieldVal.Interface())
				fields = append(fields, col.Name)
				valParams = append(valParams, "?")
			}

		}

	}
	ret.Sql += tableInfo.TableName + " (" + strings.Join(fields, ",") + ") values (" + strings.Join(valParams, ",") + ")"
	return &ret, nil
}
func (ctx *TenentDbContext) Insert(entity interface{}) error {
	var walker *Walker
	if ctx.cfg.Driver == "postgres" {
		walker = postgresWalker
	} else {
		panic(fmt.Errorf("not support db driver %s", ctx.cfg.Driver))
	}
	typ, err := ctx.Migrate(ctx.dbName, entity)
	if err != nil {
		return err
	}
	tblInfo, err := GetTableInfoByType(typ)
	if err != nil {
		return err
	}
	dataInsert, err := ctx.toArray(entity, *tblInfo)

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	if ctx.DB == nil {
		return fmt.Errorf("please open TenentDbContext first")
	}
	execSql, err := walker.Parse(dataInsert.Sql, nil)
	if walker.ResolverInsertSQL == nil {
		return fmt.Errorf("walker.ResolverInsertSQL is not set")
	}
	resolverInsertSQL := *walker.ResolverInsertSQL

	execSql2, err := resolverInsertSQL(execSql, *tblInfo)
	if err != nil {
		return err
	}
	resultArray := []interface{}{}
	mapResult := map[int]string{}
	indexOfParam := 0
	for _, col := range tblInfo.AutoValueCols {
		val := reflect.New(col.FieldType.Type).Interface()
		resultArray = append(resultArray, val)
		mapResult[indexOfParam] = col.Name
		indexOfParam++
	}

	rs := ctx.QueryRow(*execSql2, dataInsert.Params...)
	if rs.Err() != nil {
		return rs.Err()
	}
	rs.Scan(resultArray...)
	// re-assign to entity

	for indexOfParam, _ := range mapResult {

		fieldVal := reflect.ValueOf(entity).Elem().FieldByName(mapResult[indexOfParam])
		fieldTyp := reflect.TypeOf(fieldVal.Interface())
		if fieldTyp.Kind() == reflect.Ptr {
			fieldTyp = fieldTyp.Elem()
		}
		fmt.Println(fieldVal.Type())

		fmt.Println(reflect.TypeOf(resultArray[indexOfParam]))
		rType := reflect.TypeOf(resultArray[indexOfParam])
		if reflect.TypeOf(resultArray[indexOfParam]).Kind() == reflect.Ptr {
			rType := rType.Elem()
			if rType == fieldTyp {
				fieldVal.Set(reflect.ValueOf(resultArray[indexOfParam]).Elem())
			}
		} else {
			fieldVal.Set(reflect.ValueOf(resultArray[indexOfParam]))
		}

		indexOfParam++
	}
	if err != nil {
		return err
	}
	return nil
}
