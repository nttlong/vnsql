package compiler

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xwb1989/sqlparser"
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

type UsingNodeOnDelete struct {
	TableName   string
	Where       string
	TargetTable string
	ReNewSQL    string
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
type Compiler struct {
	OnParse func(compilerContext *Compiler, node Node) (Node, error)

	// Some RDBMS need special parse for insert sql
	OnParseInsertSQL func(compilerContext *Compiler, sql string, autoValueCols []string, returnColAfterInsert []string) (*string, error)
}

func (w *Compiler) Parse(sql string) (string, error) {
	if cached, ok := cacheSqlParse.Load(sql); ok {
		return cached.(SQLParseInfo).SQL, nil
	}
	sql, err := w.parse(sql)
	if err != nil {
		return "", err
	}
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
func (w *Compiler) parse(sql string) (string, error) {
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
			return w.walkOnSelectOnly(fx)
		}
		return "", fmt.Errorf("not support sql: %s", sql)

	}
	return w.walkOnStatement(stm)
}
func (w *Compiler) walkOnSelectOnly(stmt *sqlparser.Select) (string, error) {
	selector := []string{}
	for _, sel := range stmt.SelectExprs {

		s, err := w.walkSQLNode(sel)
		if err != nil {
			return "", err
		}
		selector = append(selector, s)
	}
	return "SELECT " + strings.Join(selector, ", "), nil
}
func (w *Compiler) walkSQLNode(node sqlparser.SQLNode) (string, error) {

	if fx, ok := node.(*sqlparser.StarExpr); ok {

		if !fx.TableName.IsEmpty() {
			n, err := w.OnParse(w, Node{Nt: TableName, V: fx.TableName.Name.String()})
			if err != nil {
				return "", err
			}
			return n.V + ".*", nil

		}
		return "*", nil
	}
	if fx, ok := node.(*sqlparser.AliasedExpr); ok {
		if fx.As.IsEmpty() {
			return w.walkSQLNode(fx.Expr)

		}

		nAlias, err := w.OnParse(w, Node{Nt: Alias, V: fx.As.String()})

		if err != nil {
			return "", err
		}
		n, err := w.walkSQLNode(fx.Expr)
		if err != nil {
			return "", err
		}
		return n + " AS " + nAlias.V, nil
	}
	if fx, ok := node.(*sqlparser.SQLVal); ok {
		strVal := string(fx.Val)
		if pName, ok := isParam(strVal); ok {
			n, err := w.OnParse(w, Node{Nt: Params, V: pName})
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			n, err := w.OnParse(w, Node{Nt: Value, V: string(fx.Val)})
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
			n, err := w.OnParse(
				w,
				Node{
					Nt: Params, V: fieldName[1:]},
			)
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			tblName := ""
			if !fx.Qualifier.IsEmpty() {
				tblName = fx.Qualifier.Name.String()
				n, err := w.OnParse(w, Node{Nt: Field, V: tblName + "." + fieldName})

				if err != nil {
					return "", err
				}
				return n.V, nil
			}
			n, err := w.OnParse(w, Node{Nt: Field, V: fieldName})

			if err != nil {
				return "", err
			}
			tableName, err := w.walkSQLNode(fx.Qualifier)
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

		return w.walkSQLParen(fx)

	}
	if fx, ok := node.(*sqlparser.BinaryExpr); ok {

		return w.walkOnBinaryExpr(fx)

	}
	if fx, ok := node.(*sqlparser.FuncExpr); ok {
		return w.walkOnFuncExpr(fx)
	}
	if fx, ok := node.(*sqlparser.JoinTableExpr); ok {
		return w.walkOnJoinTable(fx)
	}
	if fx, ok := node.(sqlparser.TableExprs); ok {

		return w.walkOnTable(fx)
	}
	if fx, ok := node.(sqlparser.TableName); ok {
		if fx.Name.IsEmpty() {
			return "", nil
		}
		strAlias := ""
		if !fx.Qualifier.IsEmpty() {
			strAliasGet, err := w.walkSQLNode(fx.Qualifier)
			if err != nil {
				return "", err
			}
			strAlias = strAliasGet

		}
		n, err := w.OnParse(w, Node{Nt: TableName, V: fx.Name.String()})
		if err != nil {
			return "", err
		}
		if strAlias != "" {
			return strAlias + "." + n.V, nil
		}
		return n.V, nil

	}
	if fx, ok := node.(*sqlparser.Where); ok {
		return w.walkOnWhere(fx)

	}
	if fx, ok := node.(*sqlparser.ComparisonExpr); ok {
		strLeft, err := w.walkSQLNode(fx.Left)
		if err != nil {
			return "", err
		}
		strRight, err := w.walkSQLNode(fx.Right)
		if err != nil {
			return "", err
		}
		return strLeft + " " + fx.Operator + " " + strRight, nil
	}
	if fx, ok := node.(*sqlparser.AliasedTableExpr); ok {

		return w.walkSQLNode(fx.Expr)
	}
	if fx, ok := node.(*sqlparser.JoinTableExpr); ok {
		return w.walkOnJoinTable(fx)
	}
	if fx, ok := node.(sqlparser.JoinCondition); ok {
		retStr, err := w.walkSQLNode(fx.On)
		if err != nil {
			return "", err
		}
		retStrUsing, err := w.walkSQLNode(fx.Using)
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
			s, err := w.walkSQLNode(col)
			if err != nil {
				return "", err
			}
			ret = append(ret, s)
		}
		return strings.Join(ret, ", "), nil
	}
	if fx, ok := node.(sqlparser.GroupBy); ok {
		return w.walkOnGroupBy(fx)
	}
	if fx, ok := node.(*sqlparser.Select); ok {
		return w.walkOnSelect(fx)
	}
	if fx, ok := node.(*sqlparser.OrderBy); ok {
		return w.walkOnOrderBy(fx)
	}
	if fx, ok := node.(sqlparser.OrderBy); ok {
		return w.walkOnOrderBy(&fx)
	}
	if fx, ok := node.(*sqlparser.Subquery); ok {
		return w.walkOnSubquery(*fx)

	}
	if fx, ok := node.(sqlparser.ColIdent); ok {
		n, err := w.OnParse(w, Node{Nt: Field, V: fx.String()})
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
			rc, err := w.walkSQLNode(fx.Rowcount)
			if err != nil {
				return "", err
			}
			n, err := w.OnParse(w, Node{Nt: OffsetAndLimit, Limit: rc})
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
			rc, err := w.walkSQLNode(fx.Offset)
			if err != nil {
				return "", err
			}
			n, err := w.OnParse(w, Node{Nt: OffsetAndLimit, Offset: rc})
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
			ofs, err := w.walkSQLNode(fx.Offset)
			if err != nil {
				return "", err
			}
			rc, err := w.walkSQLNode(fx.Rowcount)
			if err != nil {
				return "", err
			}
			n, err := w.OnParse(w, Node{Nt: OffsetAndLimit, Offset: ofs, Limit: rc})
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
		strL, err := w.walkSQLNode(fx.Left)
		if err != nil {
			return "", err
		}
		strR, err := w.walkSQLNode(fx.Right)
		if err != nil {
			return "", err
		}
		return strL + " AND " + strR, nil
	}
	if fx, ok := node.(*sqlparser.OrExpr); ok {
		strL, err := w.walkSQLNode(fx.Left)
		if err != nil {
			return "", err
		}
		strR, err := w.walkSQLNode(fx.Right)
		if err != nil {
			return "", err
		}
		return strL + " OR " + strR, nil
	}
	if fx, ok := node.(*sqlparser.NotExpr); ok {
		strL, err := w.walkSQLNode(fx.Expr)
		if err != nil {
			return "", err
		}
		return "NOT " + strL, nil

	}
	if fx, ok := node.(sqlparser.TableNames); ok {
		ret := []string{}
		for _, tbl := range fx {
			strTbl, err := w.walkSQLNode(tbl)
			if err != nil {
				return "", err
			}
			ret = append(ret, strTbl)
		}
		return strings.Join(ret, ", "), nil
	}
	if fx, ok := node.(*sqlparser.CaseExpr); ok {
		return w.walkOnCaseExpr(fx)
	}
	if fx, ok := node.(sqlparser.BoolVal); ok {
		str := "true"
		if !fx {
			str = "false"
		}
		n, err := w.OnParse(w, Node{Nt: Value, V: str})
		if err != nil {
			return "", err
		}
		return n.V, nil
	}

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLNode", reflect.TypeOf(node)))

}
func (w *Compiler) walkOnCaseExpr(expr *sqlparser.CaseExpr) (string, error) {
	ret := []string{}
	for _, when := range expr.Whens {
		whenStr, err := w.walkSQLNode(when.Cond)
		if err != nil {
			return "", err
		}
		thenStr, err := w.walkSQLNode(when.Val)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHEN "+whenStr+" THEN "+thenStr)
	}
	if expr.Else != nil {
		elseStr, err := w.walkSQLNode(expr.Else)
		if err != nil {
			return "", err
		}
		ret = append(ret, "ELSE "+elseStr)
	}
	return "CASE " + strings.Join(ret, " ") + " END", nil
}
func (w *Compiler) walkOnStatement(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Union:
		return w.walkOnUnion(stmt)
	case *sqlparser.Select:
		return w.walkOnSelect(stmt)
	case *sqlparser.Insert:
		return w.walkOnInsert(stmt)
	case *sqlparser.Update:
		return w.walkOnUpdate(stmt)
	case *sqlparser.Delete:
		return w.walkOnDelete(stmt)
	default:
		panic(fmt.Sprintf("unsupported statement type %T", stmt))
	}

}
func (w *Compiler) walkOnUnion(stmt *sqlparser.Union) (string, error) {
	left, err := w.walkSQLNode(stmt.Left)
	if err != nil {
		return "", err
	}
	right, err := w.walkSQLNode(stmt.Right)
	if err != nil {
		return "", err
	}
	return left + " " + stmt.Type + " " + right, nil
}

func (w *Compiler) walkOnSelect(stmt *sqlparser.Select) (string, error) {
	ret := []string{}
	selector := []string{}
	for _, sel := range stmt.SelectExprs {

		s, err := w.walkSQLNode(sel)
		if err != nil {
			return "", err
		}
		selector = append(selector, s)
	}
	ret = append(ret, strings.Join(selector, ", "))

	if stmt.From != nil {
		from, err := w.walkSQLNode(stmt.From)
		if err != nil {
			return "", err
		}
		ret = append(ret, "FROM "+from)
	}
	if stmt.GroupBy != nil {
		groupBy, err := w.walkSQLNode(stmt.GroupBy)
		if err != nil {
			return "", err
		}
		ret = append(ret, "GROUP BY "+groupBy)

	}
	if stmt.Having != nil {
		groupBy, err := w.walkSQLNode(stmt.Having)
		if err != nil {
			return "", err
		}
		ret = append(ret, "HAVING "+groupBy)
	}
	if stmt.Where != nil {
		where, err := w.walkSQLNode(stmt.Where)
		if err != nil {
			return "", err
		}
		ret = append(ret, "WHERE "+where)
	}

	if stmt.OrderBy != nil {
		orderBy, err := w.walkSQLNode(stmt.OrderBy)
		if err != nil {
			return "", err
		}
		ret = append(ret, "ORDER BY "+orderBy)
	}

	if stmt.Limit != nil {
		limit, err := w.walkSQLNode(stmt.Limit)
		if err != nil {
			return "", err
		}
		ret = append(ret, limit)
	}

	return "SELECT " + strings.Join(ret, " "), nil

}
func (w *Compiler) walkOnInsert(stmt *sqlparser.Insert) (string, error) {

	tableName, err := w.walkSQLNode(stmt.Table)
	if err != nil {
		return "", err
	}
	cols := []string{}

	for _, col := range stmt.Columns {
		colName, err := w.walkSQLNode(col)
		if err != nil {
			return "", err
		}
		cols = append(cols, colName)
	}
	if fx, ok := stmt.Rows.(*sqlparser.Select); ok {
		sqlSelect, err := w.walkOnSelect(fx)
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
				valStr, err := w.walkSQLNode(val)
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

func (w *Compiler) walkOnUpdate(stmt *sqlparser.Update) (string, error) {
	tableName, err := w.walkSQLNode(stmt.TableExprs)
	if err != nil {
		return "", err
	}
	ret := []string{}
	for _, col := range stmt.Exprs {
		colName, err := w.walkSQLNode(col.Name)
		if err != nil {
			return "", err
		}
		colValue, err := w.walkSQLNode(col.Expr)
		if err != nil {
			return "", err
		}
		ret = append(ret, colName+" = "+colValue)
	}
	where := ""
	if stmt.Where != nil {
		where, err = w.walkSQLNode(stmt.Where)
		if err != nil {
			return "", err
		}

	}
	return "UPDATE " + tableName + " SET " + strings.Join(ret, ", ") + " WHERE " + where, nil

}
func (w *Compiler) walkOnDelete(stmt *sqlparser.Delete) (string, error) {
	tableName, err := w.walkSQLNode(stmt.Targets)
	if err != nil {
		return "", err
	}
	tableNameUsing, err := w.walkSQLNode(stmt.TableExprs)
	if err != nil {
		return "", err
	}

	strWhere, err := w.walkSQLNode(stmt.Where)

	if err != nil {
		return "", err
	}
	n, err := w.OnParse(w, Node{
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
func (w *Compiler) walkOnJoinTable(expr *sqlparser.JoinTableExpr) (string, error) {
	strLeft, err := w.walkSQLNode(expr.LeftExpr)
	if err != nil {
		return "", err
	}

	strRight, err := w.walkSQLNode(expr.RightExpr)
	if err != nil {
		return "", err
	}
	strConditional, err := w.walkSQLNode(expr.Condition)
	if err != nil {
		return "", err
	}
	ret := strLeft + " " + expr.Join + " " + strRight + " ON " + strConditional
	return ret, nil
}
func (w *Compiler) walkOnOrderBy(expr *sqlparser.OrderBy) (string, error) {
	ret := []string{}
	for _, order := range *expr {
		str, err := w.walkSQLNode(order.Expr)
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

// func (w *Compiler) ParseDBDLL(sql string) (DBDDLCmds, error) {
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

func (w *Compiler) walkOnSubquery(stmt sqlparser.Subquery) (string, error) {
	subquery, err := w.walkOnStatement(stmt.Select)
	if err != nil {
		return "", err
	}
	return "(" + subquery + ")", nil
}
func (w *Compiler) walkOnGroupBy(stmt sqlparser.GroupBy) (string, error) {
	ret := []string{}
	for _, expr := range stmt {
		s, err := w.walkSQLNode(expr)
		if err != nil {
			return "", err
		}
		ret = append(ret, s)
	}
	return strings.Join(ret, ", "), nil
}
func (w *Compiler) walkOnWhere(stmt *sqlparser.Where) (string, error) {

	if stmt.Expr != nil {
		where, err := w.walkSQLNode(stmt.Expr)
		if err != nil {
			return "", err
		}
		return where, nil
	}
	return "", fmt.Errorf("sybnax error")
}
func (w *Compiler) walkOnTable(expr sqlparser.TableExprs) (string, error) {
	ret := []string{}
	for _, expr := range expr {
		if tbl, ok := expr.(*sqlparser.AliasedTableExpr); ok {

			strTableName, err := w.walkSQLNode(tbl.Expr)
			if err != nil {
				return "", err
			}

			if !tbl.As.IsEmpty() {
				n, err := w.OnParse(w, Node{Nt: Alias, V: tbl.As.String()})
				if err != nil {
					return "", err
				}
				strTableName = strTableName + " AS " + n.V
			}

			ret = append(ret, strTableName)
			continue
		}
		if tbl, ok := expr.(*sqlparser.JoinTableExpr); ok {
			strJoin, err := w.walkOnJoinTable(tbl)
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
func (w *Compiler) walkOnFuncExpr(expr *sqlparser.FuncExpr) (string, error) {
	funcName := expr.Name.String()
	n, err := w.OnParse(w, Node{Nt: Function, V: funcName})
	if err != nil {
		return "", err
	}
	params := []string{}
	for _, p := range expr.Exprs {
		s, err := w.walkSQLNode(p)
		if err != nil {
			return "", err
		}
		params = append(params, s)
	}
	return n.V + "(" + strings.Join(params, ", ") + ")", nil
}
func (w *Compiler) walkSQLParen(expr *sqlparser.ParenExpr) (string, error) {

	cExpr := expr.Expr
	if fx, ok := cExpr.(*sqlparser.BinaryExpr); ok {
		strExpr, err := w.walkOnBinaryExpr(fx)
		if err != nil {
			return "", err
		}
		return "(" + strExpr + ")", nil

	}

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLParen", reflect.TypeOf(cExpr)))
}
func (w *Compiler) walkOnBinaryExpr(expr *sqlparser.BinaryExpr) (string, error) {
	op, err := w.OnParse(w, Node{Nt: Unanry, V: expr.Operator})
	if err != nil {
		return "", err
	}
	left, err := w.walkSQLNode(expr.Left)
	if err != nil {
		return "", err
	}
	right, err := w.walkSQLNode(expr.Right)
	if err != nil {
		return "", err
	}
	return left + " " + op.V + " " + right, nil
}
