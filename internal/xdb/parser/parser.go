package parser

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
	_ "github.com/xwb1989/sqlparser"
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
type Walker struct {
	Resolver func(node Node, tblMap *TableMap) (Node, error)
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
func (w *Walker) Parse(sql string, tblMap *TableMap) (string, error) {
	stm, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
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
