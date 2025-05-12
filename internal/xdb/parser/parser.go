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
type Node struct {
	Nt NodeType
	V  string // Value of the node
	C  []Node // Children of the node
}
type SQLParseInfo struct {
	SQL    string
	Params []interface{}
}
type Walker struct {
	Resolver func(node Node) (Node, error)
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
func isNumber(s string) (interface{}, bool) {
	if ret, err := strconv.ParseFloat(s, 64); err == nil {
		return ret, true
	}
	if ret, err := strconv.ParseFloat(s, 64); err == nil {
		return ret, true
	}
	return nil, false
}
func isDate(s string) (*time.Time, bool) {
	if ret, err := time.Parse("2006-01-02", s); err == nil {
		return &ret, true
	}
	if ret, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return &ret, true
	}
	return nil, false
}

func (w *Walker) Parse(sql string) (string, error) {
	stm, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	return w.walkOnStatement(stm)
}
func (w *Walker) walkOnStatement(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return w.walkOnSelect(stmt)
	case *sqlparser.Insert:
		return w.walkOnInsert(stmt)
	case *sqlparser.Update:
		return w.walkOnUpdate(stmt)
	case *sqlparser.Delete:
		return w.walkOnDelete(stmt)
	default:
		return "", nil
	}

}
func (w *Walker) walkSQLNode(node sqlparser.SQLNode) (string, error) {
	// t := reflect.TypeOf(node)
	if fx, ok := node.(*sqlparser.StarExpr); ok {
		fmt.Println(fx)
		if !fx.TableName.IsEmpty() {
			n, err := w.Resolver(Node{Nt: TableName, V: fx.TableName.Name.String()})
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

		nAlias, err := w.Resolver(Node{Nt: Alias, V: fx.As.String()})

		fmt.Println(reflect.TypeOf(fx.Expr))

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
			n, err := w.Resolver(Node{Nt: Params, V: pName})
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			n, err := w.Resolver(Node{Nt: Value, V: string(fx.Val)})
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
			n, err := w.Resolver(Node{Nt: Params, V: fieldName[1:]})
			if err != nil {
				return "", err
			}
			return n.V, nil
		} else {
			n, err := w.Resolver(Node{Nt: Field, V: fieldName})
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
		n, err := w.Resolver(Node{Nt: TableName, V: fx.Name.String()})
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

	panic(fmt.Sprintf("unsupported type %s in parser.walkSQLNode", reflect.TypeOf(node)))

}
func (w *Walker) walkOnGroupBy(stmt sqlparser.GroupBy) (string, error) {
	ret := []string{}
	for _, expr := range stmt {
		s, err := w.walkSQLNode(expr)
		if err != nil {
			return "", err
		}
		ret = append(ret, s)
	}
	return "GROUP BY " + strings.Join(ret, ", "), nil
}
func (w *Walker) walkOnWhere(stmt *sqlparser.Where) (string, error) {
	fmt.Print(stmt)
	if stmt.Expr != nil {
		where, err := w.walkSQLNode(stmt.Expr)
		if err != nil {
			return "", err
		}
		return "WHERE " + where, nil
	}
	return "", fmt.Errorf("sybnax error")
}
func (w *Walker) walkOnTable(expr sqlparser.TableExprs) (string, error) {
	ret := []string{}
	for _, expr := range expr {
		if tbl, ok := expr.(*sqlparser.AliasedTableExpr); ok {

			strTableName, err := w.walkSQLNode(tbl.Expr)
			if err != nil {
				return "", err
			}

			if !tbl.As.IsEmpty() {
				n, err := w.Resolver(Node{Nt: Alias, V: tbl.As.String()})
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
func (w *Walker) walkOnFuncExpr(expr *sqlparser.FuncExpr) (string, error) {
	funcName := expr.Name.String()
	n, err := w.Resolver(Node{Nt: Function, V: funcName})
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
func (w *Walker) walkSQLParen(expr *sqlparser.ParenExpr) (string, error) {
	fmt.Println(expr)
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
func (w *Walker) walkOnBinaryExpr(expr *sqlparser.BinaryExpr) (string, error) {
	op, err := w.Resolver(Node{Nt: Unanry, V: expr.Operator})
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
func (w *Walker) walkOnSelect(stmt *sqlparser.Select) (string, error) {
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

	return "SELECT " + strings.Join(ret, " "), nil

}
func (w *Walker) walkOnInsert(stmt *sqlparser.Insert) (string, error) {
	panic("not implemented")
}
func (w *Walker) walkOnUpdate(stmt *sqlparser.Update) (string, error) {
	panic("not implemented")
}
func (w *Walker) walkOnDelete(stmt *sqlparser.Delete) (string, error) {
	panic("not implemented")
}
func (w *Walker) walkOnJoinTable(expr *sqlparser.JoinTableExpr) (string, error) {
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
