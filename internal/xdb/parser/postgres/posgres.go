package postgres

import (
	"fmt"
	"strings"

	"github.com/nttlong/vnsql/internal/xdb/parser"
)

func PostgresResoler(node parser.Node, tblMap *parser.TableMap) (parser.Node, error) {
	if node.Nt == parser.Function {
		return resoveFunction(node)
	}
	if node.Nt == parser.TableName || node.Nt == parser.Field {
		node.V = "\"" + node.V + "\""
	}
	if node.Nt == parser.Params {
		if node.V[0] == 'v' {
			node.V = "$" + node.V[1:]
		}
	}
	if node.Nt == parser.Value {
		if _, ok := node.IsDate(node.V); ok {
			node.V = "'" + node.V + "'"
		}
		if _, ok := node.IsBool(node.V); ok {
			node.V = "TRUE"
		}
	}
	return node, nil
}

func resoveFunction(node parser.Node) (parser.Node, error) {
	if strings.ToLower(node.V) == "len" {
		node.V = "length"
		return node, nil

	}
	if strings.ToLower(node.V) == "now" {
		node.V = "current_timestamp"
		node.Nt = parser.Field
		return node, nil
	}

	panic(fmt.Sprintf("not support function: %s in xdb/parser/postgres", node.V))
}
