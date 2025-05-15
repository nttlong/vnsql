package compilerpostgres

import (
	"strings"

	"github.com/nttlong/vnsql/compiler"
	"github.com/nttlong/vnsql/types"
)

type WalkerPostgres struct {
	*compiler.Walker
}

var Walker = WalkerPostgres{&compiler.Walker{
	Resolver:          postgresResolver,
	ResolverInsertSQL: postgresResolverInsertSQL,
}}

func postgresResolver(node compiler.Node, tblMap *compiler.TableMap) (compiler.Node, error) {

	if node.Nt == compiler.TableName || node.Nt == compiler.Field {
		if tblMap != nil {
			if fn, ok := (*tblMap)[strings.ToLower(node.V)]; ok {
				node.V = fn
			}
		}
		node.V = "\"" + node.V + "\""
		return node, nil
	}
	if node.Nt == compiler.Params {
		node.V = "$" + node.V[1:]
	}
	return node, nil

}
func postgresResolverInsertSQL(sql string, tbl types.TableInfo) (*string, error) {
	returnCols := []string{}
	for _, col := range tbl.AutoValueCols {

		returnCols = append(returnCols, col.Name)
	}
	if len(returnCols) > 0 {
		sql += " returning " + "\"" + strings.Join(returnCols, "\",\"") + "\""
	}

	return &sql, nil
}
