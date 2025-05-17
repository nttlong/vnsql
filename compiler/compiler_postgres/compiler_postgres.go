package compiler_postgres

import (
	"strings"

	"github.com/nttlong/vnsql/compiler"
)

type CompilerPostgres struct {
	*compiler.Compiler
}

var Compiler = CompilerPostgres{&compiler.Compiler{
	OnParse:          postgresOnParse,
	OnParseInsertSQL: postgresResolverInsertSQL,
	// ResolverInsertSQL: postgresResolverInsertSQL,
}}

func postgresOnParse(ctx *compiler.Compiler, node compiler.Node) (compiler.Node, error) {

	// if node.Nt == compiler.TableName || node.Nt == compiler.Alias {

	// 	if dbMap != nil {
	// 		findTable := strings.ToLower(node.V)
	// 		fmt.Println(findTable)

	// 		if fn, ok := (*dbMap)[strings.ToLower(node.V)]; ok {
	// 			node.V = "\"" + fn.TableName + "\""
	// 			return node, nil
	// 		}
	// 	}
	// 	node.V = "\"" + node.V + "\""
	// 	return node, nil
	// }
	// if node.Nt == compiler.Field {
	// 	if tblMap != nil {
	// 		if fn, ok := (*tblMap)[strings.ToLower(node.V)]; ok {
	// 			node.V = fn
	// 		}
	// 	}
	// 	if node.TblName != "" {
	// 		if dbMap != nil {
	// 			if fn, ok := (*dbMap)[strings.ToLower(node.TblName)]; ok {
	// 				node.TblName = fn.TableName
	// 				if fn.ColInfos != nil {
	// 					if fn.ColInfos[strings.ToLower(node.V)] != "" {
	// 						node.V = fn.ColInfos[strings.ToLower(node.V)]
	// 					}
	// 				}
	// 			}
	// 		}
	// 		node.V = "\"" + node.TblName + "\".\"" + node.V + "\""
	// 		return node, nil
	// 	}

	// 	node.V = "\"" + node.V + "\""
	// 	return node, nil
	// }
	// if node.Nt == compiler.Params {
	// 	node.V = "$" + node.V[1:]
	// }
	return node, nil

}
func postgresResolverInsertSQL(ctx *compiler.Compiler, sql string, autoValueCols []string, returnColAfterInsert []string) (*string, error) {
	returnCols := []string{}
	for _, colName := range autoValueCols {

		returnCols = append(returnCols, colName)
	}
	if len(returnColAfterInsert) > 0 {
		sql += " returning " + "\"" + strings.Join(returnCols, "\",\"") + "\""
	}

	return &sql, nil
}
