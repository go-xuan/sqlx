package sqlx

import (
	"fmt"
	"testing"
)

func TestKeywordIndexOfSql(t *testing.T) {
	sql := "select 1 from abc where band=1 and (a=1 and band =2) and c=3 and d=4"
	fmt.Println(sql)
	if index := indexOfKeyword(sql, "and", 4); index != -1 {
		fmt.Println(sql[index:])
		fmt.Println(index)
	}

	if indices := indicesOfKeyword(sql, "and", 4); len(indices) > 0 {
		fmt.Println(indices)
		for _, index := range indices {
			fmt.Println(sql[index:])
		}
	}

}
