package utils

import (
	"fmt"
	"github.com/go-xuan/sqlx/consts"
	"strings"
	"testing"
)

func TestKeywordIndexOfSql(t *testing.T) {
	sql := "select 1 from abc where band=1 and (a=1 and band =2) and c=3 and d=4"
	fmt.Println(sql)
	if index := IndexOfKeyword(sql, "and", 4); index != -1 {
		fmt.Println(sql[index:])
		fmt.Println(index)
	}

	if indices := IndicesOfKeyword(sql, "and", 4); len(indices) > 0 {
		fmt.Println(indices)
		for _, index := range indices {
			fmt.Println(sql[index:])
		}
	}

}

func TestSplitExcludeInBracket(t *testing.T) {
	sql := "(2.2.2), (1,1,2);"
	if list, last := SplitExcludeInBracket(sql, consts.Comma); len(list) >= 0 {
		fmt.Println(list)
		fmt.Println(last)
		if index := IndexOfString(last, consts.Semicolon, -1); index >= 0 {
			last = last[:index]
		}
		fmt.Println(last)
		fmt.Println(strings.Trim(last, "() "))
		fmt.Println(strings.Fields(last))

	}
}
