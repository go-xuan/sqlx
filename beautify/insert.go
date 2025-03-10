package beautify

import (
	"fmt"
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// ParseInsertSQL 解析插入SQL
func ParseInsertSQL(sql string, indent ...int) *Insert {
	// sql初始化
	var parser = &Insert{
		Base: NewBase(sql, indent...),
	}

	// sql解析
	parser.parsePrepare()  // 解析准备
	parser.parseTable()    // 解析主表
	parser.extractFields() // 解析字段
	parser.extractValues() // 解析插入值
	parser.parseFinish()   // 解析完成

	return parser
}

type Insert struct {
	Base
	Table     *Table     // 插入表
	Fields    []*Field   // 插入字段
	ValueData [][]string // 插入值
	Query     *Select    // 子查询
}

func (x *Insert) Beautify() string {
	var sql = strings.Builder{}
	sql.WriteString(x.beautifyInsert())
	sql.WriteString(x.beautifyFields())
	sql.WriteString(x.beautifyValues())
	if replacer := x.replacer; replacer != nil {
		return replacer.Replace(sql.String())
	} else {
		return sql.String()
	}
}

// 构建查询字段sql
func (x *Insert) beautifyInsert() string {
	var sql = strings.Builder{}
	sql.WriteString("insert into ")
	sql.WriteString(x.Table.beautify())
	sql.WriteString(consts.NextLine)
	return sql.String()
}

// 构建查询字段sql
func (x *Insert) beautifyFields() string {
	var sql = strings.Builder{}
	var maxLen int
	for _, field := range x.Fields {
		maxLen += len(field.Name)
	}

	var nextLine bool
	if maxLen > 120 || len(x.Fields) > 10 {
		nextLine = true
	}
	sql.WriteString(x.align(consts.LeftBracket))
	for i, field := range x.Fields {
		if i > 0 {
			sql.WriteString(consts.Comma)
			if nextLine {
				sql.WriteString(consts.NextLine)
				sql.WriteString(x.align())
			} else {
				sql.WriteString(consts.Blank)
			}
		}
		sql.WriteString(field.Name)
	}
	sql.WriteString(consts.RightBracket)
	sql.WriteString(consts.NextLine)
	return sql.String()
}

// 构建查询字段sql
func (x *Insert) beautifyValues() string {
	var sql = strings.Builder{}
	if x.Query != nil {
		sql.WriteString(x.Query.Beautify())
	} else if x.ValueData != nil {
		var nextLine bool
		if len(x.Fields) >= 10 {
			nextLine = true
		}
		sql.WriteString(consts.VALUES)
		sql.WriteString(consts.NextLine)
		for i, values := range x.ValueData {
			if i > 0 {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.NextLine)
			}
			sql.WriteString(x.align(consts.LeftBracket))
			for j, value := range values {
				if j > 0 {
					sql.WriteString(consts.Comma)
					if nextLine {
						sql.WriteString(consts.NextLine)
						sql.WriteString(x.align())
					} else {
						sql.WriteString(consts.Blank)
					}
				}
				sql.WriteString(value)
			}
			if nextLine {
				sql.WriteString(consts.NextLine)
				sql.WriteString(x.align(consts.RightBracket))
			} else {
				sql.WriteString(consts.RightBracket)
			}
		}
	}
	return sql.String()
}

func (x *Insert) parseTable() *Insert {
	sql := x.tempSql
	// 去除insert关键字
	if index := utils.IndexOfKeywordFirst(sql, consts.INSERT); index == 0 {
		sql = sql[7:]
	}
	// 去除into关键字
	if index := utils.IndexOfKeywordFirst(sql, consts.INTO); index == 0 {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if index := utils.IndexOfString(sql, consts.LeftBracket); index >= 0 {
		x.Table = &Table{
			Name: strings.TrimSpace(sql[:index-1]),
		}
		x.tempSql = sql[index:]
	}
	return x
}

func (x *Insert) extractFields() *Insert {
	sql := x.tempSql
	// 根据set关键字进行拆分
	if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from >= 0 && from < to {
		x.tempSql = sql[to+2:]
		sql = sql[from+1 : to]
	}
	if names := strings.Split(sql, consts.Comma); len(names) > 0 {
		var fields []*Field
		for _, name := range names {
			fields = append(fields, &Field{Name: name})
		}
		x.Fields = fields
	}
	return x
}

func (x *Insert) extractValues() *Insert {
	sql := strings.TrimLeft(x.tempSql, consts.Blank)
	if index := utils.IndexOfKeywordFirst(sql, consts.SELECT); index == 0 {
		if query := ParseSelectSQL(sql); query != nil && len(query.Fields) == len(x.Fields) {
			x.Query = query
		} else {
			panic("select字段数量和insert字段数量不匹配")
		}
		return x
	}
	// 去除values关键字
	if index := utils.IndexOfKeywordFirst(sql, consts.VALUES); index == 0 {
		sql = sql[7:]
	}

	if index := utils.IndexOfKeywordFirst(sql, consts.VALUE); index == 0 {
		sql = sql[6:]
	}

	// 根据逗号进行拆分所有插入值
	valuesList, lastValues := utils.SplitExcludeInBracket(sql, consts.Comma)

	// 去除最后一组values的分号
	if index := utils.IndexOfString(lastValues, consts.Semicolon, -1); index >= 0 {
		lastValues = lastValues[:index]
	}

	valuesList = append(valuesList, lastValues)
	for _, valuesSql := range valuesList {
		if values := utils.SplitValuesSql(valuesSql); len(values) == len(x.Fields) {
			x.ValueData = append(x.ValueData, values)
		} else {
			var names []string
			for _, field := range x.Fields {
				names = append(names, field.Name)
			}
			panic(fmt.Sprintf("insert字段数量和insert值数量不匹配：%v != %v", names, values))
		}
	}

	return x
}

//func (x *Insert) addFieldValue(valuesSql string) {
//	if values := utils.SplitValuesSql(valuesSql); len(values) == len(x.Fields) {
//		for i, field := range x.Fields {
//			field.ValueData = append(field.ValueData, strings.TrimSpace(values[i]))
//		}
//	} else {
//		panic(valuesSql + " the number of insert values does not match")
//	}
//}
