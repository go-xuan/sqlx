package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// ParseSelectSQL 解析查询SQL
func ParseSelectSQL(sql string, indent ...int) *Select {
	// sql初始化
	var parser = &Select{
		Base: NewBase(sql, indent...),
	}

	// sql解析
	parser.parsePrepare() // 解析准备
	parser.parseLimit()   // 解析limit
	parser.parseOrderBy() // 解析order by
	parser.parseFields()  // 解析字段
	parser.parseTable()   // 解析主表
	parser.parseJoins()   // 解析关联子表
	parser.parseWhere()   // 解析where
	parser.parseGroupBy() // 解析group By
	parser.parseHaving()  // 解析having
	parser.parseFinish()  // 解析完成

	return parser
}

type Select struct {
	Base
	Table    *Table       // 查询主表
	Fields   []*Field     // 查询字段
	Joins    []*Join      // 关联子表
	Where    []*Condition // 查询条件
	GroupBy  []string     // 分组条件
	Having   []*Condition // 分组筛选条件
	OrderBy  []string     // 排序条件
	Limit    string       // 限数条件
	Distinct bool         // 是否distinct
}

// Beautify SQL美化输出
func (x *Select) Beautify() string {
	if x.simple {
		return x.originSql
	}
	var sql = strings.Builder{}
	sql.WriteString(x.beautifySelect())
	sql.WriteString(x.beautifyFrom())
	sql.WriteString(x.beautifyWhere())
	sql.WriteString(x.beautifyGroupBy())
	sql.WriteString(x.beautifyHaving())
	sql.WriteString(x.beautifyOrderBy())
	sql.WriteString(x.beautifyLimit())
	if replacer := x.replacer; replacer != nil {
		return replacer.Replace(sql.String())
	} else {
		return sql.String()
	}
}

// 提取查询字段
func (x *Select) parseFields() *Select {
	sql := x.tempSql
	if form, to := utils.BetweenOfString(sql, consts.SELECT+consts.Blank, consts.Blank+consts.FROM+consts.Blank); form >= 0 {
		fieldsSql := sql[form+7 : to]
		if to-form > 16 && fieldsSql[:8] == consts.DISTINCT {
			x.Distinct = true
			fieldsSql = fieldsSql[9:]
		}
		// 判断是否有字段包含括号（子查询或者函数等内部可能会包含","逗号，从而影响字段拆分）
		list, last := utils.SplitExcludeInBracket(fieldsSql, consts.Comma)
		list = append(list, last)
		var fields []*Field
		for _, fieldSql := range list {
			var name, alias string
			fieldSql = strings.TrimSpace(fieldSql)
			if i := utils.FirstIndexOfKeyword(fieldSql, consts.AS); i >= 0 {
				name, alias = fieldSql[:i], fieldSql[i:]
			} else if fieldSql[len(fieldSql)-1:] == consts.RightBracket {
				name = fieldSql
			} else if i = strings.LastIndex(fieldSql, consts.Blank); i >= 0 {
				name, alias = fieldSql[:i], fieldSql[i+1:]
			} else {
				name = fieldSql
			}
			if utils.IndexOfString(name, consts.ReplacePrefix) >= 0 {
				name = x.replacer.Replace(name)
			}
			fields = append(fields, &Field{Name: name, Alias: alias})
		}
		x.Fields = fields
		x.tempSql = sql[to:]
	}
	return x
}

// 提取查询主表
func (x *Select) parseTable() *Select {
	x.Table, x.tempSql = ExtractTable(x.tempSql, x.indent)
	return x
}

// 提取关联子表
func (x *Select) parseJoins() *Select {
	sql := x.tempSql

	var joinSqlList []string
	joinSqlList, sql = utils.SplitExcludeInBracket(sql, consts.JOIN)
	var lastJoin string
	if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from == 1 {
		lastJoin, sql = sql[:to], sql[to:]
	}
	if _, index := utils.ContainsKeywords(sql, consts.WHERE, consts.GroupBy, consts.OrderBy, consts.LIMIT); index >= 0 {
		lastJoin, sql = lastJoin+sql[:index], sql[index:]
	} else {
		lastJoin, sql = lastJoin+sql, consts.Empty
	}
	joinSqlList = append(joinSqlList, lastJoin)
	if len(joinSqlList) > 0 {
		var joinType string
		var joins []*Join
		for i, joinSql := range joinSqlList {
			if i == 0 {
				joinType = strings.TrimSpace(joinSql)
			} else {
				var join = &Join{}
				var space = x.indent - 1
				if joinType == consts.Empty {
					space = space - 5
				}
				join.Type = joinType

				if hit, index := utils.LastIndexOfKeys(joinSql, consts.LEFT, consts.RIGHT, consts.INNER, consts.OUTER); index >= 0 {
					joinType = hit
					joinSql = joinSql[:index-1]
				}

				if index := utils.LastIndexOfKeyword(joinSql, consts.ON); index >= 0 {
					join.On, joinSql = joinSql[index+3:], joinSql[:index-1]
				}

				join.Table, _ = ExtractTable(joinSql, space+6)
				joins = append(joins, join)
			}
		}
		x.Joins = joins
		x.tempSql = sql
	}
	return x
}

// 提取查询条件
func (x *Select) parseWhere() *Select {
	if sql := x.tempSql; sql != "" {
		x.Where, x.tempSql = ExtractWhere(sql)
	}
	return x
}

// 提取group by
func (x *Select) parseGroupBy() *Select {
	sql := x.tempSql
	if index := utils.FirstIndexOfKeyword(sql, consts.GroupBy); index >= 0 {
		var groupBySql string
		if _, i := utils.ContainsKeywords(sql, consts.HAVING, consts.OrderBy, consts.LIMIT); i >= 0 {
			groupBySql, sql = sql[index+9:i], sql[i:]
		} else {
			groupBySql, sql = sql, consts.Empty
		}
		groupBys := strings.Split(groupBySql, consts.Comma)
		for i := range groupBys {
			groupBys[i] = strings.TrimSpace(groupBys[i])
		}
		x.GroupBy = groupBys
	}
	x.tempSql = sql
	return x
}

// 提取having
func (x *Select) parseHaving() *Select {
	sql := x.tempSql
	if index := utils.FirstIndexOfKeyword(sql, consts.HAVING); index >= 0 {
		sql = sql[index+6:]
		var havingSql string
		if _, i := utils.ContainsKeywords(sql, consts.OrderBy, consts.LIMIT); i >= 0 {
			havingSql, sql = sql[:i], sql[i:]
		} else {
			havingSql, sql = sql, consts.Empty
		}
		x.Having = NewConditions(havingSql)
	}
	x.tempSql = sql
	return x
}

// 提取order by
func (x *Select) parseOrderBy() *Select {
	sql := x.tempSql
	if index := utils.LastIndexOfKeyword(sql, consts.OrderBy); index > 0 {
		var orderBySql string
		if i := utils.IndexOfString(sql, consts.RightBracket, -1); i < index {
			// 排除子查询中的order by，只取主查询的order by
			orderBySql, sql = sql[index+9:], sql[:index-1]
		}
		if orderBySql != consts.Empty {
			x.OrderBy = strings.Split(orderBySql, consts.Comma)
		}
	}
	x.tempSql = sql
	return x
}

// 提取limit
func (x *Select) parseLimit() *Select {
	sql := x.tempSql
	i := utils.LastIndexOfKeyword(sql, consts.LIMIT)
	j := utils.IndexOfString(sql, consts.RightBracket, -1)
	if i > 0 && i > j {
		x.Limit, sql = sql[i+6:], sql[:i]
	}
	x.tempSql = sql
	return x
}

// 构建查询字段sql
func (x *Select) beautifySelect() string {
	var sql = strings.Builder{}
	var space = 1
	sql.WriteString(consts.SELECT)
	sql.WriteString(consts.Blank)
	if x.Distinct {
		sql.WriteString(consts.DISTINCT)
		sql.WriteString(consts.Blank)
		space += 9
	}
	var fieldAlign, aliasNum int
	for _, field := range x.Fields {
		y := len(field.Name)
		if fieldAlign < y {
			fieldAlign = y
		}
		if field.Alias != consts.Empty {
			aliasNum++
		}
	}
	fieldNum := len(x.Fields)
	for i, field := range x.Fields {
		if i > 0 {
			sql.WriteString(consts.Comma)
			if aliasNum > 0 || fieldNum >= 6 {
				sql.WriteString(consts.NextLine)
				sql.WriteString(Align(x.indent + space))
			} else {
				sql.WriteString(consts.Blank)
			}
		}
		sql.WriteString(field.Name)
		if field.Alias != consts.Empty {
			sql.WriteString(Align(fieldAlign - len(field.Name)))
			sql.WriteString(field.Alias)
		}
	}
	return sql.String()
}

func (x *Select) beautifyFrom() string {
	sql := strings.Builder{}
	sql.WriteString(consts.NextLine)
	sql.WriteString(x.align(consts.FROM))
	sql.WriteString(consts.Blank)
	sql.WriteString(x.Table.beautify(true))
	for _, join := range x.Joins {
		sql.WriteString(consts.NextLine)
		if join.Type != consts.Empty {
			sql.WriteString(x.align(join.Type))
			sql.WriteString(consts.Blank)
			sql.WriteString(consts.JOIN)
		} else {
			sql.WriteString(x.align(consts.JOIN))
		}
		sql.WriteString(consts.Blank)
		sql.WriteString(join.Table.beautify(true))
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.ON))
		sql.WriteString(consts.Blank)
		sql.WriteString(join.On)
	}
	return sql.String()
}

func (x *Select) beautifyWhere() string {
	if conditions := x.Where; len(conditions) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.WHERE))
		sql.WriteString(consts.Blank)
		for i, condition := range conditions {
			if i > 0 {
				sql.WriteString(consts.NextLine)
			}
			sql.WriteString(condition.beautify(x.indent))
		}
		return sql.String()
	}
	return ""
}

func (x *Select) beautifyHaving() string {
	if conditions := x.Having; len(conditions) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.HAVING))
		sql.WriteString(consts.Blank)
		for i, condition := range conditions {
			if i > 0 {
				sql.WriteString(consts.NextLine)
			}
			sql.WriteString(condition.beautify(x.indent))
		}
		return sql.String()
	}
	return ""
}

func (x *Select) beautifyOrderBy() string {
	if values := x.OrderBy; len(values) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.OrderBy))
		sql.WriteString(consts.Blank)
		var max, nextLine = 0, false
		for _, value := range values {
			if max = max + len(value); max > 100 {
				nextLine = true
				break
			}
		}
		for i, value := range values {
			value = strings.TrimSpace(value)
			if i > 0 {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.Blank)
				if nextLine {
					sql.WriteString(consts.NextLine)
					sql.WriteString(Align(x.indent + 4))
				}
			}
			sql.WriteString(value)
		}
		return sql.String()
	}
	return ""
}

func (x *Select) beautifyGroupBy() string {
	if values := x.GroupBy; len(values) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.GroupBy))
		sql.WriteString(consts.Blank)
		var max, nextLine = 0, false
		for _, value := range values {
			if max = max + len(value); max > 100 {
				nextLine = true
				break
			}
		}
		for i, value := range values {
			if i > 0 {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.Blank)
				if nextLine {
					sql.WriteString(consts.NextLine)
					sql.WriteString(Align(x.indent + 4))
				}
			}
			sql.WriteString(value)
		}
		return sql.String()
	}
	return ""
}

func (x *Select) beautifyLimit() string {
	sql := strings.Builder{}
	if x.Limit != consts.Empty {
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.LIMIT))
		sql.WriteString(consts.Blank)
		sql.WriteString(x.Limit)
	}
	return sql.String()
}
