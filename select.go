package sqlx

import (
	"strings"
)

type SelectParser struct {
	ParserBase
	Table    *TableParser       // 查询主表
	Fields   []*FieldParser     // 查询字段
	Joins    []*JoinParser      // 关联子表
	Where    []*ConditionParser // 查询条件
	GroupBy  []string           // 分组条件
	Having   []*ConditionParser // 分组筛选条件
	OrderBy  []string           // 排序条件
	Limit    string             // 限数条件
	Distinct bool               // 是否distinct
}

// Beautify SQL美化输出
func (p *SelectParser) Beautify() string {
	var sql = strings.Builder{}
	sql.WriteString(p.buildSelectSql())
	sql.WriteString(p.buildFromSql())
	sql.WriteString(p.buildConditionSql(WHERE))
	sql.WriteString(p.buildGroupOrderSql(GROUP))
	sql.WriteString(p.buildConditionSql(HAVING))
	sql.WriteString(p.buildGroupOrderSql(ORDER))
	sql.WriteString(p.buildLimitSql())
	if replacer := p.replacer; replacer != nil {
		return replacer.Replace(sql.String())
	} else {
		return sql.String()
	}
}

// 提取查询字段
func (p *SelectParser) parseFields() *SelectParser {
	sql := p.tempSql
	if start, end := betweenOfSql(sql, SELECT, FROM); start >= 0 {
		fieldsSql := sql[start+1 : end]
		if end-start > 9 && fieldsSql[:8] == DISTINCT {
			p.Distinct = true
			fieldsSql = fieldsSql[9:]
		}
		// 判断是否有字段包含括号（子查询或者函数等内部可能会包含","逗号，从而影响字段拆分）
		var sqlList, lastSql = splitIgnoreInBracket(fieldsSql, Comma)
		sqlList = append(sqlList, lastSql)
		var fields []*FieldParser
		for _, fieldSql := range sqlList {
			var name, alias string
			fieldSql = strings.TrimSpace(fieldSql)
			if i := keywordIndexOfSql(fieldSql, AS); i >= 0 {
				name, alias = fieldSql[:i], fieldSql[i:]
			} else if fieldSql[len(fieldSql)-1:] == RightBracket {
				name = fieldSql
			} else if i = strings.LastIndex(fieldSql, Blank); i >= 0 {
				name, alias = fieldSql[:i], fieldSql[i+1:]
			} else {
				name = fieldSql
			}
			if indexOfSql(name, ReplacePrefix) >= 0 {
				name = p.replacer.Replace(name)
			}
			fields = append(fields, &FieldParser{Name: name, Alias: alias})
		}
		p.Fields = fields
		p.tempSql = sql[end:]
	}
	return p
}

// 提取查询主表
func (p *SelectParser) parseTable() *SelectParser {
	p.Table, p.tempSql = extractTable(p.tempSql, p.indent)
	return p
}

// 提取关联子表
func (p *SelectParser) parseJoins() *SelectParser {
	sql := p.tempSql

	var sqlList []string
	sqlList, sql = splitIgnoreInBracket(sql, JOIN)
	var lastJoin string
	if x, y := betweenOfSql(sql, LeftBracket, RightBracket); x == 2 {
		lastJoin, sql = sql[:y], sql[y:]
	}
	if _, i := containsKeywords(sql, WHERE, GroupBy, OrderBy, LIMIT); i >= 0 {
		lastJoin, sql = lastJoin+sql[:i], sql[i:]
	} else {
		lastJoin, sql = lastJoin+sql, Empty
	}
	sqlList = append(sqlList, lastJoin)
	if len(sqlList) > 0 {
		var joinType string
		var joins []*JoinParser
		for i, joinSql := range sqlList {
			if i == 0 {
				joinType = strings.TrimSpace(joinSql)
			} else {
				var join = &JoinParser{}
				var space = p.indent - 1
				if joinType == Empty {
					space = space - 5
				}
				join.Type = joinType

				if a := lastIndexOfKeywords(joinSql, LEFT, RIGHT, INNER, OUTER); a >= 0 {
					joinType = strings.TrimSpace(joinSql[a:])
					joinSql = joinSql[:a-1]
				}
				if a := keywordIndexOfSql(joinSql, ON, -1); a >= 0 {
					join.On, joinSql = joinSql[a+3:], joinSql[:a-1]
				}

				join.Table, _ = extractTable(joinSql, space+6)
				joins = append(joins, join)
			}
		}
		p.Joins = joins
		p.tempSql = sql
	}
	return p
}

// 提取查询条件
func (p *SelectParser) parseWhere() *SelectParser {
	p.Where, p.tempSql = extractWhere(p.tempSql)
	return p
}

// 提取group By
func (p *SelectParser) parseGroupBy() *SelectParser {
	sql := p.tempSql
	if index := keywordIndexOfSql(sql, GroupBy); index >= 0 {
		var groupBySql string
		if _, j := containsKeywords(sql, HAVING, OrderBy, LIMIT); j >= 0 {
			groupBySql, sql = sql[index+9:j], sql[j:]
		} else {
			groupBySql, sql = sql, Empty
		}
		p.GroupBy = strings.Split(groupBySql, Comma)
	}
	p.tempSql = sql
	return p
}

// 提取having
func (p *SelectParser) parseHaving() *SelectParser {
	sql := p.tempSql
	if index := keywordIndexOfSql(sql, HAVING); index >= 0 {
		sql = sql[index+6:]
	}
	var havingSql string
	if _, index := containsKeywords(sql, OrderBy, LIMIT); index >= 0 {
		havingSql, sql = sql[:index], sql[index:]
	} else {
		havingSql, sql = sql, Empty
	}
	var sqlList, lastSql = splitIgnoreInBracket(havingSql, AND)
	sqlList = append(sqlList, lastSql)
	if len(sqlList) > 0 {
		var conditions []*ConditionParser
		for _, conditionSql := range sqlList {
			conditions = append(conditions, &ConditionParser{Content: strings.TrimSpace(conditionSql)})
		}
		p.Having = conditions
	}
	p.tempSql = sql
	return p
}

// 提取order by
func (p *SelectParser) parseOrderBy() *SelectParser {
	sql := p.tempSql
	if index := keywordIndexOfSql(sql, OrderBy, -1); index > 0 {
		var orderBySql string
		if j := keywordIndexOfSql(sql, RightBracket, -1); index > j {
			orderBySql, sql = sql[index+9:], sql[:index-1]
		}
		if orderBySql != Empty {
			p.OrderBy = strings.Split(orderBySql, Comma)
		}
	}
	p.tempSql = sql
	return p
}

// 提取limit
func (p *SelectParser) parseLimit() *SelectParser {
	sql := p.tempSql
	i := keywordIndexOfSql(sql, LIMIT, -1)
	j := keywordIndexOfSql(sql, RightBracket, -1)
	if i > 0 && i > j {
		p.Limit, sql = sql[i+6:], sql[:i]
	}
	p.tempSql = sql
	return p
}

// 新行以当前缩进量添加空格
func (p *SelectParser) newLineSpace(n int) string {
	return strings.Repeat(Blank, p.indent+n)
}

// 构建查询字段sql
func (p *SelectParser) buildSelectSql() string {
	var sql = strings.Builder{}
	var space = 1
	sql.WriteString(SELECT)
	sql.WriteString(Blank)
	if p.Distinct {
		sql.WriteString(DISTINCT)
		sql.WriteString(Blank)
		space += 9
	}
	var maxLen, aliasNum int
	for _, field := range p.Fields {
		y := len(field.Name)
		if maxLen < y {
			maxLen = y
		}
		if field.Alias != Empty {
			aliasNum++
		}
	}
	fieldNum := len(p.Fields)
	for i, field := range p.Fields {
		if i > 0 {
			sql.WriteString(Comma)
			if aliasNum > 0 || fieldNum >= 6 {
				sql.WriteString(NewLine)
				sql.WriteString(strings.Repeat(Blank, p.indent+space))
			} else {
				sql.WriteString(Blank)
			}
		}
		sql.WriteString(field.Name)
		if field.Alias != Empty {
			sql.WriteString(strings.Repeat(Blank, maxLen-len(field.Name)))
			sql.WriteString(field.Alias)
		}
	}
	return sql.String()
}

func (p *SelectParser) buildFromSql() string {
	sql := strings.Builder{}
	sql.WriteString(NewLine)
	sql.WriteString(p.align(FROM))
	sql.WriteString(Blank)
	sql.WriteString(p.Table.AliasSQL(true))
	for _, join := range p.Joins {
		sql.WriteString(NewLine)
		if join.Type != Empty {
			sql.WriteString(p.align(join.Type))
			sql.WriteString(Blank)
			sql.WriteString(JOIN)
		} else {
			sql.WriteString(p.align(JOIN))
		}
		sql.WriteString(Blank)
		sql.WriteString(join.Table.AliasSQL(true))
		sql.WriteString(NewLine)
		sql.WriteString(p.align(ON))
		sql.WriteString(Blank)
		sql.WriteString(join.On)
	}
	return sql.String()
}

func (p *SelectParser) buildConditionSql(in string) string {
	var conditions []*ConditionParser
	switch in {
	case WHERE:
		conditions = p.Where
	case HAVING:
		conditions = p.Having
	default:
		in = WHERE
		conditions = p.Where
	}
	sql := strings.Builder{}
	if len(conditions) > 0 {
		sql.WriteString(NewLine)
		sql.WriteString(p.align(in))
		sql.WriteString(Blank)
		for i, cond := range conditions {
			if i > 0 {
				sql.WriteString(NewLine)
				if cond.Type == Empty {
					sql.WriteString(p.align(AND))
					sql.WriteString(Blank)
				} else {
					sql.WriteString(p.align(cond.Type))
					sql.WriteString(Blank)
				}
			}
			sql.WriteString(cond.Content)
		}
	}
	return sql.String()
}

func (p *SelectParser) buildGroupOrderSql(in string) string {
	var values []string
	switch in {
	case GROUP:
		values = p.GroupBy
	case ORDER:
		values = p.OrderBy
	}
	sql := strings.Builder{}
	if len(values) > 0 {
		sql.WriteString(NewLine)
		sql.WriteString(p.align(in))
		sql.WriteString(Blank)
		sql.WriteString(By)
		sql.WriteString(Blank)
		for i, value := range values {
			value = strings.TrimSpace(value)
			if i > 0 {
				sql.WriteString(Comma)
				sql.WriteString(NewLine)
				sql.WriteString(strings.Repeat(Blank, p.indent+4))
			}
			sql.WriteString(value)
		}
	}
	return sql.String()
}

func (p *SelectParser) buildLimitSql() string {
	sql := strings.Builder{}
	if p.Limit != Empty {
		sql.WriteString(NewLine)
		sql.WriteString(p.align(LIMIT))
		sql.WriteString(Blank)
		sql.WriteString(p.Limit)
	}
	return sql.String()
}
