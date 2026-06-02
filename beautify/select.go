package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/model"
	"github.com/go-xuan/sqlx/utils"
)

// ParseSelectSQL 解析查询SQL
func ParseSelectSQL(sql string, indent ...int) (*Select, error) {
	var parser = &Select{
		SQL: NewSQL(sql, indent...),
	}

	parser.parsePrepare()
	parser.parseLimit()
	parser.parseOrderBy()
	parser.parseFields()
	if err := parser.parseTable(); err != nil {
		return nil, err
	}
	if err := parser.parseJoins(); err != nil {
		return nil, err
	}
	if err := parser.parseWhere(); err != nil {
		return nil, err
	}
	parser.parseGroupBy()
	if err := parser.parseHaving(); err != nil {
		return nil, err
	}
	parser.parseFinish()

	return parser, nil
}

type Select struct {
	SQL
	Table    *model.Table
	Fields   []*model.Field
	Joins    []*model.Join
	Where    []*model.Filter
	GroupBy  []string
	Having   []*model.Filter
	OrderBy  []string
	Limit    string
	Distinct bool
}

// Beautify SQL美化输出
func (x *Select) Beautify() string {
	var builder = strings.Builder{}
	builder.WriteString(x.beautifySelect())
	builder.WriteString(x.beautifyFrom())
	builder.WriteString(x.beautifyWhere())
	builder.WriteString(x.beautifyGroupBy())
	builder.WriteString(x.beautifyHaving())
	builder.WriteString(x.beautifyOrderBy())
	builder.WriteString(x.beautifyLimit())
	sql := builder.String()
	if x.replacer != nil {
		return x.replacer.Replace(sql)
	}
	return sql
}

// ToModel 转换为 model.Select
func (x *Select) ToModel() *model.Select {
	return &model.Select{
		Table:    x.Table,
		Fields:   x.Fields,
		Joins:    x.Joins,
		Where:    x.Where,
		GroupBy:  x.GroupBy,
		Having:   x.Having,
		OrderBy:  x.OrderBy,
		Limit:    x.Limit,
		Distinct: x.Distinct,
		SQL:      x.Beautify(),
	}
}

// 提取查询字段
func (x *Select) parseFields() {
	sql := x.temp
	if form, to := utils.BetweenOfString(sql, consts.SELECT+consts.Blank, consts.Blank+consts.FROM+consts.Blank); form >= 0 {
		fieldsSql := sql[form+len(consts.SELECT)+1 : to]
		if to-form > 16 && fieldsSql[:len(consts.DISTINCT)] == consts.DISTINCT {
			x.Distinct = true
			fieldsSql = fieldsSql[len(consts.DISTINCT)+1:]
		}
		list, last := utils.SplitExcludeInBracket(fieldsSql, consts.Comma)
		list = append(list, last)
		var fields []*model.Field
		for _, fieldSql := range list {
			var name, alias string
			fieldSql = strings.TrimSpace(fieldSql)
			if first := utils.IndexOfKeywordFirst(fieldSql, consts.AS); first >= 0 {
				name, alias = fieldSql[:first], fieldSql[first:]
			} else if fieldSql[len(fieldSql)-1:] == consts.RightBracket {
				name = fieldSql
			} else if first = strings.LastIndex(fieldSql, consts.Blank); first >= 0 {
				name, alias = fieldSql[:first], fieldSql[first+1:]
			} else {
				name = fieldSql
			}
			if x.replacer != nil && utils.IndexOfString(name, consts.ReplacePrefix) >= 0 {
				name = x.replacer.Replace(name)
			}
			fields = append(fields, &model.Field{Name: name, Alias: alias})
		}
		x.Fields = fields
		x.temp = sql[to:]
	}
}

// 提取查询主表
func (x *Select) parseTable() error {
	table, remaining, err := ExtractTable(x.temp, x.indent)
	if err != nil {
		return err
	}
	x.Table = table
	x.temp = remaining
	return nil
}

// 提取关联子表
func (x *Select) parseJoins() error {
	sql := x.temp

	var joinSqlList []string
	joinSqlList, sql = utils.SplitExcludeInBracket(sql, consts.JOIN)
	var lastJoin string
	if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from == 1 {
		lastJoin, sql = sql[:to], sql[to:]
	}
	if _, index := utils.ContainsKeywords(sql, consts.WHERE, consts.GROUPBY, consts.ORDERBY, consts.LIMIT); index >= 0 {
		lastJoin, sql = lastJoin+sql[:index], sql[index:]
	} else {
		lastJoin, sql = lastJoin+sql, consts.Empty
	}
	joinSqlList = append(joinSqlList, lastJoin)
	if len(joinSqlList) > 0 {
		var joinType string
		var joins []*model.Join
		for i, joinSql := range joinSqlList {
			if i == 0 {
				joinType = strings.TrimSpace(joinSql)
			} else {
				var join = &model.Join{}
				var space = x.indent - 1
				if joinType == consts.Empty {
					space = space - 5
				}
				join.Type = joinType

				if hit, index := utils.LastIndexOfKeys(joinSql, consts.LEFT, consts.RIGHT, consts.INNER, consts.OUTER); index >= 0 {
					joinType = hit
					joinSql = joinSql[:index-1]
				}

				if last := utils.IndexOfKeywordLast(joinSql, consts.ON); last >= 0 {
					join.On = strings.TrimSpace(joinSql[last+len(consts.ON)+1:])
					joinSql = joinSql[:last-1]
				}

				table, _, err := ExtractTable(joinSql, space+6)
				if err != nil {
					return err
				}
				join.Table = table
				joins = append(joins, join)
			}
		}
		x.Joins = joins
		x.temp = sql
	}
	return nil
}

// 提取查询条件
func (x *Select) parseWhere() error {
	if sql := x.temp; sql != "" {
		var err error
		x.Where, x.temp, err = ExtractWhere(sql)
		return err
	}
	return nil
}

// 提取group by
func (x *Select) parseGroupBy() {
	sql := x.temp
	if first := utils.IndexOfKeywordFirst(sql, consts.GROUPBY); first >= 0 {
		var groupBySql string
		if _, i := utils.ContainsKeywords(sql, consts.HAVING, consts.ORDERBY, consts.LIMIT); i >= 0 {
			groupBySql, sql = sql[first+len(consts.GROUPBY)+1:i], sql[i:]
		} else {
			groupBySql, sql = sql[first+len(consts.GROUPBY)+1:], consts.Empty
		}
		groupBys := strings.Split(groupBySql, consts.Comma)
		for i := range groupBys {
			groupBys[i] = strings.TrimSpace(groupBys[i])
		}
		x.GroupBy = groupBys
	}
	x.temp = sql
}

// 提取having
func (x *Select) parseHaving() error {
	sql := x.temp
	if first := utils.IndexOfKeywordFirst(sql, consts.HAVING); first >= 0 {
		sql = sql[first+len(consts.HAVING):]
		var havingSql string
		if _, i := utils.ContainsKeywords(sql, consts.ORDERBY, consts.LIMIT); i >= 0 {
			havingSql, sql = sql[:i], sql[i:]
		} else {
			havingSql, sql = sql, consts.Empty
		}
		var err error
		x.Having, err = NewConditions(havingSql)
		if err != nil {
			return err
		}
	}
	x.temp = sql
	return nil
}

// 提取order by
func (x *Select) parseOrderBy() {
	sql := x.temp
	if last := utils.IndexOfKeywordLast(sql, consts.ORDERBY); last > 0 {
		var orderBySql string
		if index := utils.IndexOfString(sql, consts.RightBracket, -1); index < last {
			orderBySql, sql = sql[last+len(consts.ORDERBY)+1:], sql[:last-1]
		}
		if orderBySql != consts.Empty {
			x.OrderBy = strings.Split(orderBySql, consts.Comma)
		}
	}
	x.temp = sql
}

// 提取limit
func (x *Select) parseLimit() {
	sql := x.temp
	last := utils.IndexOfKeywordLast(sql, consts.LIMIT)
	index := utils.IndexOfString(sql, consts.RightBracket, -1)
	if last > 0 && last > index {
		x.Limit, sql = sql[last+len(consts.LIMIT)+1:], sql[:last]
	}
	x.temp = sql
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
	sql.WriteString(tableSQL(x.Table, true))
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
		sql.WriteString(tableSQL(join.Table, true))
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.ON))
		sql.WriteString(consts.Blank)
		sql.WriteString(join.On)
	}
	return sql.String()
}

func (x *Select) beautifyWhere() string {
	if filters := x.Where; len(filters) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.WHERE))
		sql.WriteString(consts.Blank)
		for i, f := range filters {
			if i > 0 {
				sql.WriteString(consts.NextLine)
			}
			sql.WriteString(filterSQL(f, x.indent))
		}
		return sql.String()
	}
	return ""
}

func (x *Select) beautifyHaving() string {
	if filters := x.Having; len(filters) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.HAVING))
		sql.WriteString(consts.Blank)
		for i, f := range filters {
			if i > 0 {
				sql.WriteString(consts.NextLine)
			}
			sql.WriteString(filterSQL(f, x.indent))
		}
		return sql.String()
	}
	return ""
}

func (x *Select) beautifyOrderBy() string {
	if values := x.OrderBy; len(values) > 0 {
		sql := strings.Builder{}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.ORDERBY))
		sql.WriteString(consts.Blank)
		ifNextLine := utils.IfNextLine(values, 0, 100)
		for i, value := range values {
			value = strings.TrimSpace(value)
			if i > 0 {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.Blank)
				if ifNextLine {
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
		sql.WriteString(x.align(consts.GROUPBY))
		sql.WriteString(consts.Blank)
		ifNextLine := utils.IfNextLine(values, 0, 100)
		for i, value := range values {
			if i > 0 {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.Blank)
				if ifNextLine {
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
