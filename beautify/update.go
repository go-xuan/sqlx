package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// ParseUpdateSQL 解析更新SQL
func ParseUpdateSQL(sql string, indent ...int) *Update {
	// sql初始化
	var parser = &Update{
		SQL: NewSQL(sql, indent...),
	}
	// sql解析
	parser.parsePrepare() // 解析准备
	parser.parseTable()   // 解析主表
	parser.parseFields()  // 解析字段
	parser.parseWhere()   // 解析where
	parser.parseFinish()  // 解析完成

	return parser
}

type Update struct {
	SQL
	Table  *Table       // 更新表
	Fields []*Field     // 更新字段
	Where  []*Condition // 查询条件
}

func (x *Update) Beautify() string {
	var builder = strings.Builder{}
	builder.WriteString(x.beautifyUpdate())
	builder.WriteString(x.beautifyFields())
	builder.WriteString(x.beautifyCondition())
	if sql, replacer := builder.String(), x.replacer; replacer != nil {
		return replacer.Replace(sql)
	} else {
		return sql
	}
}

// 构建查询字段sql
func (x *Update) beautifyUpdate() string {
	var sql = strings.Builder{}
	sql.WriteString(consts.UPDATE)
	sql.WriteString(consts.Blank)
	sql.WriteString(x.Table.beautify())
	sql.WriteString(consts.NextLine)
	return sql.String()
}

// 构建更新字段
func (x *Update) beautifyFields() string {
	var maxLen int
	for _, field := range x.Fields {
		if l := len(field.Name); maxLen < l {
			maxLen = l
		}
	}
	var sql, first = strings.Builder{}, true
	for _, field := range x.Fields {
		name, value := field.Name, field.Value
		if value != "" {
			if first {
				sql.WriteString(x.align(consts.SET))
			} else {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.NextLine)
				sql.WriteString(x.align())
			}
			first = false
			sql.WriteString(consts.Blank)
			sql.WriteString(name)
			sql.WriteString(strings.Repeat(consts.Blank, maxLen-len(name)+1))
			sql.WriteString(consts.EQ)
			sql.WriteString(consts.Blank)
			sql.WriteString(value)
		}
	}
	return sql.String()
}

func (x *Update) beautifyCondition() string {
	if conditions := x.Where; len(conditions) > 0 {
		sql := strings.Builder{}
		var maxLen int
		for _, condition := range x.Where {
			l := len(condition.Name)
			if maxLen < l {
				maxLen = l
			}
		}
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.WHERE))
		sql.WriteString(consts.Blank)
		for i, condition := range conditions {
			if i > 0 {
				sql.WriteString(consts.NextLine)
				if condition.LogicalOperators == consts.Empty {
					sql.WriteString(x.align(consts.AND))
					sql.WriteString(consts.Blank)
				} else {
					sql.WriteString(x.align(condition.LogicalOperators))
					sql.WriteString(consts.Blank)
				}
			}
			if condition.Name != "" {
				sql.WriteString(condition.Name)
				sql.WriteString(strings.Repeat(consts.Blank, maxLen-len(condition.Name)+1))
				sql.WriteString(condition.ComparisonOperators)
				sql.WriteString(consts.Blank)
			}
			sql.WriteString(condition.Value)
		}
		return sql.String()
	}
	return ""
}

func (x *Update) parseTable() *Update {
	sql := x.temp
	// 去除update关键字
	if strings.HasPrefix(sql, consts.UPDATE) {
		sql = sql[7:]
	}
	// 如果有from则先去除
	if strings.HasPrefix(sql, consts.FROM) {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if first := utils.IndexOfKeywordFirst(sql, consts.SET); first >= 0 {
		x.temp = sql[first:]
		sql = sql[:first]
	}
	var name, alias string
	if index := utils.IndexOfString(sql, consts.Blank, 1); index >= 0 {
		name = sql[:index]
		alias = utils.ExtractAlias(sql[index+1:])
	}
	x.Table = &Table{
		Name:  name,
		Alias: alias,
	}
	return x
}

// 提取字段
func (x *Update) parseFields() *Update {
	sql := x.temp
	// 根据where关键字进行拆分
	if first := utils.IndexOfKeywordFirst(sql, consts.WHERE); first > 0 {
		x.temp = sql[first:]
		sql = sql[:first]
	}
	// 截取where关键字前面的sql片段
	if first := utils.IndexOfKeywordFirst(sql, consts.SET); first >= 0 {
		sql = sql[first+4:]
		list, last := utils.SplitExcludeInBracket(sql, consts.Comma)
		list = append(list, last)
		var fields []*Field
		for _, field := range list {
			var name, value string
			if eqi := utils.IndexOfString(field, consts.EQ); eqi >= 0 {
				name, value = field[:eqi], field[eqi+1:]
			}
			name, value = strings.TrimSpace(name), strings.TrimSpace(value)
			if utils.IndexOfString(name, consts.ReplacePrefix) >= 0 {
				name = x.replacer.Replace(name)
			}
			fields = append(fields, &Field{
				Name:  name,
				Value: value,
			})
		}
		x.Fields = fields
	}
	return x
}

// 提取查询条件
func (x *Update) parseWhere() *Update {
	if sql := x.temp; sql != "" {
		x.Where, x.temp = ExtractWhere(sql)
	}
	return x
}
