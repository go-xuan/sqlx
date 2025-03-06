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
		Base: NewBase(sql, indent...),
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
	Base
	Table  *Table       // 更新表
	Fields []*Field     // 更新字段
	Where  []*Condition // 查询条件
}

func (x *Update) Beautify() string {
	var sql = strings.Builder{}
	sql.WriteString(x.beautifyUpdate())
	sql.WriteString(x.beautifyFields())
	sql.WriteString(x.beautifyCondition())
	if replacer := x.replacer; replacer != nil {
		return replacer.Replace(sql.String())
	} else {
		return sql.String()
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
	var sql = strings.Builder{}
	var maxLen int
	for _, field := range x.Fields {
		l := len(field.Name)
		if maxLen < l {
			maxLen = l
		}
	}
	var i int
	for _, field := range x.Fields {
		if value := field.Value; value != "" {
			if i == 0 {
				sql.WriteString(x.align(consts.SET))
			} else {
				sql.WriteString(consts.Comma)
				sql.WriteString(consts.NextLine)
				sql.WriteString(x.align())
			}
			sql.WriteString(consts.Blank)
			sql.WriteString(field.Name)
			sql.WriteString(strings.Repeat(consts.Blank, maxLen-len(field.Name)+1))
			sql.WriteString(consts.EQ)
			sql.WriteString(consts.Blank)
			sql.WriteString(field.Value)
			i++
		}
	}
	return sql.String()
}

func (x *Update) beautifyCondition() string {
	sql := strings.Builder{}
	if conditions := x.Where; len(conditions) > 0 {
		sql.WriteString(consts.NextLine)
		sql.WriteString(x.align(consts.WHERE))
		sql.WriteString(consts.Blank)
		for i, cond := range conditions {
			if i > 0 {
				sql.WriteString(consts.NextLine)
				if cond.AndOr == consts.Empty {
					sql.WriteString(x.align(consts.AND))
					sql.WriteString(consts.Blank)
				} else {
					sql.WriteString(x.align(cond.AndOr))
					sql.WriteString(consts.Blank)
				}
			}
			sql.WriteString(cond.Value)
		}
	}
	return sql.String()
}

func (x *Update) parseTable() *Update {
	sql := x.tempSql
	// 去除update关键字
	if strings.HasPrefix(sql, consts.UPDATE) {
		sql = sql[7:]
	}
	// 如果有from则先去除
	if strings.HasPrefix(sql, consts.FROM) {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if index := utils.IndexOfKeywordFirst(sql, consts.SET); index >= 0 {
		x.tempSql = sql[index:]
		sql = sql[:index]
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
	sql := x.tempSql
	// 根据where关键字进行拆分
	if index := utils.IndexOfKeywordFirst(sql, consts.WHERE); index > 0 {
		x.tempSql = sql[index:]
		sql = sql[:index]
	}
	// 截取where关键字前面的sql片段
	if index := utils.IndexOfKeywordFirst(sql, consts.SET); index >= 0 {
		sql = sql[index+4:]
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
			fields = append(fields, &Field{Name: name, Value: value})
		}
		x.Fields = fields
	}
	return x
}

// 提取查询条件
func (x *Update) parseWhere() *Update {
	if sql := x.tempSql; sql != "" {
		x.Where, x.tempSql = ExtractWhere(sql)
	}
	return x
}
