package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/model"
	"github.com/go-xuan/sqlx/utils"
)

// ParseUpdateSQL 解析更新SQL
func ParseUpdateSQL(sql string, indent ...int) (*Update, error) {
	var parser = &Update{
		SQL: NewSQL(sql, indent...),
	}
	parser.parsePrepare()
	parser.parseTable()
	parser.parseFields()
	if err := parser.parseWhere(); err != nil {
		return nil, err
	}
	parser.parseFinish()

	return parser, nil
}

type Update struct {
	SQL
	Table  *model.Table
	Fields []*model.Field
	Where  []*model.Filter
}

func (x *Update) Beautify() string {
	var builder = strings.Builder{}
	builder.WriteString(x.beautifyUpdate())
	builder.WriteString(x.beautifyFields())
	builder.WriteString(x.beautifyCondition(x.Where))
	sql := builder.String()
	if x.replacer != nil {
		return x.replacer.Replace(sql)
	}
	return sql
}

// ToModel 转换为 model.Update
func (x *Update) ToModel() *model.Update {
	return &model.Update{
		Table:  x.Table,
		Fields: x.Fields,
		Where:  x.Where,
	}
}

func (x *Update) beautifyUpdate() string {
	var sql = strings.Builder{}
	sql.WriteString(consts.UPDATE)
	sql.WriteString(consts.Blank)
	sql.WriteString(tableSQL(x.Table))
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

func (x *Update) parseTable() {
	sql := x.temp
	if strings.HasPrefix(sql, consts.UPDATE) {
		sql = sql[len(consts.UPDATE)+1:]
	}
	if strings.HasPrefix(sql, consts.FROM) {
		sql = sql[len(consts.FROM)+1:]
	}
	if first := utils.IndexOfKeywordFirst(sql, consts.SET); first >= 0 {
		x.temp = sql[first:]
		sql = sql[:first]
	}
	var name, alias string
	if index := utils.IndexOfString(sql, consts.Blank, 1); index >= 0 {
		name = sql[:index]
		alias = utils.ExtractAlias(sql[index+1:])
	}
	x.Table = &model.Table{
		Name:  name,
		Alias: alias,
	}
}

// 提取字段
func (x *Update) parseFields() {
	sql := x.temp
	if first := utils.IndexOfKeywordFirst(sql, consts.WHERE); first > 0 {
		x.temp = sql[first:]
		sql = sql[:first]
	}
	if first := utils.IndexOfKeywordFirst(sql, consts.SET); first >= 0 {
		sql = sql[first+len(consts.SET)+1:]
		list, last := utils.SplitExcludeInBracket(sql, consts.Comma)
		list = append(list, last)
		var fields []*model.Field
		for _, field := range list {
			var name, value string
			if eqi := utils.IndexOfString(field, consts.EQ); eqi >= 0 {
				name, value = field[:eqi], field[eqi+1:]
			}
			name, value = strings.TrimSpace(name), strings.TrimSpace(value)
			if utils.IndexOfString(name, consts.ReplacePrefix) >= 0 {
				name = x.replacer.Replace(name)
			}
			fields = append(fields, &model.Field{
				Name:  name,
				Value: value,
			})
		}
		x.Fields = fields
	}
}

// 提取查询条件
func (x *Update) parseWhere() error {
	if sql := x.temp; sql != "" {
		var err error
		x.Where, x.temp, err = ExtractWhere(sql)
		return err
	}
	return nil
}
