package beautify

import (
	"fmt"
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/model"
	"github.com/go-xuan/sqlx/utils"
)

// ParseInsertSQL 解析插入SQL
func ParseInsertSQL(sql string, indent ...int) (*Insert, error) {
	var parser = &Insert{
		SQL: NewSQL(sql, indent...),
	}

	parser.parsePrepare()
	parser.parseTable()
	parser.extractFields()
	if err := parser.extractValues(); err != nil {
		return nil, err
	}
	parser.parseFinish()

	return parser, nil
}

type Insert struct {
	SQL
	Table     *model.Table
	Fields    []*model.Field
	ValueData [][]string
	Query     *Select
}

func (x *Insert) Beautify() string {
	var builder = strings.Builder{}
	builder.WriteString(x.beautifyInsert())
	builder.WriteString(x.beautifyFields())
	builder.WriteString(x.beautifyValues())
	sql := builder.String()
	if x.replacer != nil {
		return x.replacer.Replace(sql)
	}
	return sql
}

// ToModel 转换为 model.Insert
func (x *Insert) ToModel() *model.Insert {
	m := &model.Insert{
		Table:     x.Table,
		Fields:    x.Fields,
		ValueData: x.ValueData,
	}
	if x.Query != nil {
		m.Query = x.Query.ToModel()
	}
	return m
}

func (x *Insert) beautifyInsert() string {
	var sql = strings.Builder{}
	sql.WriteString("insert into ")
	sql.WriteString(tableSQL(x.Table))
	sql.WriteString(consts.NextLine)
	return sql.String()
}

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

func (x *Insert) beautifyValues() string {
	if x.Query != nil {
		return x.Query.Beautify()
	} else if x.ValueData != nil {
		var sql = strings.Builder{}
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
		return sql.String()
	}
	return ""
}

func (x *Insert) parseTable() {
	sql := x.temp
	if first := utils.IndexOfKeywordFirst(sql, consts.INSERT); first == 0 {
		sql = sql[len(consts.INSERT)+1:]
	}
	if first := utils.IndexOfKeywordFirst(sql, consts.INTO); first == 0 {
		sql = sql[len(consts.INTO)+1:]
	}
	if index := utils.IndexOfString(sql, consts.LeftBracket); index >= 0 {
		x.Table = &model.Table{
			Name: strings.TrimSpace(sql[:index-1]),
		}
		x.temp = sql[index:]
	}
}

func (x *Insert) extractFields() {
	sql := x.temp
	if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from >= 0 && from < to {
		x.temp = strings.TrimLeft(sql[to+1:], consts.Blank)
		sql = sql[from+1 : to]
	}
	if names := strings.Split(sql, consts.Comma); len(names) > 0 {
		var fields []*model.Field
		for _, name := range names {
			name = strings.TrimSpace(name)
			fields = append(fields, &model.Field{Name: name})
		}
		x.Fields = fields
	}
}

func (x *Insert) extractValues() error {
	sql := strings.TrimLeft(x.temp, consts.Blank)
	if first := utils.IndexOfKeywordFirst(sql, consts.SELECT); first == 0 {
		query, err := ParseSelectSQL(sql)
		if err != nil {
			return fmt.Errorf("insert子查询解析失败: %w", err)
		}
		if len(query.Fields) != len(x.Fields) {
			return fmt.Errorf("select字段数量和insert字段数量不匹配")
		}
		x.Query = query
		return nil
	}
	if first := utils.IndexOfKeywordFirst(sql, consts.VALUES); first == 0 {
		sql = sql[len(consts.VALUES)+1:]
	}

	if first := utils.IndexOfKeywordFirst(sql, consts.VALUE); first == 0 {
		sql = sql[len(consts.VALUE)+1:]
	}

	valuesList, lastValues := utils.SplitExcludeInBracket(sql, consts.Comma)

	if index := utils.IndexOfString(lastValues, consts.Semicolon, -1); index >= 0 {
		lastValues = lastValues[:index]
	}

	valuesList = append(valuesList, lastValues)
	for _, valuesSql := range valuesList {
		values := utils.SplitValuesSql(valuesSql)
		if len(values) == len(x.Fields) {
			x.ValueData = append(x.ValueData, values)
		} else {
			var names []string
			for i, field := range x.Fields {
				if i < len(values) {
					names = append(names, field.Name+" : "+values[i])
				}
			}
			return fmt.Errorf("insert字段数量和insert值数量不匹配: \n%s", strings.Join(names, "\n"))
		}
	}

	return nil
}
