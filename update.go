package sqlx

import (
	"strings"
)

type UpdateSqlParser struct {
	ParserBase
	Table  *TableParser       // 更新表
	Fields []*FieldParser     // 更新字段
	Where  []*ConditionParser // 查询条件
}

func (p *UpdateSqlParser) Beautify() string {
	var sql = strings.Builder{}
	sql.WriteString(p.buildUpdateSql())
	sql.WriteString(p.buildFieldsSql())
	sql.WriteString(p.buildConditionSql())
	if replacer := p.replacer; replacer != nil {
		return replacer.Replace(sql.String())
	} else {
		return sql.String()
	}
}

// 构建查询字段sql
func (p *UpdateSqlParser) buildUpdateSql() string {
	var sql = strings.Builder{}
	sql.WriteString(UPDATE)
	sql.WriteString(Blank)
	sql.WriteString(p.Table.AliasSQL())
	sql.WriteString(NewLine)
	return sql.String()
}

// 构建查询字段sql
func (p *UpdateSqlParser) buildFieldsSql() string {
	var sql = strings.Builder{}
	var maxLen int
	for _, field := range p.Fields {
		l := len(field.Name)
		if maxLen < l {
			maxLen = l
		}
	}
	for i, field := range p.Fields {
		if i == 0 {
			sql.WriteString(p.align(SET))
		} else {
			sql.WriteString(Comma)
			sql.WriteString(NewLine)
			sql.WriteString(p.align())
		}
		sql.WriteString(Blank)
		sql.WriteString(field.Name)
		sql.WriteString(strings.Repeat(Blank, maxLen-len(field.Name)+1))
		sql.WriteString(Equals)
		sql.WriteString(Blank)
		sql.WriteString(field.Value)
	}
	return sql.String()
}

func (p *UpdateSqlParser) buildConditionSql() string {
	sql := strings.Builder{}
	if conditions := p.Where; len(conditions) > 0 {
		sql.WriteString(NewLine)
		sql.WriteString(p.align(WHERE))
		sql.WriteString(Blank)
		for i, cond := range conditions {
			if i > 0 {
				sql.WriteString(NewLine)
				if cond.LogicalOperator == Empty {
					sql.WriteString(p.align(AND))
					sql.WriteString(Blank)
				} else {
					sql.WriteString(p.align(cond.LogicalOperator))
					sql.WriteString(Blank)
				}
			}
			sql.WriteString(cond.Content)
		}
	}
	return sql.String()
}

func (p *UpdateSqlParser) parseTable() *UpdateSqlParser {
	sql := p.tempSql
	// 去除update关键字
	if strings.HasPrefix(sql, UPDATE) {
		sql = sql[7:]
	}
	// 如果有from则先去除
	if strings.HasPrefix(sql, FROM) {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if index := firstIndexOfKeyword(sql, SET); index >= 0 {
		p.tempSql = sql[index:]
		sql = sql[:index]
	}
	var name, alias string
	if index := indexOfString(sql, Blank, 1); index >= 0 {
		name = sql[:index]
		alias = extractAlias(sql[index+1:])
	}
	p.Table = &TableParser{
		Name:  name,
		Alias: alias,
	}
	return p
}

// 提取字段
func (p *UpdateSqlParser) parseFields() *UpdateSqlParser {
	sql := p.tempSql
	// 根据where关键字进行拆分
	if index := firstIndexOfKeyword(sql, WHERE); index > 0 {
		p.tempSql = sql[index:]
		sql = sql[:index]
	}
	// 截取where关键字前面的sql片段
	if index := firstIndexOfKeyword(sql, SET); index >= 0 {
		sql = sql[index+4:]
		var fieldList, lastField = splitExcludeInBracket(sql, Comma)
		fieldList = append(fieldList, lastField)
		var fields []*FieldParser
		for _, field := range fieldList {
			var name, value string
			if eqi := indexOfString(field, Equals); eqi >= 0 {
				name, value = field[:eqi], field[eqi+1:]
			}
			name, value = strings.TrimSpace(name), strings.TrimSpace(value)
			if indexOfString(name, ReplacePrefix) >= 0 {
				name = p.replacer.Replace(name)
			}
			fields = append(fields, &FieldParser{Name: name, Value: value})
		}
		p.Fields = fields
	}
	return p
}

// 提取查询条件
func (p *UpdateSqlParser) parseWhere() *UpdateSqlParser {
	p.Where, p.tempSql = extractWhere(p.tempSql)
	return p
}
