package sqlx

import "strings"

type CreateParser struct {
	ParserBase                    // 原始sql
	Table      *TableParser       // 创建表
	Fields     []*FieldParser     // 表字段
	Where      []*ConditionParser // 查询条件
}

func (p *CreateParser) Beautify() string {
	return ""
}

func (p *CreateParser) parseTable() *CreateParser {
	sql := p.tempSql
	// 去除update关键字
	if strings.HasPrefix(sql, INSERT) {
		sql = sql[7:]
	}
	// 如果有from则先去除
	if strings.HasPrefix(sql, FROM) {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if index := keywordIndexOfSql(sql, WHERE); index >= 0 {
		p.tempSql = sql[index:]
		sql = sql[:index]
	}
	var name, alias string
	if index := indexOfSql(sql, Blank, 1); index >= 0 {
		name = sql[:index]
		alias = extractAlias(sql[index+1:])
	}
	p.Table = &TableParser{
		Name:  name,
		Alias: alias,
	}
	return p
}
