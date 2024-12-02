package sqlx

import "strings"

type InsertParser struct {
	ParserBase
	Table  *TableParser   // 插入表
	Fields []*FieldParser // 插入字段
	Select *SelectParser  // 查询条件
}

func (p *InsertParser) Beautify() string {
	return ""
}

func (p *InsertParser) extractTable() *InsertParser {
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
