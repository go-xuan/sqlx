package sqlx

import "strings"

type DeleteParser struct {
	ParserBase
	Table *TableParser       // 删除表
	Where []*ConditionParser // 查询条件
}

func (p *DeleteParser) Beautify() string {
	return ""
}

func (p *DeleteParser) extractTable() *DeleteParser {
	sql := p.tempSql
	// 去除update关键字
	if strings.HasPrefix(sql, DELETE) {
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

// 提取查询条件
func (p *DeleteParser) extractWhere() *DeleteParser {
	p.Where, p.tempSql = extractWhere(p.tempSql)
	return p
}
