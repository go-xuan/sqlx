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

func (p *InsertParser) parseTable() *InsertParser {
	sql := p.tempSql
	// 去除insert关键字
	if strings.HasPrefix(sql, INSERT) {
		sql = sql[7:]
	}
	// 去除into关键字
	if strings.HasPrefix(sql, INTO) {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if index := indexOfSql(sql, LeftBracket); index >= 0 {
		p.Table = &TableParser{
			Name: strings.TrimSpace(sql[:index-1]),
		}
		p.tempSql = sql[index:]
	}
	return p
}

func (p *InsertParser) extractFields() *InsertParser {
	sql := p.tempSql
	// 根据set关键字进行拆分
	if start, end := betweenOfSql(sql, LeftBracket, RightBracket); start >= 0 && end >= start {
		sql = sql[start+1 : end-1]
		p.tempSql = sql[end+1:]
	}
	if names := strings.Split(sql, Comma); len(names) > 0 {
		var fields []*FieldParser
		for _, name := range names {
			fields = append(fields, &FieldParser{Name: name})
		}
		p.Fields = fields
	}
	return p
}
