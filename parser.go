package sqlx

import (
	"regexp"
	"strings"
)

// Parser SQL解析器
type Parser interface {
	Beautify() string
}

// ParserBase SQL解析器base
type ParserBase struct {
	originSql string            // 原始sql
	tempSql   string            // 临时sql
	indent    int               // 缩进量
	replacer  *strings.Replacer // 替换器
}

// 初始化SQL解析器base
func newParserBase(sql string, indent ...int) ParserBase {
	var base = ParserBase{
		originSql: sql,
		tempSql:   sql,
		indent:    6,
	}
	if len(indent) > 0 {
		base.indent += indent[0]
	}
	return base
}

// 完成解析
func (p *ParserBase) prepare() {
	sql := p.tempSql
	// 提取sql中所有的参数值，避免参数值值影响后续sql解析
	var replacer *strings.Replacer
	if sql, replacer = parseValuesInSql(sql); replacer != nil {
		p.replacer = replacer
	}
	// 将sql中所有关键字转为小写
	p.tempSql = allKeywordsToLower(sql)
}

// 完成解析
func (p *ParserBase) finish() {
	p.tempSql = ""
}

// 以当前缩进量对齐
func (p *ParserBase) align(sql ...string) string {
	if len(sql) == 0 {
		return strings.Repeat(Blank, p.indent)
	} else if str := sql[0]; len(str) <= p.indent {
		return strings.Repeat(Blank, p.indent-len(str)) + str
	} else if cut, _ := cutSql(str, Blank); len(cut) <= p.indent {
		return strings.Repeat(Blank, p.indent-len(cut)) + str
	} else {
		return str
	}
}

func Parse(sql string) Parser {
	sql = strings.ReplaceAll(sql, NewLine, Blank)                // 移除换行
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, Blank) // 去除多余空格
	sql = strings.TrimSpace(sql)                                 // 去除空格

	sqlType := strings.ToLower(sql[:6]) // 根据sql查询语句开头关键字判断sql类型
	switch sqlType {
	case SELECT:
		return parseSelectSQL(sql)
	case UPDATE:
		return parseUpdateSQL(sql)
	case DELETE:
		return parseDeleteSQL(sql)
	case INSERT:
		return parseInsertSQL(sql)
	case CREATE:
		return parseCreateSQL(sql)
	default:
		panic("")
	}
}

// 解析sql字符串
func parseSelectSQL(sql string, indent ...int) *SelectParser {
	// sql初始化
	var parser = &SelectParser{
		ParserBase: newParserBase(sql, indent...),
	}

	parser.prepare()        // 解析准备
	parser.extractLimit()   // 提取limit
	parser.extractOrderBy() // 提取order by
	parser.extractFields()  // 提取查询字段
	parser.extractTable()   // 提取主表
	parser.extractJoins()   // 提取关联子表
	parser.extractWhere()   // 提取where
	parser.extractGroupBy() // 提取group By
	parser.extractHaving()  // 提取having
	parser.finish()         // 解析完成

	return parser
}

func parseUpdateSQL(sql string, indent ...int) *UpdateSqlParser {
	// sql初始化
	var parser = &UpdateSqlParser{
		ParserBase: newParserBase(sql, indent...),
	}

	parser.prepare()       // 解析准备
	parser.extractTable()  // 提取更新主表
	parser.extractFields() // 提取更新字段
	parser.extractWhere()  // 提取查询条件
	parser.finish()        // 解析完成

	return parser
}

func parseDeleteSQL(sql string, indent ...int) *DeleteParser {
	// sql初始化
	var parser = &DeleteParser{
		ParserBase: newParserBase(sql, indent...),
	}

	parser.prepare()      // 解析准备
	parser.extractTable() // 提取更新主表
	parser.extractWhere() // 提取查询条件

	parser.finish() // 解析完成

	return parser
}

func parseInsertSQL(sql string, indent ...int) *InsertParser {
	// sql初始化
	var parser = &InsertParser{
		ParserBase: newParserBase(sql, indent...),
	}

	parser.prepare() // 解析准备

	parser.finish() // 解析完成
	return parser
}

func parseCreateSQL(sql string, indent ...int) *CreateParser {
	// sql初始化
	var parser = &CreateParser{
		ParserBase: newParserBase(sql, indent...),
	}

	parser.prepare() // 解析准备

	parser.finish() // 解析完成
	return parser
}
