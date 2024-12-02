package sqlx

type CreateParser struct {
	ParserBase                    // 原始sql
	table      *TableParser       // 创建表
	fields     []*FieldParser     // 表字段
	where      []*ConditionParser // 查询条件
}

func (p *CreateParser) Beautify() string {
	return ""
}
