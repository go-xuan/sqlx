package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// NewBase 初始化SQL解析器base
func NewBase(sql string, indent ...int) Base {
	var base = Base{
		originSql: sql,
		tempSql:   sql,
		indent:    6,
	}
	if len(indent) > 0 { // 累加缩缩进
		base.indent += indent[0]
	}
	return base
}

// Base SQL解析器base
type Base struct {
	originSql string            // 原始sql，原始完整sql（变量值需要通过 replacer 进行还原）
	tempSql   string            // 临时sql，存储每个步骤经过sql拆解之后的sql片段
	indent    int               // 缩进量
	simple    bool              // 简单sql
	replacer  *strings.Replacer // 变量值替换器，consts.ReplacePrefix + 编号 + consts.ReplaceSuffix
}

// 解析准备
func (b *Base) parsePrepare() {
	sql := b.tempSql
	// 解析sql中所有的参数值，避免参数值值影响后续sql解析
	var replacer *strings.Replacer
	if sql, replacer = utils.ParseValuesInSql(sql); replacer != nil {
		b.replacer = replacer
	}
	// 将sql中所有关键字转为小写
	b.tempSql = utils.AllKeywordsToLower(sql)
}

// 解析完成
func (b *Base) parseFinish() {
	b.tempSql = ""
}

// 以当前缩进量对齐
func (b *Base) align(key ...string) string {
	return Align(b.indent, key...)
}

// Align 根据缩进量对齐
func Align(indent int, key ...string) string {
	if len(key) == 0 {
		return strings.Repeat(consts.Blank, indent)
	} else if str := key[0]; len(str) <= indent {
		return strings.Repeat(consts.Blank, indent-len(str)) + str
	} else if cut, _ := utils.CutString(str, consts.Blank); len(cut) <= indent {
		return strings.Repeat(consts.Blank, indent-len(cut)) + str
	} else {
		return str
	}
}

// ExtractWhere 提取条件
func ExtractWhere(sql string) ([]*Condition, string) {
	if sql != "" {
		if index := utils.IndexOfKeywordFirst(sql, consts.WHERE); index >= 0 {
			// 去除where关键字
			sql = sql[index+5:]
			// 提取where部分sql
			var whereSql string
			if _, end := utils.ContainsKeywords(sql, consts.GROUPBY, consts.ORDERBY, consts.LIMIT); end >= 0 {
				whereSql, sql = sql[:end], sql[end:]
			} else {
				whereSql, sql = sql, consts.Empty
			}
			// 提取Conditions条件
			return NewConditions(whereSql), sql
		}
	}
	return nil, sql
}

// NewConditions 全部条件
func NewConditions(sql string) []*Condition {
	// 去除前后多余括号
	sql = utils.TrimBrackets(sql)
	var conditions []*Condition
	var loop, andOr = true, ""
	for loop {
		if index := utils.IndexExcludeBrackets(sql, consts.AND, true); index > 0 {
			conditions = append(conditions, NewCondition(sql[:index], andOr))
			sql, andOr = sql[index+4:], consts.AND
		} else if index = utils.IndexExcludeBrackets(sql, consts.OR, true); index > 0 {
			conditions = append(conditions, NewCondition(sql[:index], andOr))
			sql, andOr = sql[index+3:], consts.OR
		} else {
			conditions = append(conditions, NewCondition(sql, andOr))
			loop = false
		}
	}
	return conditions
}

// NewCondition 单个条件
func NewCondition(sql string, andOr string) *Condition {
	// 去除前后空格
	sql = strings.TrimSpace(sql)
	var condition = &Condition{AndOr: andOr}
	if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from == 0 && to == len(sql)-1 {
		condition.Conditions = NewConditions(sql[from+1 : to]) // ()括号在前后两端表示是联合子条件
	} else if index := utils.IndexExcludeBrackets(sql, consts.NE, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+2]
		condition.Value = sql[index+3:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.GE, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+2]
		condition.Value = sql[index+3:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.LE, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+2]
		condition.Value = sql[index+3:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.EQ, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+1]
		condition.Value = sql[index+2:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.LT, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+1]
		condition.Value = sql[index+2:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.GT, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+1]
		condition.Value = sql[index+2:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.LIKE, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+4]
		condition.Value = sql[index+5:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.NOTIN, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+6]
		condition.parseIn(sql[index+7:])
	} else if index = utils.IndexExcludeBrackets(sql, consts.IN, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+2]
		condition.parseIn(sql[index+3:])
	} else if index = utils.IndexExcludeBrackets(sql, consts.ISNOT, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+6]
		condition.Value = sql[index+7:]
	} else if index = utils.IndexExcludeBrackets(sql, consts.IS, true); index > 0 {
		condition.Name = sql[:index-1]
		condition.Operator = sql[index : index+2]
		condition.Value = sql[index+3:]
	} else {
		condition.Name = sql
	}
	return condition
}

// Join 关联表解析
type Join struct {
	Table *Table // join表对象
	Type  string // join类型left/right/inner
	On    string // 关联条件
}

// Condition 查询条件解析
type Condition struct {
	AndOr      string       // and/or
	Name       string       // 字段
	Operator   string       // 运算符（=、!=、like、in、not in、is、is not）
	Value      string       // 值
	Values     []string     // in值
	Select     *Select      // 子查询
	Conditions []*Condition // 子条件
}

func (c *Condition) parseIn(sql string) {
	sql = strings.Trim(sql, "() ;")
	if index := utils.IndexOfKeywordFirst(sql, consts.SELECT); index >= 0 {
		indent := len(c.Name) + 12
		c.Select = ParseSelectSQL(sql, indent)
	} else {
		c.Values = strings.Split(sql, consts.Comma)
	}
}

func (c *Condition) beautify(indent int) string {
	var sql = strings.Builder{}
	if c.AndOr != "" {
		// 增加缩进
		sql.WriteString(Align(indent, c.AndOr))
		sql.WriteString(consts.Blank)
	}
	if len(c.Conditions) > 0 { // 联合子条件
		sql.WriteString("(")
		for i, condition := range c.Conditions {
			if i > 0 {
				sql.WriteString(consts.Blank)
			}
			sql.WriteString(condition.beautify(0))
		}
		sql.WriteString(")")
	} else { // 单条件
		indent = indent + len(c.Name) + 6
		sql.WriteString(c.Name)
		sql.WriteString(consts.Blank)
		sql.WriteString(c.Operator)
		sql.WriteString(consts.Blank)
		if c.Operator == consts.IN || c.Operator == consts.NOTIN {
			sql.WriteString(consts.LeftBracket)
			if len(c.Values) > 0 {
				var nextLine = len(c.Values) > 3
				for i, value := range c.Values {
					if i > 0 {
						sql.WriteString(consts.Comma)
						sql.WriteString(consts.Blank)
						if nextLine {
							sql.WriteString(consts.NextLine)
							sql.WriteString(Align(indent))
						}
					}
					sql.WriteString(value)
				}
			} else {
				sql.WriteString(c.Select.Beautify())
			}
			sql.WriteString(consts.RightBracket)
		} else {
			sql.WriteString(c.Value)
		}
	}
	return sql.String()
}

// ExtractTable 提取主表
func ExtractTable(sql string, indent int) (*Table, string) {
	if index := utils.IndexExcludeBrackets(sql, consts.FROM, true); index >= 0 {
		sql = sql[index+4:] // 截取掉from，但是保留表名前面的空格
	} else if sql[:1] != consts.Blank {
		sql = consts.Blank + sql // 没空格则补上空格
	}
	var table = &Table{}
	if sql[1:2] == consts.LeftBracket { // 如果from后面跟括号，表示是子查询
		if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from >= 0 && from < to {
			table.Select = ParseSelectSQL(sql[from+1:to], indent+2)
			sql = sql[to:]
		} else {
			panic("解析sql异常")
		}
	} else { // from后面直接跟表名
		before := utils.IndexOfString(sql, consts.Blank, 1)                 // 表名前空格下标，前面已经做了处理，所以此空格必定存在
		if after := utils.IndexOfString(sql, consts.Blank, 2); after >= 0 { // 表名后空格下标
			table.Name, sql = sql[before+1:after], sql[after+1:]
		} else {
			table.Name, sql = sql[before+1:], ""
		}
	}
	if sql != "" {
		var alias string
		if _, index := utils.ContainsKeywords(sql, consts.LEFT, consts.RIGHT, consts.INNER, consts.OUTER, consts.JOIN, consts.WHERE, consts.GROUPBY, consts.ORDERBY, consts.LIMIT); index >= 0 {
			// 判断是否是复杂查询
			alias, sql = sql[:index], sql[index:]
		} else { // 简单查询
			alias, sql = sql, consts.Empty
		}
		table.Alias = utils.ExtractAlias(alias)
	}
	return table, sql
}

// Table 主表解析
type Table struct {
	Name   string  // 表名
	Alias  string  // 表别名
	Select *Select // 子查询
}

func (p *Table) beautify(withAs ...bool) string {
	sql := strings.Builder{}
	if p.Select != nil {
		sql.WriteString(consts.LeftBracket)
		sql.WriteString(p.Select.Beautify())
		sql.WriteString(consts.RightBracket)
	} else {
		sql.WriteString(p.Name)
	}
	if p.Alias != "" {
		if len(withAs) > 0 && withAs[0] {
			sql.WriteString(consts.Blank)
			sql.WriteString(consts.AS)
		}
		sql.WriteString(consts.Blank)
		sql.WriteString(p.Alias)
	}
	return sql.String()

}

// Field 字段解析
type Field struct {
	Name      string // 字段名
	Alias     string // 字段别名，仅查询使用
	Table     string // 表名
	Value     string // 字段值
	Type      string // 字段类型
	Precision int    // 长度
	Scale     int    // 小数点
	Nullable  bool   // 允许为空
	Default   string // 默认值
	Comment   string // 注释
}
