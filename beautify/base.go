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
	originSql string            // 原始sql，原始完整sql（变量值需要通过 replacer 还原）
	tempSql   string            // 临时sql，存储每个步骤经过sql拆解之后的sql片段
	indent    int               // 缩进量
	replacer  *strings.Replacer // 替换器，用于将 originSql 的变量占位符还原成原始值
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
func (b *Base) align(sql ...string) string {
	if len(sql) == 0 {
		return strings.Repeat(consts.Blank, b.indent)
	} else if str := sql[0]; len(str) <= b.indent {
		return strings.Repeat(consts.Blank, b.indent-len(str)) + str
	} else if cut, _ := utils.CutString(str, consts.Blank); len(cut) <= b.indent {
		return strings.Repeat(consts.Blank, b.indent-len(cut)) + str
	} else {
		return str
	}
}

// ExtractWhere 提取条件
func ExtractWhere(sql string) ([]*Condition, string) {
	if sql == "" {
		return nil, ""
	}
	// 去除where
	if index := utils.FirstIndexOfKeyword(sql, consts.WHERE); index >= 0 {
		sql = sql[index+5:]
	}
	var whereSql string
	if _, index := utils.ContainsKeywords(sql, consts.GroupBy, consts.OrderBy, consts.LIMIT); index >= 0 {
		whereSql, sql = sql[:index], sql[index:]
	} else {
		whereSql, sql = sql, consts.Empty
	}
	list, last := utils.SplitExcludeInBracket(whereSql, consts.AND)
	list = append(list, last)
	var conditions []*Condition
	if len(list) > 0 {
		for _, condition := range list {
			conditions = append(conditions, &Condition{
				Content: strings.TrimSpace(condition),
			})
		}
	}
	return conditions, sql
}

// Condition 查询条件解析
type Condition struct {
	LogicalOperator string // 逻辑运算符
	Content         string // 条件
}

// Join 关联表解析
type Join struct {
	Table *Table // join表对象
	Type  string // join类型left/right/inner
	On    string // 关联条件
}

// ExtractTable 提取主表
func ExtractTable(sql string, indent int) (*Table, string) {
	if index := utils.IndexExcludeInBracket(sql, consts.FROM); index >= 0 {
		sql = sql[index+4:] // 截取掉from，但是保留表名前面的空格
	} else if sql[:1] != consts.Blank {
		sql = consts.Blank + sql // 没空格则补上空格
	}
	var table = &Table{}
	if sql[1:2] == consts.LeftBracket { // 如果from后面跟括号，表示是子查询
		if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from < to {
			table.Select = ParseSelectSQL(sql[from:to], indent+2)
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
		if _, index := utils.ContainsKeywords(sql, consts.LEFT, consts.RIGHT, consts.INNER, consts.OUTER, consts.JOIN, consts.WHERE, consts.GroupBy, consts.OrderBy, consts.LIMIT); index >= 0 {
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

func (p *Table) AliasSQL(withAs ...bool) string {
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
