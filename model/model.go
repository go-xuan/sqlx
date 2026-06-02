// Package model SQL 解析与转换的中间结构体
package model

// OP 比较运算符
type OP string

const (
	EQ    OP = "="
	NE    OP = "!="
	LT    OP = "<"
	GT    OP = ">"
	LE    OP = "<="
	GE    OP = ">="
	LIKE  OP = "like"
	IN    OP = "in"
	NIN   OP = "not in"
	IS    OP = "is"
	ISNOT OP = "is not"
)

// LogicOp 逻辑运算符
type LogicOp string

const (
	AND LogicOp = "and"
	OR  LogicOp = "or"
)

// Table 表
type Table struct {
	Name   string  // 表名
	Alias  string  // 表别名
	Select *Select // 子查询（FROM 子查询）
	SubSQL string  // 子查询美化 SQL（由 beautify 填充）
}

// Field 字段
type Field struct {
	Name  string // 字段名
	Type  string // 字段类型
	Alias string // 字段别名
	Value string // 字段值（UPDATE SET）
}

// Filter 过滤条件
type Filter struct {
	Field    *Field    // 过滤字段
	Op       OP        // 比较运算符
	Value    any       // 值
	Values   []any     // IN / NOT IN 值列表
	LogicOp  LogicOp   // 与前一个条件的逻辑关系
	Children []*Filter // 嵌套子条件
	Select   *Select   // 子查询（IN 子查询）
	SubSQL   string    // 子查询美化 SQL（由 beautify 填充）
}

// Join 关联表
type Join struct {
	Table *Table // 关联表
	Type  string // left / right / inner / outer
	On    string // 关联条件
}

// Order 排序
type Order struct {
	Field *Field
	Desc  bool
}

// Page 分页
type Page struct {
	Limit  int
	Offset int
}
