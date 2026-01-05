package convert

// Convert 转换器
type Convert interface {
	Conv2SQL() string
	Conv2Mongo() string
}

// QueryConvert 查询转换器
type QueryConvert struct {
	Table   *Table
	Fields  []*Field
	Filters []*Filter
	Orders  []*Order
	Page    *Page
}

// Table 表
type Table struct {
	Name  string
	Alias string
}

// Field 字段
type Field struct {
	Name  string
	Type  string
	Alias string
}

// Filter 过滤条件
type Filter struct {
	Field *Field
	Value any
}

type Order struct {
	Field *Field
	Desc  bool
}

type Page struct {
	Limit  int
	Offset int
}
