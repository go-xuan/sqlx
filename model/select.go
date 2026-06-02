package model

// Select 查询语句解析结果
type Select struct {
	Table    *Table
	Fields   []*Field
	Joins    []*Join
	Where    []*Filter
	GroupBy  []string
	Having   []*Filter
	OrderBy  []string
	Limit    string
	Distinct bool
	SQL      string // 美化后的 SQL（由 beautify 填充）
}

// Insert 插入语句解析结果
type Insert struct {
	Table     *Table
	Fields    []*Field
	ValueData [][]string
	Query     *Select // INSERT ... SELECT
}

// Update 更新语句解析结果
type Update struct {
	Table  *Table
	Fields []*Field
	Where  []*Filter
}

// Delete 删除语句解析结果
type Delete struct {
	Table *Table
	Where []*Filter
}
