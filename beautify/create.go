package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// ParseCreateSQL 解析建表SQL
func ParseCreateSQL(sql string, indent ...int) *Create {
	// sql初始化
	var parser = &Create{
		Base: NewBase(sql, indent...),
	}

	// sql解析
	parser.parsePrepare() // 解析准备
	parser.parseTable()   // 解析主表
	parser.parseFinish()  // 解析完成

	return parser
}

type Create struct {
	Base                // 原始sql
	Table  *Table       // 创建表
	Fields []*Field     // 表字段
	Where  []*Condition // 查询条件
}

func (x *Create) Beautify() string {
	return ""
}

func (x *Create) parseTable() *Create {
	sql := x.tempSql
	// 去除update关键字
	if strings.HasPrefix(sql, consts.INSERT) {
		sql = sql[7:]
	}
	// 如果有from则先去除
	if strings.HasPrefix(sql, consts.FROM) {
		sql = sql[5:]
	}
	// 根据set关键字进行拆分
	if index := utils.FirstIndexOfKeyword(sql, consts.WHERE); index >= 0 {
		x.tempSql = sql[index:]
		sql = sql[:index]
	}
	var name, alias string
	if index := utils.IndexOfString(sql, consts.Blank, 1); index >= 0 {
		name = sql[:index]
		alias = utils.ExtractAlias(sql[index+1:])
	}
	x.Table = &Table{
		Name:  name,
		Alias: alias,
	}
	return x
}
