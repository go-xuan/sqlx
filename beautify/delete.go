package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// ParseDeleteSQL 解析删除SQL
func ParseDeleteSQL(sql string, indent ...int) *Delete {
	// sql初始化
	var parser = &Delete{
		SQL: NewSQL(sql, indent...),
	}

	// sql解析
	parser.parsePrepare() // 解析准备
	parser.parseTable()   // 解析主表
	parser.parseWhere()   // 解析查询条件
	parser.parseFinish()  // 解析完成

	return parser
}

type Delete struct {
	SQL
	Table *Table       // 删除表
	Where []*Condition // 查询条件
}

func (x *Delete) Beautify() string {
	return ""
}

func (x *Delete) parseTable() *Delete {
	sql := x.temp
	// 去除update关键字
	if strings.HasPrefix(sql, consts.DELETE) {
		sql = sql[7:]
	}
	// 去除from关键字
	if strings.HasPrefix(sql, consts.FROM) {
		sql = sql[5:]
	}
	// 根据where关键字进行拆分
	if first := utils.IndexOfKeywordFirst(sql, consts.WHERE); first >= 0 {
		x.temp = sql[first:]
		sql = sql[:first]
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

// 提取查询条件
func (x *Delete) parseWhere() *Delete {
	if sql := x.temp; sql != "" {
		x.Where, x.temp = ExtractWhere(sql)
	}
	return x
}
