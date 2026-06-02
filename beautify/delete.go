package beautify

import (
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/model"
	"github.com/go-xuan/sqlx/utils"
)

// ParseDeleteSQL 解析删除SQL
func ParseDeleteSQL(sql string, indent ...int) (*Delete, error) {
	var parser = &Delete{
		SQL: NewSQL(sql, indent...),
	}

	parser.parsePrepare()
	parser.parseTable()
	if err := parser.parseWhere(); err != nil {
		return nil, err
	}
	parser.parseFinish()

	return parser, nil
}

type Delete struct {
	SQL
	Table *model.Table
	Where []*model.Filter
}

func (x *Delete) Beautify() string {
	var builder = strings.Builder{}
	builder.WriteString(x.beautifyDelete())
	builder.WriteString(x.beautifyCondition(x.Where))
	sql := builder.String()
	if x.replacer != nil {
		return x.replacer.Replace(sql)
	}
	return sql
}

// ToModel 转换为 model.Delete
func (x *Delete) ToModel() *model.Delete {
	return &model.Delete{
		Table: x.Table,
		Where: x.Where,
	}
}

func (x *Delete) beautifyDelete() string {
	var sql = strings.Builder{}
	sql.WriteString(consts.DELETE)
	sql.WriteString(consts.Blank)
	sql.WriteString(consts.FROM)
	sql.WriteString(consts.Blank)
	sql.WriteString(tableSQL(x.Table))
	return sql.String()
}

func (x *Delete) parseTable() {
	sql := x.temp
	if strings.HasPrefix(sql, consts.DELETE) {
		sql = sql[len(consts.DELETE)+1:]
	}
	if strings.HasPrefix(sql, consts.FROM) {
		sql = sql[len(consts.FROM)+1:]
	}
	if first := utils.IndexOfKeywordFirst(sql, consts.WHERE); first >= 0 {
		x.temp = sql[first:]
		sql = sql[:first]
	}
	var name, alias string
	if index := utils.IndexOfString(sql, consts.Blank, 1); index >= 0 {
		name = sql[:index]
		alias = utils.ExtractAlias(sql[index+1:])
	}
	x.Table = &model.Table{
		Name:  name,
		Alias: alias,
	}
}

// 提取查询条件
func (x *Delete) parseWhere() error {
	if sql := x.temp; sql != "" {
		var err error
		x.Where, x.temp, err = ExtractWhere(sql)
		return err
	}
	return nil
}
