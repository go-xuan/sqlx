package beautify

import (
	"fmt"
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/utils"
)

// IParser SQL解析器
type IParser interface {
	Beautify() string
}

func Parse(sql string) (IParser, error) {
	// 折叠sql
	sql = utils.CollapseSql(sql)
	if len(sql) < 6 {
		return nil, fmt.Errorf("当前输入sql无法解析: %s", sql)
	}
	switch t := strings.ToLower(sql[:6]); t {
	case consts.SELECT:
		return ParseSelectSQL(sql)
	case consts.UPDATE:
		return ParseUpdateSQL(sql)
	case consts.DELETE:
		return ParseDeleteSQL(sql)
	case consts.INSERT:
		return ParseInsertSQL(sql)
	default:
		return nil, fmt.Errorf("当前输入sql无法解析: %s", sql)
	}
}
