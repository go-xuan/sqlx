package beautify

import (
	"regexp"
	"strings"

	"github.com/go-xuan/sqlx/consts"
)

func Parse(sql string) IParser {
	sql = strings.ReplaceAll(sql, consts.NextLine, consts.Blank)        // 移除换行
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, consts.Blank) // 去除多余空格
	sql = strings.TrimSpace(sql)                                        // 去除空格
	switch t := strings.ToLower(sql[:6]); t {                           // 根据sql查询语句开头关键字判断sql类型
	case consts.SELECT:
		return ParseSelectSQL(sql)
	case consts.UPDATE:
		return ParseUpdateSQL(sql)
	case consts.DELETE:
		return ParseDeleteSQL(sql)
	case consts.INSERT:
		return ParseInsertSQL(sql)
	case consts.CREATE:
		return ParseCreateSQL(sql)
	default:
		panic("不支持当前SQL：" + t)
	}
}

// IParser SQL解析器
type IParser interface {
	Beautify() string
}
