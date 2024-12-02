package sqlx

import (
	"regexp"
	"strconv"
	"strings"
)

// 解析表
func extractTable(sql string, indent int) (*TableParser, string) {
	if index := indexIgnoreInBracket(sql, FROM); index >= 0 {
		sql = sql[index+4:] // 截取掉from，但是保留表名前面的空格
	} else if sql[:1] != Blank {
		sql = Blank + sql // 没空格则补上空格
	}
	var table = &TableParser{}
	var start, end int
	if sql[1:2] == LeftBracket { // 如果from后面跟括号，表示是子查询
		start, end = betweenOfSql(sql, LeftBracket, RightBracket) // 截取最近的子查询，根据一个对括号内容进行截取
		table.Select = parseSelectSQL(sql[start:end], indent+2)
		sql = sql[end:]
	} else { // from后面直接跟表名
		start = indexOfSql(sql, Blank, 1) // 表名前空格下标
		end = indexOfSql(sql, Blank, 2)   // 表名后空格下标
		if end >= 0 {
			table.Name, sql = sql[start+1:end], sql[end+1:] // 如果表名后面还跟有其他sql，则截取掉表名，继续处理
		} else {
			table.Name, sql = sql[start+1:], ""
		}
	}
	if sql != "" {
		var alias string
		if _, index := containsKeywords(sql, LEFT, RIGHT, INNER, OUTER, JOIN, WHERE, GroupBy, OrderBy, LIMIT); index >= 0 {
			// 判断是否是复杂查询
			alias, sql = sql[:index], sql[index:]
		} else { // 简单查询
			alias, sql = sql, Empty
		}
		table.Alias = extractAlias(alias)
	}
	return table, sql
}

// 提取别名
func extractAlias(sql string) string {
	sql = strings.TrimSpace(sql)
	if index := keywordIndexOfSql(sql, AS); index >= 0 {
		return sql[index+3:]
	} else if index = indexOfSql(sql, Blank); index >= 0 {
		return sql[index+1:]
	} else {
		return sql
	}
}

// 提取条件
func extractWhere(sql string) ([]*ConditionParser, string) {
	// 去除where
	if index := keywordIndexOfSql(sql, WHERE); index >= 0 {
		sql = sql[index+5:]
	}
	var whereSql string
	if _, index := containsKeywords(sql, GroupBy, OrderBy, LIMIT); index >= 0 {
		whereSql, sql = sql[:index], sql[index:]
	} else {
		whereSql, sql = sql, Empty
	}
	var sqlList, lastSql = splitIgnoreInBracket(whereSql, AND)
	sqlList = append(sqlList, lastSql)
	var conditions []*ConditionParser
	if len(sqlList) > 0 {
		for _, conditionSql := range sqlList {
			conditions = append(conditions, &ConditionParser{Content: strings.TrimSpace(conditionSql)})
		}
	}
	return conditions, sql
}

// 解析sql中的变量值
func parseValuesInSql(sql string) (string, *strings.Replacer) {
	values := regexp.MustCompile(`'[^']*'`).FindAllString(sql, -1)
	if len(values) > 0 {
		var all []string
		for i, value := range values {
			var key = ReplacePrefix + strconv.Itoa(i+1)
			sql = strings.Replace(sql, value, key, 1)
			all = append(all, key, value)
		}
		replacer := strings.NewReplacer(all...)
		return sql, replacer
	}
	return sql, nil
}

// 将所有关键字转为小写
func allKeywordsToLower(sql string) string {
	var oldnew []string
	var KEYWORDS = []string{
		SELECT, UPDATE, CREATE, DELETE, INSERT, INTO, FROM,
		WHERE, SET, JOIN, GROUP, ORDER, HAVING, LIMIT, OFFSET,
		ASC, DESC, CASE, WHEN, THEN, END, INNER, OUTER, LEFT, RIGHT,
		DISTINCT, PARTITION, OVER, AS, AND, ON, OR, IN, NOT, LIKE, By,
	}
	for _, keyword := range KEYWORDS {
		switch keyword {
		case ASC, DESC:
			keyword = " " + keyword
		default:
			keyword = keyword + " "
		}
		oldnew = append(oldnew, strings.ToUpper(keyword), keyword)
	}
	sql = strings.NewReplacer(oldnew...).Replace(sql)
	return sql
}

// splitIgnoreInBracket 根据分隔符进行拆分但是忽略括号内的分隔符
func splitIgnoreInBracket(sql, sep string) ([]string, string) {
	var slice []string
	// l：总长度  k:sep长度  m:上个拆分点  n:括号个数
	var l, k, m, n = len(sql), len(sep), 0, 0
	for i := 0; i < l-k; i++ {
		if sql[i] == sep[0] && sql[i:i+k] == sep {
			if n == 0 {
				// 当前位置已将前面的括号对全部消完，才是有效的分割位
				slice = append(slice, sql[m:i])
				m = i + k // 将当前拆分点后移一个sep长度
			}
		} else if sql[i:i+1] == LeftBracket {
			n++ // 遍历到左括号则加一
		} else if sql[i:i+1] == RightBracket && n > 0 {
			n-- // 遍历到右括号则消掉
		}
	}
	return slice, sql[m:]
}

// indexIgnoreInBracket 获取关键字下标但是忽略括号内的关键字
func indexIgnoreInBracket(sql, key string) int {
	// l：总长度  k:关键字长度   n:括号个数
	var l, k, n = len(sql), len(key), 0
	for i := 0; i < l-k; i++ {
		if sql[i] == key[0] && sql[i:i+k] == key {
			if n == 0 {
				// 当前位置已将前面的括号对全部消完，才是有效的关键字
				return i
			}
		} else if sql[i:i+1] == LeftBracket {
			n++ // 遍历到左括号则加一
		} else if sql[i:i+1] == RightBracket && n > 0 {
			n-- // 遍历到右括号则消掉
		}
	}
	return -1
}

// containsKeywords 字符串是否包含sql关键字
func containsKeywords(sql string, keys ...string) (string, int) {
	var hit, index = "", -1
	for _, key := range keys {
		if i := keywordIndexOfSql(sql, key); i >= 0 {
			if i < index {
				hit, index = key, i
			} else if index == -1 {
				hit, index = key, i
			}
		}
	}
	return hit, index
}

// lastIndexOfKeywords 字符串是否包含sql关键字
func lastIndexOfKeywords(sql string, keys ...string) int {
	for _, key := range keys {
		if i := keywordIndexOfSql(sql, key, -1); i >= 0 {
			return i
		}
	}
	return -1
}

func keywordIndexOfSql(sql, key string, positions ...int) int {
	// 获取所有命中字符下标
	if allIndices := allIndexOfSql(sql, key); len(allIndices) >= 0 {
		sl, kl := len(sql), len(key)
		// 筛选出有效下标
		var validIndices []int
		for _, index := range allIndices {
			if index == 0 {
				if sql[index+kl:index+kl+1] == Blank {
					validIndices = append(validIndices, index)
				}
			} else if index == sl-kl {
				if sql[index-1:index] == Blank {
					validIndices = append(validIndices, index)
				}
			} else if sql[index-1:index] == Blank && sql[index+kl:index+kl+1] == Blank {
				validIndices = append(validIndices, index)
			}
		}
		if vl := len(validIndices); vl > 0 {
			if len(positions) > 0 {
				if position := positions[0]; position > 0 && position <= vl { // 正序
					return validIndices[position-1]
				} else if position < 0 && vl+position >= 0 { // 倒序
					return validIndices[vl+position]
				} else {
					return -1
				}
			} else {
				return validIndices[0]
			}
		}
	}
	return -1
}

// allIndexOfSql 获取所有下标, x：命中数量
func allIndexOfSql(sql, sep string) []int {
	var indices []int
	l, m, n := len(sql), len(sep), 0
	for i := 0; i <= l-m; i++ {
		if sql[i] == sep[0] && sql[i:i+m] == sep {
			indices = append(indices, i)
			n++
			i = i + m - 1
		}
	}
	return indices
}

// betweenOfSql 获取起始字符首次出现和结尾字符末次出现的下标
func betweenOfSql(sql, start, end string) (from, to int) {
	from, to = -1, -1
	if start == end {
		if indices := allIndexOfSql(sql, start); len(indices) > 1 {
			from, to = indices[0], indices[1]
		} else if len(indices) == 1 {
			from = indices[0]
		}
		return
	}
	var l, m, n = len(sql), len(start), len(end)
	if m > l || n > l {
		return
	}
	// x:start个数  y:end个数
	var x, y int
	for i := 0; i < l; i++ {
		if sql[i] == start[0] {
			if sql[i:i+m] == start {
				x++
				if x == 1 {
					from = i + m
				}
				i = i + m - 1
			}
		}
		if sql[i] == end[0] {
			if sql[i:i+n] == end {
				y++
				if y == x || x == 1 {
					to = i
					break
				}
				i = i + n - 1
			}
		}
	}
	if to == -1 {
		from = -1
	}
	return
}

// indexOfSql 获取命中子串的首位字符的下标
// position：表示获取位置，默认position=1即正序第1处，position=-1即倒序第1处
func indexOfSql(sql, sep string, position ...int) int {
	if l, m := len(sql), len(sep); l >= m {
		var x, y = 1, 0 // x：目标获取位置，y：sep出现次数计数
		if len(position) > 0 {
			x = position[0]
		}
		for i := 0; i <= l-m; i++ {
			if x > 0 {
				if sql[i] == sep[0] && sql[i:i+m] == sep {
					y++
					if x == y {
						return i
					}
				}
			} else {
				j := l - i
				if sql[j-1] == sep[m-1] && sql[j-m:j] == sep {
					y--
					if x == y {
						return j - m
					}
				}
			}
		}
	}
	return -1
}

// cutSql 分割字符串（reverse=true从右往左）
// position：表示分割位置，默认position=1即正序第1处，position=-1即倒序第1处
func cutSql(sql, sep string, position ...int) (string, string) {
	if i := indexOfSql(sql, sep, position...); i >= 0 {
		return sql[:i], sql[i+len(sep):]
	}
	return sql, ""
}

// DB2GoType DB-Go类型映射
func DB2GoType(t string) string {
	switch t {
	case Char, Varchar, Varchar100, Text, Uuid:
		return String
	case Int, Int2, Int4, Tinyint, Smallint, Mediumint:
		return Int
	case Int8, Bigint:
		return Int64
	case Float, Float4, Numeric:
		return Float64
	case Timestamp, Timestampz, Datetime, Time, Date:
		return TimeTime
	case Bool:
		return Bool
	default:
		return String
	}
}
