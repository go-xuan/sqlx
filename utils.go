package sqlx

import (
	"regexp"
	"strconv"
	"strings"
)

type SqlUtils struct {
	sql string
}

// 解析表
func extractTable(sql string, indent int) (*TableParser, string) {
	if index := indexExcludeInBracket(sql, FROM); index >= 0 {
		sql = sql[index+4:] // 截取掉from，但是保留表名前面的空格
	} else if sql[:1] != Blank {
		sql = Blank + sql // 没空格则补上空格
	}
	var table = &TableParser{}
	var start, end int
	if sql[1:2] == LeftBracket { // 如果from后面跟括号，表示是子查询
		start, end = betweenOfString(sql, LeftBracket, RightBracket) // 截取最近的子查询，根据一个对括号内容进行截取
		table.Select = parseSelectSQL(sql[start:end], indent+2)
		sql = sql[end:]
	} else { // from后面直接跟表名
		start = indexOfString(sql, Blank, 1) // 表名前空格下标
		end = indexOfString(sql, Blank, 2)   // 表名后空格下标
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
	if index := firstIndexOfKeyword(sql, AS); index >= 0 {
		return sql[index+3:]
	} else if index = indexOfString(sql, Blank); index >= 0 {
		return sql[index+1:]
	} else {
		return sql
	}
}

// 提取条件
func extractWhere(sql string) ([]*ConditionParser, string) {
	// 去除where
	if index := firstIndexOfKeyword(sql, WHERE); index >= 0 {
		sql = sql[index+5:]
	}
	var whereSql string
	if _, index := containsKeywords(sql, GroupBy, OrderBy, LIMIT); index >= 0 {
		whereSql, sql = sql[:index], sql[index:]
	} else {
		whereSql, sql = sql, Empty
	}
	var sqlList, lastSql = splitExcludeInBracket(whereSql, AND)
	sqlList = append(sqlList, lastSql)
	var conditions []*ConditionParser
	if len(sqlList) > 0 {
		for _, condition := range sqlList {
			conditions = append(conditions, &ConditionParser{
				Content: strings.TrimSpace(condition),
			})
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

// splitExcludeInBracket 根据分隔符进行拆分但是排除括号内的分隔符
func splitExcludeInBracket(sql, sep string) ([]string, string) {
	var slice []string
	// l：总长度  k:sep长度  m:上个拆分点  n:括号个数
	var l, k, m, n = len(sql), len(sep), 0, 0
	for i := 0; i < l-k; i++ {
		if sql[i] == sep[0] && sql[i:i+k] == sep {
			if n == 0 { // 括号对全部抵消才是有效的分割位
				slice = append(slice, sql[m:i])
				m = i + k // 将当前拆分点后移一个sep长度
			}
		} else if sql[i:i+1] == LeftBracket {
			n++ // 括号加一
		} else if sql[i:i+1] == RightBracket && n > 0 {
			n-- // 抵消一对括号
		}
	}
	return slice, sql[m:]
}

// indexExcludeInBracket 获取关键字下标但排除略括号内的关键字
func indexExcludeInBracket(sql, key string) int {
	// l：总长度  k:关键字长度   n:括号对个数
	var l, k, n = len(sql), len(key), 0
	for i := 0; i < l-k; i++ {
		if sql[i] == key[0] && sql[i:i+k] == key {
			if n == 0 { // 括号对全部抵则是有效关键字
				return i
			}
		} else if sql[i:i+1] == LeftBracket {
			n++ // 括号加一
		} else if sql[i:i+1] == RightBracket && n > 0 {
			n-- // 抵消一对括号
		}
	}
	return -1
}

// containsKeywords 是否包含sql关键字
func containsKeywords(sql string, keys ...string) (string, int) {
	var hit, index = "", -1
	for _, key := range keys {
		if i := firstIndexOfKeyword(sql, key); i >= 0 {
			if i < index {
				hit, index = key, i
			} else if index == -1 {
				hit, index = key, i
			}
		}
	}
	return hit, index
}

// lastIndexOfSqlKeys 获取多个关键字中任一关键字最后命中下标
func lastIndexOfSqlKeys(sql string, keys ...string) int {
	for _, key := range keys {
		if index := lastIndexOfKeyword(sql, key); index >= 0 {
			return index
		}
	}
	return -1
}

// indicesOfString 获取所有下标, x：命中数量
func indicesOfKeyword(sql, key string, size ...int) []int {
	if sl, kl := len(sql), len(key); sl >= kl {
		var s, n = 0, 0
		if size[0] > 0 {
			s = size[0]
		}
		var indices []int
		var index, offset int
		for n <= s || s == 0 {
			if newIndex := firstIndexOfKeyword(sql, key); newIndex >= 0 {
				index = offset + newIndex
				offset = index + kl
				sql = sql[newIndex+kl:]
				indices = append(indices, index)
				n++
			} else {
				break
			}
		}
		return indices
	}
	return nil
}

// 获取关键字的正向N次出现下标
func indexOfKeyword(sql, key string, position int) int {
	if position < 0 {
		return indexOfKeywordReverse(sql, key, position)
	}
	if sl, kl := len(sql), len(key); sl >= kl {
		var index, offset int
		for i := 0; i < position; i++ {
			if newIndex := firstIndexOfKeyword(sql, key); newIndex >= 0 {
				index = offset + newIndex
				offset = index + kl
				sql = sql[newIndex+kl:]
			} else {
				index = -1
				break
			}
		}
		return index
	}
	return -1
}

// 获取关键字的反向n次出现下标
func indexOfKeywordReverse(sql, key string, position int) int {
	if position > 0 {
		return indexOfKeyword(sql, key, position)
	}
	if sl, kl := len(sql), len(key); sl >= kl {
		var index = -1
		for i := 0; i > position; i-- {
			if index = lastIndexOfKeyword(sql, key); index >= 0 {
				sql = sql[:index]
			} else {
				break
			}
		}
		return index
	}
	return -1
}

// 获取sql中关键字首次出现的下标
func firstIndexOfKeyword(sql, key string) int {
	kl, loop, index := len(key), true, 0
	for loop {
		if newIndex := indexOfString(sql, key, 1); newIndex >= 0 {
			sl := len(sql)
			if newIndex == 0 && sql[kl:kl+1] == Blank {
				index, loop = index+newIndex, false
			} else if newIndex == sl-kl && sql[newIndex-1:newIndex] == Blank {
				index, loop = index+newIndex, false
			} else if sql[newIndex-1:newIndex] == Blank && sql[newIndex+kl:newIndex+kl+1] == Blank {
				index, loop = index+newIndex, false
			} else {
				// 当前index无效则缩减原sql继续loop
				index = newIndex + kl
				sql = sql[index:]
			}
		} else {
			index, loop = -1, false // 没找到直接跳出
		}
	}
	return index
}

// 获取sql中关键字末次出现的下标
func lastIndexOfKeyword(sql, key string) int {
	kl, loop, index := len(key), true, 0
	for loop {
		if newIndex := indexOfString(sql, key, -1); newIndex >= 0 {
			sl := len(sql)
			if newIndex == 0 && sql[kl:kl+1] == Blank {
				index, loop = index+newIndex, false
			} else if newIndex == sl-kl && sql[newIndex-1:newIndex] == Blank {
				index, loop = index+newIndex, false
			} else if sql[newIndex-1:newIndex] == Blank && sql[newIndex+kl:newIndex+kl+1] == Blank {
				index, loop = index+newIndex, false
			} else {
				// 当前index无效则缩减原sql继续loop
				sql = sql[:newIndex]
			}
		} else {
			index, loop = -1, false // 没找到直接跳出
		}
	}
	return index
}

// betweenOfString 获取起始字符首次出现和结尾字符末次出现的下标
func betweenOfString(str, start, end string) (from, to int) {
	from, to = -1, -1
	if start == end {
		if indices := indicesOfString(str, start, 2); len(indices) == 2 {
			from, to = indices[0], indices[1]
		} else if len(indices) == 1 {
			from = indices[0]
		}
		return
	}
	var l, m, n = len(str), len(start), len(end)
	if m > l || n > l {
		return
	}
	// x:start个数  y:end个数
	var x, y int
	for i := 0; i < l; i++ {
		if str[i] == start[0] {
			if str[i:i+m] == start {
				x++
				if x == 1 {
					from = i + m
				}
				i = i + m - 1
			}
		}
		if str[i] == end[0] {
			if str[i:i+n] == end {
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

// indicesOfString 获取所有下标, x：命中数量
func indicesOfString(sql, str string, size ...int) []int {
	var s = 0
	if size[0] > 0 {
		s = size[0]
	}
	var indices []int
	l, m, n := len(sql), len(str), 0
	for i := 0; i <= l-m; i++ {
		if n <= s || s == 0 {
			if sql[i] == str[0] && sql[i:i+m] == str {
				indices = append(indices, i)
				i = i + m - 1
				n++
			}
		} else {
			break
		}
	}
	return indices
}

// indexOfString 获取命中子串的首位字符的下标
// position：表示获取位置，默认position=1即正序第1处，position=-1即倒序第1处
func indexOfString(sql, str string, position ...int) int {
	if l, m := len(sql), len(str); l >= m {
		var x, y = 1, 0 // x：目标获取位置，y：sep出现次数计数
		if len(position) > 0 {
			x = position[0]
		}
		for i := 0; i <= l-m; i++ {
			if x > 0 {
				if sql[i] == str[0] && sql[i:i+m] == str {
					y++
					if x == y {
						return i
					}
				}
			} else {
				j := l - i
				if sql[j-1] == str[m-1] && sql[j-m:j] == str {
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

// cutString 分割字符串（reverse=true从右往左）
// position：表示分割位置，默认position=1即正序第1处，position=-1即倒序第1处
func cutString(sql, str string, position ...int) (string, string) {
	if i := indexOfString(sql, str, position...); i >= 0 {
		return sql[:i], sql[i+len(str):]
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
