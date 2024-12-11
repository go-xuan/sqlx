package utils

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/go-xuan/sqlx/consts"
)

type SqlUtils struct {
	sql string
}

// ExtractAlias 提取别名
func ExtractAlias(sql string) string {
	sql = strings.TrimSpace(sql)
	if index := FirstIndexOfKeyword(sql, consts.AS); index >= 0 {
		return sql[index+3:]
	} else if index = IndexOfString(sql, consts.Blank); index >= 0 {
		return sql[index+1:]
	} else {
		return sql
	}
}

// ParseValuesInSql 解析sql中的变量值
func ParseValuesInSql(sql string) (string, *strings.Replacer) {
	values := regexp.MustCompile(`'[^']*'`).FindAllString(sql, -1)
	if len(values) > 0 {
		var all []string
		for i, value := range values {
			var key = consts.ReplacePrefix + strconv.Itoa(i+1)
			sql = strings.Replace(sql, value, key, 1)
			all = append(all, key, value)
		}
		replacer := strings.NewReplacer(all...)
		return sql, replacer
	}
	return sql, nil
}

// AllKeywordsToLower 将所有关键字转为小写
func AllKeywordsToLower(sql string) string {
	var oldnew []string
	var KEYWORDS = []string{
		consts.SELECT, consts.UPDATE, consts.CREATE, consts.DELETE, consts.INSERT, consts.INTO, consts.FROM,
		consts.WHERE, consts.SET, consts.JOIN, consts.GROUP, consts.ORDER, consts.HAVING, consts.LIMIT, consts.OFFSET,
		consts.ASC, consts.DESC, consts.CASE, consts.WHEN, consts.THEN, consts.END, consts.INNER, consts.OUTER, consts.LEFT, consts.RIGHT,
		consts.DISTINCT, consts.PARTITION, consts.OVER, consts.AS, consts.AND, consts.ON, consts.OR, consts.IN, consts.NOT, consts.LIKE, consts.By,
	}
	for _, keyword := range KEYWORDS {
		switch keyword {
		case consts.ASC, consts.DESC:
			keyword = " " + keyword
		default:
			keyword = keyword + " "
		}
		oldnew = append(oldnew, strings.ToUpper(keyword), keyword)
	}
	sql = strings.NewReplacer(oldnew...).Replace(sql)
	return sql
}

func SplitValuesSql(valuesSql string) []string {
	valuesSql = strings.Trim(valuesSql, "() ")
	values, value := SplitExcludeInBracket(valuesSql, consts.Comma)
	values = append(values, value)
	return values
}

// SplitExcludeInBracket 根据分隔符进行拆分但是排除括号内的分隔符
func SplitExcludeInBracket(sql, key string) ([]string, string) {
	var slice []string
	var sl, kl, offset, brackets = len(sql), len(key), 0, 0
	for i := 0; i < sl-kl; i++ {
		if sql[i] == key[0] && sql[i:i+kl] == key {
			// 当前位置前面的括号全部抵消才表示是有效命中
			if brackets == 0 {
				slice = append(slice, sql[offset:i])
				offset = i + kl // 将当前拆分点后移一个sep长度
			}
		} else if sql[i:i+1] == consts.LeftBracket {
			brackets++ // 括号加一
		} else if sql[i:i+1] == consts.RightBracket && brackets > 0 {
			brackets-- // 抵消一对括号
		}
	}
	return slice, sql[offset:]
}

// IndexExcludeBrackets 获取关键字下标但排除略括号内的关键字
func IndexExcludeBrackets(sql, key string, pure bool) int {
	var sl, kl, brackets = len(sql), len(key), 0
	for i := 0; i < sl-kl; i++ {
		if sql[i] == key[0] && sql[i:i+kl] == key {
			// 当前位置前面的括号全部抵消才表示是有效命中
			if brackets == 0 {
				if pure && !HasAdjacent(sql, key, consts.Blank, i) {
					continue
				}
				return i
			}
		} else if sql[i:i+1] == consts.LeftBracket {
			brackets++ // 括号加一
		} else if sql[i:i+1] == consts.RightBracket && brackets > 0 {
			brackets-- // 抵消一对括号
		}
	}
	return -1
}

// ContainsKeywords 是否包含sql关键字
func ContainsKeywords(sql string, keys ...string) (string, int) {
	var hit, index = "", -1
	for _, key := range keys {
		if i := FirstIndexOfKeyword(sql, key); i >= 0 {
			if i < index {
				hit, index = key, i
			} else if index == -1 {
				hit, index = key, i
			}
		}
	}
	return hit, index
}

// LastIndexOfKeys 获取多个关键字中任一关键字最后命中下标
func LastIndexOfKeys(sql string, keys ...string) (string, int) {
	var hit, index = "", -1
	for _, key := range keys {
		if i := LastIndexOfKeyword(sql, key); i >= 0 && i > index {
			hit, index = key, i
		}
	}
	return hit, index
}

// FirstIndexOfKeys 获取多个关键字中任一关键字首次命中下标
func FirstIndexOfKeys(sql string, keys ...string) (string, int) {
	var max = len(sql) - 1
	var hit, index = "", max
	for _, key := range keys {
		if i := FirstIndexOfKeyword(sql, key); i >= 0 && i < index {
			index = i
		}
	}
	if index == max {
		index = -1
	}
	return hit, index
}

// IndicesOfKeyword 获取所有下标, x：命中数量
func IndicesOfKeyword(sql, key string, size ...int) []int {
	if sl, kl := len(sql), len(key); sl >= kl {
		var s, n = 0, 0
		if size[0] > 0 {
			s = size[0]
		}
		var indices []int
		var index, offset int
		for n <= s || s == 0 {
			if newIndex := FirstIndexOfKeyword(sql, key); newIndex >= 0 {
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

// IndexOfKeyword 获取关键字的正向N次出现下标
func IndexOfKeyword(sql, key string, position int) int {
	if position < 0 {
		return IndexOfKeywordReverse(sql, key, position)
	}
	if sl, kl := len(sql), len(key); sl >= kl {
		var index, offset int
		for i := 0; i < position; i++ {
			if newIndex := FirstIndexOfKeyword(sql, key); newIndex >= 0 {
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

// IndexOfKeywordReverse 获取关键字的反向n次出现下标
func IndexOfKeywordReverse(sql, key string, position int) int {
	if position > 0 {
		return IndexOfKeyword(sql, key, position)
	}
	if sl, kl := len(sql), len(key); sl >= kl {
		var index = -1
		for i := 0; i > position; i-- {
			if index = LastIndexOfKeyword(sql, key); index >= 0 {
				sql = sql[:index]
			} else {
				break
			}
		}
		return index
	}
	return -1
}

// FirstIndexOfKeyword 获取sql中关键字首次出现的下标
func FirstIndexOfKeyword(sql, key string) int {
	//kl: key字符长度 loop:继续循环 index：命中下标
	kl, loop, index := len(key), true, 0
	for loop {
		if newIndex := IndexOfString(sql, key, 1); newIndex >= 0 {
			if HasAdjacent(sql, key, consts.Blank, newIndex) {
				index, loop = index+newIndex, false
			} else {
				index = newIndex + kl
				sql = sql[index:]
			}
		} else {
			index, loop = -1, false // 没找到直接跳出
		}
	}
	return index
}

// LastIndexOfKeyword 获取sql中关键字末次出现的下标
func LastIndexOfKeyword(sql, key string) int {
	loop, index := true, 0
	for loop {
		if newIndex := IndexOfString(sql, key, -1); newIndex >= 0 {
			if HasAdjacent(sql, key, consts.Blank, newIndex) {
				index, loop = index+newIndex, false
			} else {
				sql = sql[:newIndex]
			}
		} else {
			index, loop = -1, false // 没找到直接跳出
		}
	}
	return index
}

// HasAdjacent 判断目标kew在文本中当前位置是否有相邻字符
func HasAdjacent(str, key, adjacent string, index int) bool {
	sl, kl, al := len(str), len(key), len(adjacent)
	if index == 0 {
		return str[kl:kl+al] == adjacent
	} else if index == sl-kl {
		return str[index-al:index] == adjacent
	} else {
		return str[index-al:index] == adjacent && str[index+kl:index+kl+al] == adjacent
	}
}

// BetweenOfString 获取起始字符首次出现和结尾字符末次出现的下标
func BetweenOfString(str, start, end string) (from, to int) {
	from, to = -1, -1
	if start == end {
		if indices := indicesOfString(str, start, 2); len(indices) == 2 {
			from, to = indices[0], indices[1]
		} else if len(indices) == 1 {
			from = indices[0]
		}
		return
	}
	var l, sl, el = len(str), len(start), len(end)
	if sl > l || el > l {
		return
	}
	// x:start个数  y:end个数
	var x, y int
	for i := 0; i < l; i++ {
		if str[i] == start[0] {
			if str[i:i+sl] == start {
				x++
				if x == 1 {
					from = i
				}
				i = i + sl - 1
			}
		}
		if str[i] == end[0] {
			if str[i:i+el] == end {
				y++
				if y == x || x == 1 {
					to = i
					break
				}
				i = i + el - 1
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

// IndexOfString 获取命中子串的首位字符的下标
// position：表示获取位置，默认position=1即正序第1处，position=-1即倒序第1处
func IndexOfString(sql, str string, position ...int) int {
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

func TrimBrackets(sql string) string {
	var loop = true
	for loop {
		var max = len(sql) - 1
		if from, to := BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from == 0 && to == max {
			sql = sql[from+1 : to]
		} else {
			loop = false
		}
	}
	return sql
}

// CutString 分割字符串（reverse=true从右往左）
// position：表示分割位置，默认position=1即正序第1处，position=-1即倒序第1处
func CutString(sql, str string, position ...int) (string, string) {
	if i := IndexOfString(sql, str, position...); i >= 0 {
		return sql[:i], sql[i+len(str):]
	}
	return sql, ""
}

// DB2GoType DB-Go类型映射
func DB2GoType(t string) string {
	switch t {
	case consts.Char, consts.Varchar, consts.Varchar100, consts.Text, consts.Uuid:
		return consts.String
	case consts.Int, consts.Int2, consts.Int4, consts.Tinyint, consts.Smallint, consts.Mediumint:
		return consts.Int
	case consts.Int8, consts.Bigint:
		return consts.Int64
	case consts.Float, consts.Float4, consts.Numeric:
		return consts.Float64
	case consts.Timestamp, consts.Timestampz, consts.Datetime, consts.Time, consts.Date:
		return consts.TimeTime
	case consts.Bool:
		return consts.Bool
	default:
		return consts.String
	}
}
