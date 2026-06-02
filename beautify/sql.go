package beautify

import (
	"fmt"
	"strings"

	"github.com/go-xuan/sqlx/consts"
	"github.com/go-xuan/sqlx/model"
	"github.com/go-xuan/sqlx/utils"
)

// NewSQL 初始化SQL
func NewSQL(sql string, indent ...int) SQL {
	var n int
	if len(indent) > 0 {
		n = indent[0]
	}
	return SQL{
		origin: sql,
		temp:   sql,
		indent: 6 + n,
	}
}

// SQL 解析器base
type SQL struct {
	origin   string            // 原始sql，原始完整sql（变量值需要通过 replacer 进行还原）
	temp     string            // 临时sql，存储每个步骤经过sql拆解之后的sql片段
	indent   int               // 缩进，初始值为6
	replacer *strings.Replacer // 变量值替换器，consts.ReplacePrefix + 编号 + consts.ReplaceSuffix
}

// 解析准备
func (s *SQL) parsePrepare() {
	sql := s.temp
	var replacer *strings.Replacer
	if sql, replacer = utils.ParseValuesInSql(sql); replacer != nil {
		s.replacer = replacer
	}
	s.temp = utils.AllKeywordsToLower(sql)
}

// 解析完成
func (s *SQL) parseFinish() {
	s.temp = ""
}

// 以当前缩进量对齐
func (s *SQL) align(key ...string) string {
	return Align(s.indent, key...)
}

// Align 根据缩进量对齐
func Align(indent int, key ...string) string {
	if len(key) == 0 {
		return strings.Repeat(consts.Blank, indent)
	} else if str := key[0]; len(str) <= indent {
		return strings.Repeat(consts.Blank, indent-len(str)) + str
	} else if cut, _ := utils.CutString(str, consts.Blank); len(cut) <= indent {
		return strings.Repeat(consts.Blank, indent-len(cut)) + str
	} else {
		return str
	}
}

// ExtractWhere 提取条件
func ExtractWhere(sql string) ([]*model.Filter, string, error) {
	if sql != "" {
		if first := utils.IndexOfKeywordFirst(sql, consts.WHERE); first >= 0 {
			sql = sql[first+len(consts.WHERE):]
			var whereSql string
			if _, end := utils.ContainsKeywords(sql, consts.GROUPBY, consts.ORDERBY, consts.LIMIT); end >= 0 {
				whereSql, sql = sql[:end], sql[end:]
			} else {
				whereSql, sql = sql, consts.Empty
			}
			filters, err := NewConditions(whereSql)
			if err != nil {
				return nil, sql, err
			}
			return filters, sql, nil
		}
	}
	return nil, sql, nil
}

// NewConditions 全部条件
func NewConditions(sql string) ([]*model.Filter, error) {
	sql = utils.TrimBrackets(sql)
	var filters []*model.Filter
	var loop, logical = true, model.LogicOp("")
	for loop {
		if index := utils.IndexExcludeBrackets(sql, consts.AND, true); index > 0 {
			filter, err := NewCondition(sql[:index], logical)
			if err != nil {
				return nil, err
			}
			filters = append(filters, filter)
			sql, logical = sql[index+len(consts.AND)+1:], model.AND
		} else if index = utils.IndexExcludeBrackets(sql, consts.OR, true); index > 0 {
			filter, err := NewCondition(sql[:index], logical)
			if err != nil {
				return nil, err
			}
			filters = append(filters, filter)
			sql, logical = sql[index+len(consts.OR)+1:], model.OR
		} else {
			filter, err := NewCondition(sql, logical)
			if err != nil {
				return nil, err
			}
			filters = append(filters, filter)
			loop = false
		}
	}
	return filters, nil
}

// NewCondition 单个条件
func NewCondition(sql string, logical model.LogicOp) (*model.Filter, error) {
	sql = strings.TrimSpace(sql)
	var filter = &model.Filter{LogicOp: logical}
	if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from == 0 && to == len(sql)-1 {
		children, err := NewConditions(sql[from+1 : to])
		if err != nil {
			return nil, err
		}
		filter.Children = children
	} else if index := utils.IndexExcludeBrackets(sql, consts.NE, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.NE))
	} else if index = utils.IndexExcludeBrackets(sql, consts.GE, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.GE))
	} else if index = utils.IndexExcludeBrackets(sql, consts.LE, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.LE))
	} else if index = utils.IndexExcludeBrackets(sql, consts.EQ, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.EQ))
	} else if index = utils.IndexExcludeBrackets(sql, consts.LT, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.LT))
	} else if index = utils.IndexExcludeBrackets(sql, consts.GT, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.GT))
	} else if index = utils.IndexExcludeBrackets(sql, consts.LIKE, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.LIKE))
	} else if index = utils.IndexExcludeBrackets(sql, consts.NOTIN, true); index > 0 {
		filter.Field = &model.Field{Name: sql[:index-1]}
		filter.Op = model.NIN
		if err := parseIN(filter, sql[index+len(consts.NOTIN)+1:]); err != nil {
			return nil, err
		}
	} else if index = utils.IndexExcludeBrackets(sql, consts.IN, true); index > 0 {
		filter.Field = &model.Field{Name: sql[:index-1]}
		filter.Op = model.IN
		if err := parseIN(filter, sql[index+len(consts.IN)+1:]); err != nil {
			return nil, err
		}
	} else if index = utils.IndexExcludeBrackets(sql, consts.ISNOT, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.ISNOT))
	} else if index = utils.IndexExcludeBrackets(sql, consts.IS, true); index > 0 {
		filter.Field, filter.Op, filter.Value = extractOp(sql, index, len(consts.IS))
	} else {
		filter.Field = &model.Field{Name: sql}
	}
	return filter, nil
}

func extractOp(sql string, index, opLen int) (field *model.Field, op model.OP, value string) {
	return &model.Field{Name: sql[:index-1]}, model.OP(sql[index : index+opLen]), sql[index+opLen+1:]
}

func parseIN(filter *model.Filter, sql string) error {
	sql = strings.Trim(sql, "() ;")
	if first := utils.IndexOfKeywordFirst(sql, consts.SELECT); first >= 0 {
		indent := len(filter.Field.Name) + 12
		subSelect, err := ParseSelectSQL(sql, indent)
		if err != nil {
			return err
		}
		filter.SubSQL = subSelect.Beautify()
		filter.Select = subSelect.ToModel()
	} else {
		vals := strings.Split(sql, consts.Comma)
		filter.Values = make([]any, len(vals))
		for i, v := range vals {
			filter.Values[i] = strings.TrimSpace(v)
		}
	}
	return nil
}

func getIN(filter *model.Filter, indent int) string {
	var b strings.Builder
	b.WriteString(consts.LeftBracket)
	if filter.SubSQL != "" {
		b.WriteString(filter.SubSQL)
	} else if len(filter.Values) > 0 {
		nextLine := len(filter.Values) > 3
		for i, v := range filter.Values {
			if i > 0 {
				b.WriteString(consts.Comma)
				b.WriteString(consts.Blank)
				if nextLine {
					b.WriteString(consts.NextLine)
					b.WriteString(Align(indent))
				}
			}
			fmt.Fprint(&b, v)
		}
	}
	b.WriteString(consts.RightBracket)
	return b.String()
}

func filterSQL(filter *model.Filter, indent int) string {
	var b strings.Builder
	if filter.LogicOp != "" {
		b.WriteString(Align(indent, string(filter.LogicOp)))
		b.WriteString(consts.Blank)
	}
	if len(filter.Children) > 0 {
		b.WriteString("(")
		for i, child := range filter.Children {
			if i > 0 {
				b.WriteString(consts.Blank)
			}
			b.WriteString(filterSQL(child, 0))
		}
		b.WriteString(")")
	} else {
		b.WriteString(filter.Field.Name)
		b.WriteString(consts.Blank)
		b.WriteString(string(filter.Op))
		b.WriteString(consts.Blank)
		if filter.Op == model.IN || filter.Op == model.NIN {
			b.WriteString(getIN(filter, indent+len(filter.Field.Name)+6))
		} else {
			fmt.Fprint(&b, filter.Value)
		}
	}
	return b.String()
}

// ExtractTable 提取主表
func ExtractTable(sql string, indent int) (*model.Table, string, error) {
	if index := utils.IndexExcludeBrackets(sql, consts.FROM, true); index >= 0 {
		sql = sql[index+len(consts.FROM):]
	} else if sql[:1] != consts.Blank {
		sql = consts.Blank + sql
	}
	var table = &model.Table{}
	if sql[1:2] == consts.LeftBracket {
		if from, to := utils.BetweenOfString(sql, consts.LeftBracket, consts.RightBracket); from >= 0 && from < to {
			subSelect, err := ParseSelectSQL(sql[from+1:to], indent+2)
			if err != nil {
				return nil, "", err
			}
			table.SubSQL = subSelect.Beautify()
			table.Select = subSelect.ToModel()
			sql = sql[to:]
		} else {
			return nil, "", fmt.Errorf("解析sql异常：无法匹配括号")
		}
	} else {
		before := utils.IndexOfString(sql, consts.Blank, 1)
		if after := utils.IndexOfString(sql, consts.Blank, 2); after >= 0 {
			table.Name, sql = sql[before+1:after], sql[after+1:]
		} else {
			table.Name, sql = sql[before+1:], ""
		}
	}
	if sql != "" {
		var alias string
		if _, index := utils.ContainsKeywords(sql, consts.LEFT, consts.RIGHT, consts.INNER, consts.OUTER, consts.JOIN,
			consts.WHERE, consts.GROUPBY, consts.ORDERBY, consts.LIMIT); index >= 0 {
			alias, sql = sql[:index], sql[index:]
		} else {
			alias, sql = sql, consts.Empty
		}
		table.Alias = utils.ExtractAlias(alias)
	}
	return table, sql, nil
}

func tableSQL(t *model.Table, withAs ...bool) string {
	var b strings.Builder
	if t.SubSQL != "" {
		b.WriteString(consts.LeftBracket)
		b.WriteString(t.SubSQL)
		b.WriteString(consts.RightBracket)
	} else {
		b.WriteString(t.Name)
	}
	if t.Alias != "" {
		if len(withAs) > 0 && withAs[0] {
			b.WriteString(consts.Blank)
			b.WriteString(consts.AS)
		}
		b.WriteString(consts.Blank)
		b.WriteString(t.Alias)
	}
	return b.String()
}

// beautifyCondition 构建条件SQL（供 Update 和 Delete 复用）
func (s *SQL) beautifyCondition(filters []*model.Filter) string {
	if len(filters) == 0 {
		return ""
	}
	var b strings.Builder
	var maxLen int
	for _, f := range filters {
		if l := len(f.Field.Name); maxLen < l {
			maxLen = l
		}
	}
	b.WriteString(consts.NextLine)
	b.WriteString(s.align(consts.WHERE))
	b.WriteString(consts.Blank)
	for i, f := range filters {
		if i > 0 {
			b.WriteString(consts.NextLine)
			if f.LogicOp == "" {
				b.WriteString(s.align(consts.AND))
				b.WriteString(consts.Blank)
			} else {
				b.WriteString(s.align(string(f.LogicOp)))
				b.WriteString(consts.Blank)
			}
		}
		if f.Field.Name != "" {
			b.WriteString(f.Field.Name)
			b.WriteString(strings.Repeat(consts.Blank, maxLen-len(f.Field.Name)+1))
			b.WriteString(string(f.Op))
			b.WriteString(consts.Blank)
		}
		fmt.Fprint(&b, f.Value)
	}
	return b.String()
}
