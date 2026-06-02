package convert

import (
	"fmt"
	"strings"

	"github.com/go-xuan/sqlx/model"
)

// Conv2SQL 将查询转换为 SQL 语句
func (c QueryConvert) Conv2SQL() string {
	var b strings.Builder

	// SELECT
	b.WriteString("SELECT ")
	if len(c.Fields) == 0 {
		b.WriteString("*")
	} else {
		for i, f := range c.Fields {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(f.Name)
			if f.Alias != "" {
				b.WriteString(" AS ")
				b.WriteString(f.Alias)
			}
		}
	}

	// FROM
	b.WriteString(" FROM ")
	b.WriteString(c.Table.Name)
	if c.Table.Alias != "" {
		b.WriteString(" AS ")
		b.WriteString(c.Table.Alias)
	}

	// WHERE
	if len(c.Filters) > 0 {
		b.WriteString(" WHERE ")
		for i, f := range c.Filters {
			if i > 0 {
				b.WriteString(" AND ")
			}
			b.WriteString(c.filterSQL(f))
		}
	}

	// ORDER BY
	if len(c.Orders) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range c.Orders {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(o.Field.Name)
			if o.Desc {
				b.WriteString(" DESC")
			}
		}
	}

	// LIMIT / OFFSET
	if c.Page != nil {
		if c.Page.Limit > 0 {
			fmt.Fprintf(&b, " LIMIT %d", c.Page.Limit)
		}
		if c.Page.Offset > 0 {
			fmt.Fprintf(&b, " OFFSET %d", c.Page.Offset)
		}
	}

	return b.String()
}

func (c QueryConvert) filterSQL(f *model.Filter) string {
	col := f.Field.Name
	val := c.formatValue(f.Value)

	switch f.Op {
	case model.EQ, "":
		return fmt.Sprintf("%s = %s", col, val)
	case model.NE:
		return fmt.Sprintf("%s != %s", col, val)
	case model.LT:
		return fmt.Sprintf("%s < %s", col, val)
	case model.GT:
		return fmt.Sprintf("%s > %s", col, val)
	case model.LE:
		return fmt.Sprintf("%s <= %s", col, val)
	case model.GE:
		return fmt.Sprintf("%s >= %s", col, val)
	case model.LIKE:
		return fmt.Sprintf("%s LIKE %s", col, val)
	case model.IN:
		return fmt.Sprintf("%s IN (%s)", col, c.formatSlice(f.Values))
	case model.NIN:
		return fmt.Sprintf("%s NOT IN (%s)", col, c.formatSlice(f.Values))
	default:
		return fmt.Sprintf("%s = %s", col, val)
	}
}

func (c QueryConvert) formatValue(v any) string {
	switch t := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(t, "'", "''"))
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", t)
	}
}

func (c QueryConvert) formatSlice(v []any) string {
	strs := make([]string, len(v))
	for i, e := range v {
		switch t := e.(type) {
		case string:
			strs[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(t, "'", "''"))
		default:
			strs[i] = fmt.Sprintf("%v", t)
		}
	}
	return strings.Join(strs, ", ")
}
