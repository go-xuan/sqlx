package convert

import (
	"fmt"
	"strings"

	"github.com/go-xuan/sqlx/model"
)

// Conv2Mongo 将查询转换为 MongoDB 查询语句
func (c QueryConvert) Conv2Mongo() string {
	var b strings.Builder

	collection := c.Table.Name
	fmt.Fprintf(&b, "db.%s.find(", collection)

	// filter
	filter := c.mongoFilter()
	if filter != "" {
		b.WriteString(filter)
	}

	// projection
	if proj := c.mongoProjection(); proj != "" {
		b.WriteString(", ")
		b.WriteString(proj)
	}

	b.WriteString(")")

	// sort
	if sort := c.mongoSort(); sort != "" {
		fmt.Fprintf(&b, ".sort(%s)", sort)
	}

	// skip / limit
	if c.Page != nil {
		if c.Page.Offset > 0 {
			fmt.Fprintf(&b, ".skip(%d)", c.Page.Offset)
		}
		if c.Page.Limit > 0 {
			fmt.Fprintf(&b, ".limit(%d)", c.Page.Limit)
		}
	}

	return b.String()
}

func (c QueryConvert) mongoFilter() string {
	if len(c.Filters) == 0 {
		return "{}"
	}
	var parts []string
	for _, f := range c.Filters {
		parts = append(parts, c.filterMongo(f))
	}
	if len(parts) == 1 {
		return fmt.Sprintf("{%s}", parts[0])
	}
	return fmt.Sprintf("{$and: [{%s}]}", strings.Join(parts, "}, {"))
}

func (c QueryConvert) mongoProjection() string {
	if len(c.Fields) == 0 {
		return ""
	}
	var parts []string
	for _, f := range c.Fields {
		if f.Name == "*" {
			return ""
		}
		parts = append(parts, fmt.Sprintf("%s: 1", f.Name))
	}
	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

func (c QueryConvert) mongoSort() string {
	if len(c.Orders) == 0 {
		return ""
	}
	var parts []string
	for _, o := range c.Orders {
		dir := 1
		if o.Desc {
			dir = -1
		}
		parts = append(parts, fmt.Sprintf("%s: %d", o.Field.Name, dir))
	}
	return fmt.Sprintf("{%s}", strings.Join(parts, ", "))
}

func (c QueryConvert) filterMongo(f *model.Filter) string {
	val := c.mongoValue(f.Value)
	switch f.Op {
	case model.EQ, "":
		return fmt.Sprintf("%s: %s", f.Field.Name, val)
	case model.NE:
		return fmt.Sprintf("%s: {$ne: %s}", f.Field.Name, val)
	case model.LT:
		return fmt.Sprintf("%s: {$lt: %s}", f.Field.Name, val)
	case model.GT:
		return fmt.Sprintf("%s: {$gt: %s}", f.Field.Name, val)
	case model.LE:
		return fmt.Sprintf("%s: {$lte: %s}", f.Field.Name, val)
	case model.GE:
		return fmt.Sprintf("%s: {$gte: %s}", f.Field.Name, val)
	case model.LIKE:
		return fmt.Sprintf("%s: {$regex: %s}", f.Field.Name, val)
	case model.IN:
		return fmt.Sprintf("%s: {$in: [%s]}", f.Field.Name, c.mongoSlice(f.Values))
	case model.NIN:
		return fmt.Sprintf("%s: {$nin: [%s]}", f.Field.Name, c.mongoSlice(f.Values))
	default:
		return fmt.Sprintf("%s: %s", f.Field.Name, val)
	}
}

func (c QueryConvert) mongoValue(v any) string {
	switch t := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", strings.ReplaceAll(t, "\"", "\\\""))
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", t)
	}
}

func (c QueryConvert) mongoSlice(v []any) string {
	strs := make([]string, len(v))
	for i, e := range v {
		strs[i] = c.mongoValue(e)
	}
	return strings.Join(strs, ", ")
}
