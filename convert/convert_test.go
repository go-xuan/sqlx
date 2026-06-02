package convert

import (
	"testing"

	"github.com/go-xuan/sqlx/model"
)

func TestConv2SQL_Basic(t *testing.T) {
	q := QueryConvert{
		Table:  &model.Table{Name: "users"},
		Fields: []*model.Field{{Name: "id"}, {Name: "name"}, {Name: "age"}},
	}
	want := "SELECT id, name, age FROM users"
	if got := q.Conv2SQL(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2SQL_SelectAll(t *testing.T) {
	q := QueryConvert{
		Table: &model.Table{Name: "users"},
	}
	want := "SELECT * FROM users"
	if got := q.Conv2SQL(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2SQL_WithAlias(t *testing.T) {
	q := QueryConvert{
		Table:  &model.Table{Name: "users", Alias: "u"},
		Fields: []*model.Field{{Name: "id", Alias: "user_id"}, {Name: "name", Alias: "user_name"}},
	}
	want := "SELECT id AS user_id, name AS user_name FROM users AS u"
	if got := q.Conv2SQL(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2SQL_WithFilters(t *testing.T) {
	q := QueryConvert{
		Table:  &model.Table{Name: "users"},
		Fields: []*model.Field{{Name: "*"}},
		Filters: []*model.Filter{
			{Field: &model.Field{Name: "id"}, Op: model.EQ, Value: 1},
			{Field: &model.Field{Name: "name"}, Op: model.EQ, Value: "Alice"},
			{Field: &model.Field{Name: "age"}, Op: model.GE, Value: 18},
		},
	}
	want := "SELECT * FROM users WHERE id = 1 AND name = 'Alice' AND age >= 18"
	if got := q.Conv2SQL(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2SQL_WithOrder(t *testing.T) {
	q := QueryConvert{
		Table:  &model.Table{Name: "users"},
		Fields: []*model.Field{{Name: "*"}},
		Orders: []*model.Order{
			{Field: &model.Field{Name: "id"}, Desc: true},
			{Field: &model.Field{Name: "name"}},
		},
	}
	want := "SELECT * FROM users ORDER BY id DESC, name"
	if got := q.Conv2SQL(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2SQL_WithPage(t *testing.T) {
	q := QueryConvert{
		Table:  &model.Table{Name: "users"},
		Fields: []*model.Field{{Name: "*"}},
		Page:   &model.Page{Limit: 10, Offset: 20},
	}
	want := "SELECT * FROM users LIMIT 10 OFFSET 20"
	if got := q.Conv2SQL(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2SQL_OperatorTypes(t *testing.T) {
	tests := []struct {
		name   string
		op     model.OP
		val    any
		values []any
		want   string
	}{
		{"eq", model.EQ, 1, nil, "id = 1"},
		{"ne", model.NE, 1, nil, "id != 1"},
		{"lt", model.LT, 100, nil, "id < 100"},
		{"gt", model.GT, 0, nil, "id > 0"},
		{"le", model.LE, 100, nil, "id <= 100"},
		{"ge", model.GE, 0, nil, "id >= 0"},
		{"like", model.LIKE, "%test%", nil, "id LIKE '%test%'"},
		{"in", model.IN, nil, []any{1, 2, 3}, "id IN (1, 2, 3)"},
		{"nin", model.NIN, nil, []any{"a", "b"}, "id NOT IN ('a', 'b')"},
		{"default", "", 1, nil, "id = 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := QueryConvert{
				Table:  &model.Table{Name: "t"},
				Fields: []*model.Field{{Name: "*"}},
				Filters: []*model.Filter{
					{Field: &model.Field{Name: "id"}, Op: tt.op, Value: tt.val, Values: tt.values},
				},
			}
			want := "SELECT * FROM t WHERE " + tt.want
			if got := q.Conv2SQL(); got != want {
				t.Errorf("\nwant: %s\ngot : %s", want, got)
			}
		})
	}
}

func TestConv2Mongo_Basic(t *testing.T) {
	q := QueryConvert{
		Table:  &model.Table{Name: "users"},
		Fields: []*model.Field{{Name: "id"}, {Name: "name"}},
	}
	want := `db.users.find({}, {id: 1, name: 1})`
	if got := q.Conv2Mongo(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2Mongo_NoFields(t *testing.T) {
	q := QueryConvert{
		Table: &model.Table{Name: "users"},
	}
	want := "db.users.find({})"
	if got := q.Conv2Mongo(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2Mongo_WithFilters(t *testing.T) {
	q := QueryConvert{
		Table: &model.Table{Name: "users"},
		Filters: []*model.Filter{
			{Field: &model.Field{Name: "age"}, Op: model.GE, Value: 18},
		},
	}
	want := "db.users.find({age: {$gte: 18}})"
	if got := q.Conv2Mongo(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2Mongo_WithSort(t *testing.T) {
	q := QueryConvert{
		Table: &model.Table{Name: "users"},
		Orders: []*model.Order{
			{Field: &model.Field{Name: "id"}, Desc: true},
		},
	}
	want := "db.users.find({}).sort({id: -1})"
	if got := q.Conv2Mongo(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2Mongo_WithPage(t *testing.T) {
	q := QueryConvert{
		Table: &model.Table{Name: "users"},
		Page:  &model.Page{Limit: 10, Offset: 20},
	}
	want := "db.users.find({}).skip(20).limit(10)"
	if got := q.Conv2Mongo(); got != want {
		t.Errorf("\nwant: %s\ngot : %s", want, got)
	}
}

func TestConv2Mongo_OperatorTypes(t *testing.T) {
	tests := []struct {
		name   string
		op     model.OP
		val    any
		values []any
		want   string
	}{
		{"eq", model.EQ, 1, nil, "db.t.find({id: 1})"},
		{"ne", model.NE, 1, nil, "db.t.find({id: {$ne: 1}})"},
		{"lt", model.LT, 100, nil, "db.t.find({id: {$lt: 100}})"},
		{"gt", model.GT, 0, nil, "db.t.find({id: {$gt: 0}})"},
		{"le", model.LE, 100, nil, "db.t.find({id: {$lte: 100}})"},
		{"ge", model.GE, 0, nil, "db.t.find({id: {$gte: 0}})"},
		{"like", model.LIKE, "test", nil, `db.t.find({id: {$regex: "test"}})`},
		{"in", model.IN, nil, []any{1, 2, 3}, "db.t.find({id: {$in: [1, 2, 3]}})"},
		{"nin", model.NIN, nil, []any{"a", "b"}, `db.t.find({id: {$nin: ["a", "b"]}})`},
		{"default", "", 1, nil, "db.t.find({id: 1})"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := QueryConvert{
				Table: &model.Table{Name: "t"},
				Filters: []*model.Filter{
					{Field: &model.Field{Name: "id"}, Op: tt.op, Value: tt.val, Values: tt.values},
				},
			}
			if got := q.Conv2Mongo(); got != tt.want {
				t.Errorf("\nwant: %s\ngot : %s", tt.want, got)
			}
		})
	}
}
