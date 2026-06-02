package beautify

import (
	"strings"
	"testing"
)

// assertBeautify 比较美化后的 SQL 与预期输出，不一致时打印差异
func assertBeautify(t *testing.T, sql, want string) {
	t.Helper()
	parser, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	got := parser.Beautify()
	t.Logf("\n%s", got)
	if got != want {
		t.Errorf("\n===== MISMATCH =====\ninput: %s\nwant : %q\ngot  : %q\n====================", sql, want, got)
	}
}

// assertBeautifyContains 验证美化输出包含指定内容
func assertBeautifyContains(t *testing.T, sql string, keywords ...string) {
	t.Helper()
	parser, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	got := parser.Beautify()
	t.Logf("\n%s", got)
	lower := strings.ToLower(got)
	for _, kw := range keywords {
		if !strings.Contains(lower, strings.ToLower(kw)) {
			t.Errorf("\nexpected %q in output:\n%s", kw, got)
		}
	}
}

func TestSelect_Simple(t *testing.T) {
	assertBeautify(t,
		"select id, name, age from users",
		"select id, name, age\n  from users")
}

func TestSelect_Where(t *testing.T) {
	assertBeautify(t,
		"select * from users where id = 1",
		"select *\n  from users\n where id = 1")
}

func TestSelect_WhereAnd(t *testing.T) {
	assertBeautify(t,
		"select * from users where id = 1 and name = 'test'",
		"select *\n  from users\n where id = 1\n   and name = 'test'")
}

func TestSelect_OrderBy(t *testing.T) {
	assertBeautify(t,
		"select * from users order by id desc",
		"select *\n  from users\n order by id desc")
}

func TestSelect_GroupBy(t *testing.T) {
	assertBeautify(t,
		"select dept, count(*) from users group by dept",
		"select dept, count(*)\n  from users\n group by dept")
}

func TestSelect_Limit(t *testing.T) {
	assertBeautify(t,
		"select * from users limit 10",
		"select *\n  from users\n limit 10")
}

func TestSelect_Distinct(t *testing.T) {
	assertBeautify(t,
		"select distinct name from users",
		"select distinct name\n  from users")
}

func TestSelect_Alias(t *testing.T) {
	assertBeautify(t,
		"select id as user_id, name as user_name from users u",
		"select id   as user_id,\n       name as user_name\n  from users as u")
}

func TestSelect_Join(t *testing.T) {
	assertBeautify(t,
		"select u.name, o.total from users u left join orders o on u.id = o.user_id where u.status = 1",
		"select u.name, o.total\n  from users as u\n  left join orders as o\n    on u.id = o.user_id\n where u.status = 1")
}

func TestInsert_Simple(t *testing.T) {
	assertBeautify(t,
		`insert into users (name, age) values ('Alice', 30)`,
		"insert into users\n     (name, age)\nvalues\n     ('Alice', 30)")
}

func TestInsert_MultiRows(t *testing.T) {
	assertBeautify(t,
		`insert into users (name, age) values ('Alice', 30), ('Bob', 25)`,
		"insert into users\n     (name, age)\nvalues\n     ('Alice', 30),\n     ('Bob', 25)")
}

func TestUpdate_Simple(t *testing.T) {
	assertBeautify(t,
		"update users set name = 'Bob' where id = 1",
		"update users\n   set name = 'Bob'\n where id = 1")
}

func TestUpdate_MultiFields(t *testing.T) {
	assertBeautify(t,
		"update users set name = 'Bob', age = 30 where id = 1",
		"update users\n   set name = 'Bob',\n       age  = 30\n where id = 1")
}

func TestDelete_Simple(t *testing.T) {
	assertBeautify(t,
		"delete from users where id = 1",
		"delete from users\n where id = 1")
}

func TestSelect_Subquery(t *testing.T) {
	assertBeautifyContains(t,
		"select * from (select id, name from users) t",
		"select", "from", "users", "id", "name", "t")
}

func TestCondition_Operators(t *testing.T) {
	tests := []struct {
		name, sql, wantOp, wantName, wantValue string
	}{
		{"=", "id = 1", "=", "id", "1"},
		{"!=", "id != 1", "!=", "id", "1"},
		{"<", "age < 18", "<", "age", "18"},
		{">", "age > 18", ">", "age", "18"},
		{"<=", "age <= 18", "<=", "age", "18"},
		{">=", "age >= 18", ">=", "age", "18"},
		{"like", "name like '%test%'", "like", "name", "'%test%'"},
		{"in", "id in (1,2,3)", "in", "id", ""},
		{"not in", "id not in (1,2,3)", "not in", "id", ""},
		{"is null", "name is null", "is", "name", "null"},
		{"is not null", "name is not null", "is not", "name", "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewCondition(tt.sql, "")
			if err != nil {
				t.Fatalf("NewCondition error: %v", err)
			}
			if string(filter.Op) != tt.wantOp {
				t.Errorf("Op: want %q, got %q", tt.wantOp, string(filter.Op))
			}
			if filter.Field.Name != tt.wantName {
				t.Errorf("Name: want %q, got %q", tt.wantName, filter.Field.Name)
			}
			if tt.wantValue != "" && filter.Value != tt.wantValue {
				t.Errorf("Value: want %q, got %q", tt.wantValue, filter.Value)
			}
		})
	}
}

func TestCondition_Nested(t *testing.T) {
	filter, err := NewCondition("(a = 1 and b = 2)", "")
	if err != nil {
		t.Fatalf("NewCondition error: %v", err)
	}
	if len(filter.Children) != 2 {
		t.Fatalf("want 2 sub-conditions, got %d", len(filter.Children))
	}
	if filter.Children[0].LogicOp != "" {
		t.Errorf("first LogicOp: want empty, got %q", filter.Children[0].LogicOp)
	}
	if filter.Children[1].LogicOp != "and" {
		t.Errorf("second LogicOp: want 'and', got %q", filter.Children[1].LogicOp)
	}
}

func TestExtractWhere_Remaining(t *testing.T) {
	// With GROUP BY following
	conditions, remaining, err := ExtractWhere("where a = 1 group by b")
	if err != nil {
		t.Fatalf("ExtractWhere error: %v", err)
	}
	if len(conditions) != 1 {
		t.Errorf("want 1 condition, got %d", len(conditions))
	}
	if remaining != "group by b" {
		t.Errorf("remaining: want %q, got %q", "group by b", remaining)
	}

	// With ORDER BY following
	conditions, remaining, err = ExtractWhere("where a = 1 order by b desc")
	if err != nil {
		t.Fatalf("ExtractWhere error: %v", err)
	}
	if len(conditions) != 1 {
		t.Errorf("want 1 condition, got %d", len(conditions))
	}
	if remaining != "order by b desc" {
		t.Errorf("remaining: want %q, got %q", "order by b desc", remaining)
	}

	// Simple where only
	conditions, remaining, err = ExtractWhere("where a = 1 and b = 2")
	if err != nil {
		t.Fatalf("ExtractWhere error: %v", err)
	}
	if len(conditions) != 2 {
		t.Errorf("want 2 conditions, got %d", len(conditions))
	}
	if remaining != "" {
		t.Errorf("remaining: want empty, got %q", remaining)
	}
}

func TestParse_ErrorOnBadSQL(t *testing.T) {
	for _, sql := range []string{"", "foo", "create table users (id int)"} {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err == nil {
				t.Errorf("expected error for %q", sql)
			}
		})
	}
}
