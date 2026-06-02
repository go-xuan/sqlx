package utils

import (
	"strings"
	"testing"

	"github.com/go-xuan/sqlx/consts"
)

func TestCollapseSql(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{"removes newlines", "select *\nfrom\nusers", "select * from users"},
		{"removes extra spaces", "select   *    from   users", "select * from users"},
		{"trims leading/trailing spaces", "  select * from users  ", "select * from users"},
		{"trims trailing semicolon", "select * from users;", "select * from users"},
		{"trims semicolon with spaces", "  select * from users;  ", "select * from users"},
		{"handles complex sql", "  select a,\n       b\n  from t\n  where c = 1;  ", "select a, b from t where c = 1"},
		{"no semicolon", "select 1", "select 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CollapseSql(tt.input); got != tt.want {
				t.Errorf("CollapseSql(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractAlias(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{"lowercase AS", "tbl as t", "t"},
		{"implicit alias", "tbl t", "t"},
		{"no alias", "tbl", "tbl"},
		{"trimmed input", "  tbl as t  ", "t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractAlias(tt.input); got != tt.want {
				t.Errorf("ExtractAlias(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseValuesInSql(t *testing.T) {
	t.Run("with string values", func(t *testing.T) {
		sql := "select * from users where name = 'Alice' and city = 'NYC'"
		newSql, replacer := ParseValuesInSql(sql)
		if replacer == nil {
			t.Fatal("expected non-nil replacer")
		}
		if strings.Contains(newSql, "'Alice'") {
			t.Error("SQL should not contain raw string values")
		}
		restored := replacer.Replace(newSql)
		if restored != sql {
			t.Errorf("restored SQL = %q, want %q", restored, sql)
		}
	})
	t.Run("no string values", func(t *testing.T) {
		sql := "select * from users where id = 1"
		newSql, replacer := ParseValuesInSql(sql)
		if replacer != nil {
			t.Error("expected nil replacer for SQL without string values")
		}
		if newSql != sql {
			t.Errorf("SQL should be unchanged, got %q", newSql)
		}
	})
}

func TestAllKeywordsToLower(t *testing.T) {
	input := "SELECT * FROM Users WHERE ID = 1 ORDER BY Name ASC"
	got := AllKeywordsToLower(input)
	for _, kw := range []string{"SELECT", "FROM", "WHERE", "ORDER", "BY", "ASC"} {
		if strings.Contains(got, kw) {
			t.Errorf("keyword %q should be lowercased in: %s", kw, got)
		}
	}
}

func TestSplitValuesSql(t *testing.T) {
	tests := []struct {
		name, input string
		wantLen     int
		wantFirst   string
	}{
		{"simple values", "(1, 2, 3)", 3, "1"},
		{"with parens", "(1, 2, func(123))", 3, "1"},
		{"with strings", `('a', 'b', 'c')`, 3, "'a'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SplitValuesSql(tt.input)
			if len(got) != tt.wantLen {
				t.Errorf("SplitValuesSql(%q) len = %d, want %d", tt.input, len(got), tt.wantLen)
			}
			if len(got) > 0 && got[0] != tt.wantFirst {
				t.Errorf("SplitValuesSql(%q)[0] = %q, want %q", tt.input, got[0], tt.wantFirst)
			}
		})
	}
}

func TestSplitExcludeInBracket(t *testing.T) {
	tests := []struct {
		name, sql, sep string
		wantParts      int // total parts (slice + last)
	}{
		{"simple split", "a, b, c", consts.Comma, 3},
		{"with brackets", "(1, 2), (3, 4)", consts.Comma, 2},
		{"mixed", "a, func(b, c), d", consts.Comma, 3},
		{"nested parens", "now(), interval '8 hour'", consts.Comma, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slice, last := SplitExcludeInBracket(tt.sql, tt.sep)
			got := len(slice) + 1
			if got != tt.wantParts {
				t.Errorf("SplitExcludeInBracket(%q, %q) parts = %d, want %d (slice=%v, last=%q)",
					tt.sql, tt.sep, got, tt.wantParts, slice, last)
			}
		})
	}
}

func TestIndexExcludeBrackets(t *testing.T) {
	t.Run("keyword outside brackets", func(t *testing.T) {
		idx := IndexExcludeBrackets("a = 1 and (b = 2 and c = 3)", consts.AND, false)
		if idx < 0 {
			t.Error("should find 'and' outside brackets")
		}
	})
	t.Run("keyword inside brackets excluded", func(t *testing.T) {
		// "and" inside brackets counts as bracket content
		idx := IndexExcludeBrackets("a = 1 and (b = 2 and c = 3)", consts.AND, true)
		if idx < 0 {
			t.Error("should find 'and' (the one before the bracket)")
		}
	})
}

func TestContainsKeywords(t *testing.T) {
	hit, idx := ContainsKeywords("select * from t where a = 1 order by id", consts.WHERE, consts.FROM)
	if hit != consts.FROM {
		t.Errorf("earliest keyword should be FROM, got %q", hit)
	}
	if idx < 0 {
		t.Error("should find FROM")
	}
}

func TestFirstIndexOfKeys(t *testing.T) {
	hit, idx := FirstIndexOfKeys("select * from t where a = 1", consts.WHERE, consts.FROM)
	if hit != consts.FROM {
		t.Errorf("first keyword should be FROM, got %q", hit)
	}
	if idx < 0 {
		t.Error("should find FROM index")
	}
}

func TestLastIndexOfKeys(t *testing.T) {
	hit, idx := LastIndexOfKeys("select * from t where a = 1 order by id", consts.WHERE, consts.FROM, consts.ORDERBY)
	if hit != consts.ORDERBY {
		t.Errorf("last keyword should be ORDER BY, got %q", hit)
	}
	if idx < 0 {
		t.Error("should find ORDER BY index")
	}
}

func TestIndicesOfKeyword(t *testing.T) {
	sql := "select 1 from t where a = 1 and b = 2 and c = 3"
	indices := IndicesOfKeyword(sql, consts.AND, 2)
	if len(indices) != 2 {
		t.Fatalf("expected 2 occurrences of AND, got %d", len(indices))
	}
	// verify indices are correct by checking the substrings
	for _, idx := range indices {
		if sql[idx:idx+len(consts.AND)] != consts.AND {
			t.Errorf("index %d does not point to AND", idx)
		}
	}
}

func TestIndexOfKeyword(t *testing.T) {
	sql := "select 1 from t where a = 1 and b = 2 and c = 3 and d = 4"

	t.Run("1st occurrence", func(t *testing.T) {
		idx := IndexOfKeyword(sql, consts.AND, 1)
		if idx < 0 || sql[idx:idx+len(consts.AND)] != consts.AND {
			t.Error("should find 1st AND")
		}
	})
	t.Run("3rd occurrence", func(t *testing.T) {
		idx := IndexOfKeyword(sql, consts.AND, 3)
		if idx < 0 || sql[idx:idx+len(consts.AND)] != consts.AND {
			t.Error("should find 3rd AND")
		}
	})
	t.Run("out of range", func(t *testing.T) {
		idx := IndexOfKeyword(sql, consts.AND, 10)
		if idx != -1 {
			t.Error("should return -1 for out-of-range position")
		}
	})
	t.Run("position 0 invalid", func(t *testing.T) {
		idx := IndexOfKeyword(sql, consts.AND, 0)
		if idx != -1 {
			t.Error("should return -1 for position 0")
		}
	})
}

func TestIndexOfKeywordFirst(t *testing.T) {
	t.Run("finds keyword", func(t *testing.T) {
		idx := IndexOfKeywordFirst("select * from t where a = 1", consts.WHERE)
		if idx < 0 {
			t.Error("should find WHERE")
		}
	})
	t.Run("skips non-keyword matches", func(t *testing.T) {
		// "band" contains "and" but is not a keyword
		idx := IndexOfKeywordFirst("band = 1 and c = 2", consts.AND)
		if idx < 0 {
			t.Error("should find standalone AND, skipping 'band'")
		}
	})
	t.Run("not found", func(t *testing.T) {
		idx := IndexOfKeywordFirst("select * from t", consts.WHERE)
		if idx != -1 {
			t.Error("should return -1 when keyword not found")
		}
	})
}

func TestIndexOfKeywordLast(t *testing.T) {
	t.Run("finds last keyword", func(t *testing.T) {
		idx := IndexOfKeywordLast("select * from t where a = 1 and b = 2 and c = 3", consts.AND)
		if idx < 0 {
			t.Error("should find last AND")
		}
	})
	t.Run("not found", func(t *testing.T) {
		idx := IndexOfKeywordLast("select * from t", consts.WHERE)
		if idx != -1 {
			t.Error("should return -1 when keyword not found")
		}
	})
	t.Run("single occurrence", func(t *testing.T) {
		idx := IndexOfKeywordLast("select * from t where a = 1", consts.WHERE)
		if idx < 0 {
			t.Error("should find single WHERE")
		}
	})
}

func TestHasAdjacent(t *testing.T) {
	t.Run("surrounded by spaces", func(t *testing.T) {
		if !HasAdjacent("a and b", "and", consts.Blank, 2) {
			t.Error("'and' at index 2 in 'a and b' should be adjacent to spaces")
		}
	})
	t.Run("at start with space after", func(t *testing.T) {
		if !HasAdjacent("and b", "and", consts.Blank, 0) {
			t.Error("'and' at index 0 with space after should pass")
		}
	})
	t.Run("at end with space before", func(t *testing.T) {
		if !HasAdjacent("a and", "and", consts.Blank, 2) {
			t.Error("'and' at end with space before should pass")
		}
	})
	t.Run("not a keyword (no spaces)", func(t *testing.T) {
		if HasAdjacent("band", "and", consts.Blank, 1) {
			t.Error("'and' inside 'band' should not be adjacent to spaces")
		}
	})
}

func TestBetweenOfString(t *testing.T) {
	t.Run("matching pair", func(t *testing.T) {
		from, to := BetweenOfString("(hello)", consts.LeftBracket, consts.RightBracket)
		if from != 0 || to != 6 {
			t.Errorf("BetweenOfString = (%d, %d), want (0, 6)", from, to)
		}
	})
	t.Run("nested brackets", func(t *testing.T) {
		from, to := BetweenOfString("a(b(c)d)e", consts.LeftBracket, consts.RightBracket)
		if from != 1 || to != 7 {
			t.Errorf("BetweenOfString = (%d, %d), want (1, 7)", from, to)
		}
	})
	t.Run("no match", func(t *testing.T) {
		from, to := BetweenOfString("no brackets", consts.LeftBracket, consts.RightBracket)
		if from != -1 || to != -1 {
			t.Errorf("BetweenOfString = (%d, %d), want (-1, -1)", from, to)
		}
	})
	t.Run("same start and end", func(t *testing.T) {
		from, to := BetweenOfString("a'b'c", "'", "'")
		if from != 1 || to != 3 {
			t.Errorf("BetweenOfString with same delimiters = (%d, %d), want (1, 3)", from, to)
		}
	})
}

func TestIndexOfString(t *testing.T) {
	t.Run("first occurrence (default)", func(t *testing.T) {
		idx := IndexOfString("a,b,c", consts.Comma)
		if idx != 1 {
			t.Errorf("IndexOfString = %d, want 1", idx)
		}
	})
	t.Run("explicit position 1", func(t *testing.T) {
		idx := IndexOfString("a,b,c", consts.Comma, 1)
		if idx != 1 {
			t.Errorf("IndexOfString pos=1 = %d, want 1", idx)
		}
	})
	t.Run("position 2", func(t *testing.T) {
		idx := IndexOfString("a,b,c", consts.Comma, 2)
		if idx != 3 {
			t.Errorf("IndexOfString pos=2 = %d, want 3", idx)
		}
	})
	t.Run("reverse last", func(t *testing.T) {
		idx := IndexOfString("a,b,c", consts.Comma, -1)
		if idx != 3 {
			t.Errorf("IndexOfString pos=-1 = %d, want 3", idx)
		}
	})
	t.Run("not found", func(t *testing.T) {
		idx := IndexOfString("abc", consts.Comma)
		if idx != -1 {
			t.Errorf("IndexOfString = %d, want -1", idx)
		}
	})
}

func TestTrimBrackets(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{"no brackets", "hello", "hello"},
		{"single layer", "(hello)", "hello"},
		{"double layer", "((hello))", "hello"},
		{"not wrapping", "(a) and (b)", "(a) and (b)"},
		{"spaces inside", "( hello )", " hello "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TrimBrackets(tt.input); got != tt.want {
				t.Errorf("TrimBrackets(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCutString(t *testing.T) {
	t.Run("cut at comma", func(t *testing.T) {
		left, right := CutString("a,b", consts.Comma)
		if left != "a" || right != "b" {
			t.Errorf("CutString = (%q, %q), want (%q, %q)", left, right, "a", "b")
		}
	})
	t.Run("not found", func(t *testing.T) {
		left, right := CutString("abc", consts.Comma)
		if left != "abc" || right != "" {
			t.Errorf("CutString = (%q, %q), want (%q, %q)", left, right, "abc", "")
		}
	})
}

func TestIfNextLine(t *testing.T) {
	t.Run("should wrap by count", func(t *testing.T) {
		if !IfNextLine([]string{"a", "b", "c", "d", "e", "f", "g"}, 5, 200) {
			t.Error("7 values with size=5 should trigger newline")
		}
	})
	t.Run("should wrap by length", func(t *testing.T) {
		if !IfNextLine([]string{"verylongvalue"}, 10, 5) {
			t.Error("long value exceeding max should trigger newline")
		}
	})
	t.Run("should not wrap", func(t *testing.T) {
		if IfNextLine([]string{"a", "b"}, 5, 200) {
			t.Error("2 values with size=5 should not trigger newline")
		}
	})
}
