package beautify

import (
	"fmt"
	"testing"
)

func TestSelectBeautify(t *testing.T) {
	sql := `INSERT INTO "test" ("aaa","bbb","ccc","ddd","eee","fff","ggg") VALUES (1, 2,'3','4',func(123),'6', now());`
	parser := Parse(sql)
	fmt.Println(parser.Beautify())
}

func TestUpdateBeautify(t *testing.T) {
	fmt.Println(Parse(`update quanchao_test set name = 'quanchao', sss = 123 where id = 1 and d = true`).Beautify())
}

func TestInsertBeautify(t *testing.T) {
	fmt.Println(Parse(`insert into quanchao_test (aaa,bbb,ccc,ddd) values (101,102,103,104),(201,202,203,204),(301,302,303,304);`).Beautify())
	fmt.Println(Parse(`insert into quanchao_test (aaa,bbb,ccc,ddd) select aaa,bbb,ccc,ddd from sssss_fff`).Beautify())
}
