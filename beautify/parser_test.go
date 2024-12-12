package beautify

import (
	"fmt"
	"testing"
)

func TestSelectBeautify(t *testing.T) {
	sql := `SELECT '123' as qqq, '456', '789', good(t1.aaa, ',') as aaa,"user_from", t1.bbb, good(t1.ccc, ',') as ccc
	FROM t_table_1 as t1 left join t_table_2 t2 on t1.aaa = t2.aaa right join (select aaa,bbb,ccc from t_table_4 where aaa = '1') t2 on t2.aaa = t3.aaa
	where t1.aaa = '1' and (t2.bbb = '1' or t3.ccc = '1') and t1.ddd in ('111111111','222222','33333') and t1.eee in ('111111111','222222','33333','44444') 
	and t1.fff in ( select id from t_table_4)
	group by t1.aaa, t2.bbb, t3.ccc having t1.aaa > 1 and (t2.bbb <= 1 or t3.ccc = 0)
	order by t1.aaa ASC, t2.bbb DESC, t3.ccc ASC limit 10 offset 20`
	parser := Parse(sql)
	parser.Beautify()
	fmt.Println(parser.Beautify())
}

func TestUpdateBeautify(t *testing.T) {
	fmt.Println(Parse(`update quanchao_test set name = 'quanchao', sss = 123 where id = 1 and d = true`).Beautify())
}

func TestInsertBeautify(t *testing.T) {
	fmt.Println(Parse(`insert into quanchao_test (aaa,bbb,ccc,ddd) values (101,102,103,104),(201,202,203,204),(301,302,303,304);`).Beautify())
	fmt.Println(Parse(`insert into quanchao_test (aaa,bbb,ccc,ddd) select aaa,bbb,ccc,ddd from sssss_fff`).Beautify())
}
