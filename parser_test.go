package sqlx

import (
	"fmt"
	"testing"
)

func TestSqlBeautifier(t *testing.T) {
	//fmt.Println(Parse(`SELECT '123' as qqq, '456', '789', good(t1.aaa, ',') as aaa, t1.bbb, good(t1.ccc, ',') as ccc
	//FROM t_abc_2 as t1 left join t_abc_2 t2 on t1.aaa = t2.aaa right join (select aaa,bbb,ccc from t_abc_3 where aaa = '1') t2 on t2.aaa = t3.aaa
	//where t1.aaa = '1' and (t2.bbb = '1' or t3.ccc = '1') group by t1.aaa, t2.bbb, t3.ccc having t1.aaa > 1 and (t2.bbb <= 1 or t3.ccc = 0)
	//order by t1.aaa ASC, t2.bbb DESC, t3.ccc ASC limit 10 offset 20`).Beautify())

	fmt.Println(Parse(`update quanchao_test set name = 'quanchao', sss = 123 where id = 1 and d = true`).Beautify())
}
