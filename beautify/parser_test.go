package beautify

import (
	"fmt"
	"testing"
)

func TestSelectBeautify(t *testing.T) {
	sql := `INSERT INTO test ("aaa","bbb","ccc","ddd","eee","fff","ggg") VALUES (1, 2,'3','4',func(123),'6', now())`
	parser := Parse(sql)
	fmt.Println(parser.Beautify())
}

func TestUpdateBeautify(t *testing.T) {
	fmt.Println(Parse(`update test set name = 'test', sss = 123 where id = 1 and d = true`).Beautify())
}

func TestInsertBeautify(t *testing.T) {
	fmt.Println(Parse(`INSERT INTO cron_task (id, "name", "type", spec, status, remark, request_api, request_method, request_url, request_headers, request_form, request_body, create_time, update_time, delete_time)
VALUES ('5e3b7fec-74ec-47f6-9e0c-1639ba18def0',
       'credits_activate_retry',
           'request',
       '@every 5m',
       1,
       'ai激活赠送积分重试（每5分钟执行一次) ',
       'order-api',
       'POST',
       '/api/back_stage/business/mg/credits/activate_retry',
       '',
       '',
       '{"source_from":"task_scheduler"}',
       now() + interval '8 hour',
       now() + interval '8 hour');`).Beautify())
	//fmt.Println(Parse(`insert into test (aaa,bbb,ccc,ddd) select aaa,bbb,ccc,ddd from test_backup`).Beautify())
}
