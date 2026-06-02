package convert

import "github.com/go-xuan/sqlx/model"

// QueryConvert 查询转换器
type QueryConvert struct {
	Table   *model.Table
	Fields  []*model.Field
	Filters []*model.Filter
	Orders  []*model.Order
	Page    *model.Page
}
