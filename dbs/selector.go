package dbs

import (
	"regexp"
	"strconv"
	"strings"
)

type PageInfo struct {
	Page      int `json:"page"`
	PageSize  int `json:"page_size"`
	Count     int `json:"count"`
	PageCount int `json:"page_count"`
}

type Selector struct {
	*Condition
	*PageInfo
	db     *DB
	table  string
	limit  string
	fields *Frame
	orders *Frame
	groups *Frame
	having *Condition
	joins  *Frame
	unions []*Frame
}

func NewSelector(db *DB, table string) *Selector {
	info := &PageInfo{
		Page:      1,
		Count:     -1,
		PageSize:  20,
		PageCount: 0,
	}
	selector := &Selector{
		db:        db,
		Condition: NewCondition(),
		PageInfo:  info,
		table:     strings.TrimSpace(table),
	}
	return selector
}

func DBSelector(table string) (*Selector, error) {
	db, err := Db()
	if err != nil {
		return nil, err
	}
	return NewSelector(db, table), nil
}

func (slt *Selector) Field(fields string, args ...interface{}) *Selector {
	fields = strings.TrimSpace(fields)
	if fields == "" {
		return slt
	}
	slt.fields = NewFrame(fields, "field", args...)
	return slt
}
func (slt *Selector) Order(order string, args ...interface{}) *Selector {
	order = strings.TrimSpace(order)
	reg, _ := regexp.Compile(`(?i)^(by\s+|,)`)
	order = reg.ReplaceAllString(order, "")
	order = strings.TrimSpace(order)
	if order == "" {
		return slt
	}
	if slt.orders == nil {
		slt.orders = NewFrame("order by "+order, "order", args...)
	} else {
		slt.orders.Add(","+order, args...)
	}
	return slt
}
func (slt *Selector) Limit(offset int, size int) *Selector {
	if offset == 0 && size == 0 {
		slt.limit = ""
		return slt
	}
	if size == 0 {
		slt.limit = "limit " + strconv.Itoa(offset)
	} else {
		slt.limit = "limit " + strconv.Itoa(offset) + "," + strconv.Itoa(size)
	}
	return slt
}
func (slt *Selector) Group(group string, args ...interface{}) *Selector {
	group = strings.TrimSpace(group)
	reg, _ := regexp.Compile(`(?i)^(by\s+|,)`)
	group = reg.ReplaceAllString(group, "")
	group = strings.TrimSpace(group)
	if group == "" {
		return slt
	}
	if slt.groups == nil {
		slt.groups = NewFrame("group by "+group, "group", args...)
	} else {
		slt.groups.Add(","+group, args...)
	}
	return slt
}
func (slt *Selector) Having(sql string, args ...interface{}) *Selector {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return slt
	}
	if slt.having == nil {
		slt.having = NewCondition()
	}
	slt.having.Where(sql, args)
	return slt
}
func (slt *Selector) LeftJoin(table string, args ...interface{}) *Selector {
	table = strings.TrimSpace(table)
	if table == "" {
		return slt
	}
	sql := "left join " + table
	if slt.joins == nil {
		slt.joins = NewFrame(sql, "join", args...)
	} else {
		slt.joins.Add(sql, args...)
	}
	return slt
}
func (slt *Selector) RightJoin(table string, args ...interface{}) *Selector {
	table = strings.TrimSpace(table)
	if table == "" {
		return slt
	}
	sql := "right join " + table
	if slt.joins == nil {
		slt.joins = NewFrame(sql, "join", args...)
	} else {
		slt.joins.Add(sql, args...)
	}
	return slt
}
func (slt *Selector) InnerJoin(table string, args ...interface{}) *Selector {
	table = strings.TrimSpace(table)
	if table == "" {
		return slt
	}
	sql := "inner join " + table
	if slt.joins == nil {
		slt.joins = NewFrame(sql, "join", args...)
	} else {
		slt.joins.Add(sql, args...)
	}
	return slt
}
func (slt *Selector) OuterJoin(table string, args ...interface{}) *Selector {
	table = strings.TrimSpace(table)
	if table == "" {
		return slt
	}
	sql := "outer join " + table
	if slt.joins == nil {
		slt.joins = NewFrame(sql, "join", args...)
	} else {
		slt.joins.Add(sql, args...)
	}
	return slt
}
func (slt *Selector) FullJoin(table string, args ...interface{}) *Selector {
	table = strings.TrimSpace(table)
	if table == "" {
		return slt
	}
	sql := "full join " + table
	if slt.joins == nil {
		slt.joins = NewFrame(sql, "join", args...)
	} else {
		slt.joins.Add(sql, args...)
	}
	return slt
}
func (slt *Selector) JoinOn(sql string, args ...interface{}) *Selector {
	if slt.joins == nil {
		return slt
	}
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return slt
	}
	sql = "on " + sql
	slt.joins.Add(sql, args...)
	return slt
}
func (slt *Selector) Union(sql string, args ...interface{}) *Selector {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return slt
	}
	if slt.unions == nil {
		slt.unions = make([]*Frame, 0)
	}
	frame := &Frame{
		Sql:  sql,
		Typ:  "union",
		Args: args,
	}
	slt.unions = append(slt.unions, frame)
	return slt
}
func (slt *Selector) UnionAll(sql string, args ...interface{}) *Selector {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return slt
	}
	if slt.unions == nil {
		slt.unions = make([]*Frame, 0)
	}
	frame := &Frame{
		Sql:  sql,
		Typ:  "union-all",
		Args: args,
	}
	slt.unions = append(slt.unions, frame)
	return slt
}

/**
创建基础数据
*/
func (slt *Selector) BuildSql(optimize bool) *Frame {
	execSql := make([]string, 0)
	argItems := make([]interface{}, 0)
	reg1, _ := regexp.Compile(`\s+`)
	if reg1.MatchString(slt.table) || slt.joins != nil || slt.limit == "" || slt.groups != nil || slt.having != nil || slt.unions != nil {
		optimize = false
	}
	//字段信息
	findSql := "*"
	if slt.fields != nil && slt.fields.Sql != "" {
		findSql = slt.fields.Sql
		argItems = append(argItems, slt.fields.Args...)
	}
	if reg1.MatchString(slt.table) {
		execSql = append(execSql, "select "+findSql+" from "+slt.table)
	} else {
		execSql = append(execSql, "select "+findSql+" from `"+slt.table+"`")
	}
	//WHERE
	if slt.joins != nil && slt.joins.Sql != "" {
		execSql = append(execSql, slt.joins.Sql)
		argItems = append(argItems, slt.joins.Args...)
	}
	reg2, _ := regexp.Compile(`(?i)^(or|and)\s+`)
	if optimize {
		execSql = append(execSql, "where id in (select id from (select id from `"+slt.table+"`")
	}
	//查询条件
	frame := slt.GetFrame()
	if frame.Sql != "" {
		tempSql := frame.Sql
		if reg2.MatchString(tempSql) {
			tempSql = reg2.ReplaceAllString(tempSql, "")
		}
		execSql = append(execSql, "where "+tempSql)
		argItems = append(argItems, frame.Args...)
	}
	//GROUP BY
	if slt.groups != nil && slt.groups.Sql != "" {
		execSql = append(execSql, slt.groups.Sql)
		argItems = append(argItems, slt.groups.Args...)
	}
	//havingItem
	if slt.having != nil {
		hFrame := slt.having.GetFrame()
		if hFrame.Sql != "" {
			tempSql := hFrame.Sql
			if reg2.MatchString(tempSql) {
				tempSql = reg2.ReplaceAllString(tempSql, "")
			}
			execSql = append(execSql, "having "+tempSql)
			argItems = append(argItems, hFrame.Args...)
		}
	}
	//UNION
	if slt.unions != nil && len(slt.unions) > 0 {
		execSql = append([]string{"("}, execSql...)
		execSql = append(execSql, ")")
		for _, uFrame := range slt.unions {
			if uFrame.Typ == "union-all" {
				execSql = append(execSql, "union all ( "+uFrame.Sql+" )")
			} else {
				execSql = append(execSql, "union ( "+uFrame.Sql+" )")
			}
			argItems = append(argItems, uFrame.Args...)
		}
	}
	if slt.orders != nil && slt.orders.Sql != "" {
		execSql = append(execSql, slt.orders.Sql)
		argItems = append(argItems, slt.orders.Args...)
	}
	if slt.limit != "" {
		execSql = append(execSql, slt.limit)
	}
	if optimize {
		execSql = append(execSql, ") Z)")
		if slt.orders != nil && slt.orders.Sql != "" {
			execSql = append(execSql, slt.orders.Sql)
			argItems = append(argItems, slt.orders.Args...)
		}
	}
	return &Frame{
		Sql:  strings.Join(execSql, " "),
		Args: argItems,
		Typ:  "sql",
	}
}

/**
创建用于查询数量的语句
*/
func (slt *Selector) BuildCount() *Frame {
	if slt.groups != nil || slt.unions != nil || slt.having != nil {
		order := slt.orders
		slt.orders = nil
		limit := slt.limit
		slt.limit = ""
		item := slt.BuildSql(false)
		item.Sql = "select count(1) as mCount from (" + item.Sql + ") CountTempTable"
		slt.orders = order
		slt.limit = limit
		return item
	}
	execSql := make([]string, 0)
	argItems := make([]interface{}, 0)
	reg1, _ := regexp.Compile(`\s+`)
	if reg1.MatchString(slt.table) {
		execSql = append(execSql, "select count(1) as mCount from "+slt.table)
	} else {
		execSql = append(execSql, "select count(1) as mCount from `"+slt.table+"`")
	}
	//JOIN
	if slt.joins != nil && slt.joins.Sql != "" {
		execSql = append(execSql, slt.joins.Sql)
		argItems = append(argItems, slt.joins.Args...)
	}
	reg2, _ := regexp.Compile(`(?i)^(or|and)\s+`)
	//查询条件
	frame := slt.GetFrame()
	if frame.Sql != "" {
		tempSql := frame.Sql
		if reg2.MatchString(tempSql) {
			tempSql = reg2.ReplaceAllString(tempSql, "")
		}
		execSql = append(execSql, "where "+tempSql)
		argItems = append(argItems, frame.Args...)
	}
	return &Frame{
		Sql:  strings.Join(execSql, " "),
		Args: argItems,
		Typ:  "sql",
	}
}

/**
设置分页
*/
func (slt *Selector) SetPage(page int, pageSize int) *Selector {
	slt.Page = page
	slt.PageSize = pageSize
	if slt.Page < 1 {
		slt.Page = 1
	}
	if slt.PageSize < 1 {
		slt.PageSize = 20
	}
	return slt
}

/**
获取分页数据
*/
func (slt *Selector) GetPageInfo() (*PageInfo, error) {
	if slt.Count == -1 {
		_, err := slt.GetCount()
		if err != nil {
			return nil, err
		}
	}
	if slt.PageSize < 1 {
		slt.PageSize = 20
	}
	if slt.Page < 1 {
		slt.Page = 1
	}
	slt.PageCount = slt.Count / slt.PageSize
	if slt.Count > slt.PageCount*slt.PageSize {
		slt.PageCount += 1
	}
	return slt.PageInfo, nil
}

/**
获取分页列表
*/
func (slt *Selector) PageList() ([]H, error) {
	if slt.Page < 1 {
		slt.Page = 1
	}
	if slt.PageSize < 1 {
		slt.PageSize = 20
	}
	limit := slt.limit
	offset := (slt.Page - 1) * slt.PageSize
	slt.limit = "limit " + strconv.Itoa(offset) + "," + strconv.Itoa(slt.PageSize)
	item := slt.BuildSql(true)
	slt.limit = limit
	return slt.db.Query(item.Sql, item.Args...)
}

/**
获取数量
*/
func (slt *Selector) GetCount() (int, error) {
	count := 0
	item := slt.BuildCount()
	row, err := slt.db.QueryRow(item.Sql, item.Args...)
	if err != nil {
		return 0, err
	}
	count = row["mCount"].(int)
	slt.Count = count
	return count, nil
}

/**
获取查询结果
*/
func (slt *Selector) GetList() ([]H, error) {
	item := slt.BuildSql(true)
	return slt.db.Query(item.Sql, item.Args...)
}
