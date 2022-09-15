package dbs

import (
	"regexp"
	"strings"
)

type SearchType int32

const (
	WithoutEmpty SearchType = 0
	WithoutZero             = 1
	WithoutNil              = 2
)

type Condition struct {
	typ   string
	items []*Frame
}

/*
*
创建查询条件
*/
func NewCondition() *Condition {
	return &Condition{
		typ:   "and",
		items: make([]*Frame, 0),
	}
}

/*
*
查询条件
*/
func (cond *Condition) Where(sql string, args ...any) *Condition {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return cond
	}
	item := NewFrame(sql, "where", args...)
	cond.items = append(cond.items, item)
	return cond
}

func (cond *Condition) WhereC(c *Condition) *Condition {
	frame := c.GetFrame()
	if frame.Sql == "" {
		return cond
	}
	tempSql := frame.Sql
	tempArgs := frame.Args
	reg, _ := regexp.Compile(`(?i)^(or|and)\s+`)
	if reg.MatchString(tempSql) {
		tempSql = reg.ReplaceAllString(tempSql, "")
	}
	if frame.Typ == "" {
		item := NewFrame("("+tempSql+")", "where", tempArgs...)
		cond.items = append(cond.items, item)
	} else {
		item := NewFrame(frame.Typ+" ("+tempSql+")", "where", tempArgs...)
		cond.items = append(cond.items, item)
	}
	return cond
}

func (cond *Condition) Search(sql string, value any, typ SearchType) *Condition {
	isA := false
	if typ == WithoutZero {
		switch value.(type) {
		case nil:
			return cond
		case string:
			if value == "" {
				return cond
			}
		case int64:
			if value.(int64) == 0 {
				return cond
			}
		case uint64:
			if value.(uint64) == 0 {
				return cond
			}
		case int32:
			if value.(int32) == 0 {
				return cond
			}
		case uint32:
			if value.(uint32) == 0 {
				return cond
			}
		case int:
			if value.(int) == 0 {
				return cond
			}
		case uint:
			if value.(uint) == 0 {
				return cond
			}
		case float64:
			if value.(float64) == 0 {
				return cond
			}
		case float32:
			if value.(float32) == 0 {
				return cond
			}
		case bool:
			if value.(bool) == false {
				return cond
			}
		case []any:
			isA = true
			if len(value.([]any)) == 0 {
				return cond
			}
			break
		default:
			return cond
		}
	} else if typ == WithoutEmpty {
		switch value.(type) {
		case nil:
			return cond
		case string:
			if value.(string) == "" {
				return cond
			}
			break
		case int64, uint64, int32, uint32, int, uint, float64, float32, bool:
			break
		case []any:
			isA = true
			if len(value.([]any)) == 0 {
				return cond
			}
			break
		default:
			return cond
		}
	} else if typ == WithoutNil {
		switch value.(type) {
		case nil:
			return cond
		case string, int64, uint64, int32, uint32, int, uint, float64, float32, bool:
			break
		case []any:
			isA = true
			if len(value.([]any)) == 0 {
				return cond
			}
			break
		default:
			return cond
		}
	}
	if isA {
		if strings.Count(sql, "[?]") != 1 {
			return cond
		}
		ques := make([]string, 0)
		args := value.([]any)
		aLen := len(args)
		for i := 0; i < aLen; i++ {
			ques = append(ques, "?")
		}
		sql = strings.Replace(sql, "[?]", strings.Join(ques, ","), 1)
		return cond.Where(sql, args...)
	}
	return cond.Where(sql, value)
}

func (cond *Condition) GetFrame() *Frame {
	sqlItems := make([]string, 0)
	argItems := make([]any, 0)
	for _, item := range cond.items {
		tempSql := item.Sql
		reg, _ := regexp.Compile(`(?i)^(or|and)\s+`)
		if reg.MatchString(tempSql) {
			if len(sqlItems) == 0 {
				tempSql = reg.ReplaceAllString(tempSql, "")
			}
		} else {
			if len(sqlItems) > 0 {
				tempSql = "and " + tempSql
			}
		}
		sqlItems = append(sqlItems, tempSql)
		argItems = append(argItems, item.Args...)
	}
	return &Frame{
		Sql:  strings.Join(sqlItems, " "),
		Args: argItems,
		Typ:  cond.typ,
	}
}

func (cond *Condition) Empty() *Condition {
	cond.items = make([]*Frame, 0)
	return cond
}
