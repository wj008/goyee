package dbs

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
	"time"
	"yee/config"
)

type H = map[string]interface{}
type A = []interface{}

type DB struct {
	*sql.DB
}
type Tx struct {
	*sql.Tx
}

var mainDb *DB

func Raw(sql string, args ...interface{}) *Frame {
	return &Frame{
		Sql:  sql,
		Args: args,
		Typ:  "raw",
	}
}

/**
获取主数据库
*/
func Db() (*DB, error) {
	if mainDb != nil {
		return mainDb, nil
	}
	userName := config.String("db_username", "root")
	password := config.String("db_password", "")
	host := config.String("db_host", "127.0.0.1")
	port := config.String("db_port", "3306")
	dbName := config.String("db_dbname", "test")
	charset := config.String("db_charset", "utf8")
	maxLifetime := config.Int("db_max_lifetime", 100)
	poolSize := config.Int("db_pool_size", 1)
	path := strings.Join([]string{userName, ":", password, "@tcp(", host, ":", port, ")/", dbName, "?charset=", charset, "&parseTime=True"}, "")
	db, _ := sql.Open("mysql", path)
	//设置数据库超时时间
	db.SetConnMaxLifetime(time.Duration(maxLifetime) * time.Second)
	db.SetMaxOpenConns(poolSize)
	//设置上数据库最大闲置连接数
	db.SetMaxIdleConns(1)
	//验证连接
	if err := db.Ping(); err != nil {
		log.Println("打开数据库失败", err.Error())
		return nil, err
	}
	mainDb = &DB{DB: db}
	return mainDb, nil
}

/**
查询多行
*/
func (db *DB) Query(sql string, args ...interface{}) ([]H, error) {
	rows, err := db.DB.Query(sql, args...)
	if err != nil {
		log.Println(sql, err.Error())
		log.Println(args...)
		return nil, err
	}
	return fetch(rows)
}

/**
查询1行
*/
func (db *DB) QueryRow(sql string, args ...interface{}) (H, error) {
	rows, err := db.DB.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	list, err := fetch(rows)
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return nil, nil
}

/**
插入数据集
*/
func (db *DB) Insert(table string, data H) (sql.Result, error) {
	var names []string
	var temps []string
	var values []interface{}
	for key, value := range data {
		names = append(names, "`"+key+"`")
		switch value.(type) {
		case *Frame:
			temps = append(temps, value.(*Frame).Format())
			break
		default:
			temps = append(temps, "?")
			values = append(values, value)
			break
		}
	}
	if len(names) == 0 {
		return nil, errors.New("插入失败，没有相应的数据")
	}
	sql := "insert into `" + table + "` (" + strings.Join(names, ",") + ") values (" + strings.Join(temps, ",") + ")"
	return db.Exec(sql, values...)
}

/**
替换数据集
*/
func (db *DB) Replace(table string, data H) (sql.Result, error) {
	var names []string
	var temps []string
	var values []interface{}
	for key, value := range data {
		names = append(names, "`"+key+"`")
		switch value.(type) {
		case *Frame:
			temps = append(temps, value.(*Frame).Format())
			break
		default:
			temps = append(temps, "?")
			values = append(values, value)
			break
		}
	}
	if len(names) == 0 {
		return nil, errors.New("插入失败，没有相应的数据")
	}
	sql := "replace into `" + table + "` (" + strings.Join(names, ",") + ") values (" + strings.Join(temps, ",") + ")"
	return db.Exec(sql, values...)
}

/**
更新数据集合
*/
func (db *DB) Update(table string, data H, where interface{}, args ...interface{}) (sql.Result, error) {
	var names []string
	var values []interface{}
	var temps []interface{}
	whereSql := ""
	switch where.(type) {
	case int, int64, int32, uint32, uint64:
		temps = append(temps, where)
		whereSql = "id=?"
		break
	default:
		whereSql = where.(string)
		break
	}
	if len(whereSql) == 0 {
		return nil, errors.New("更新，缺少查询语句")
	}
	for key, value := range data {
		switch value.(type) {
		case *Frame:
			names = append(names, "`"+key+"`="+value.(*Frame).Format())
			break
		default:
			names = append(names, "`"+key+"`=?")
			values = append(values, value)
			break
		}
	}
	if len(names) == 0 {
		return nil, errors.New("更新，没有相应的数据")
	}
	values = append(values, temps...)
	values = append(values, args...)
	sql := "update `" + table + "` set " + strings.Join(names, ",") + " where " + whereSql
	return db.Exec(sql, values...)
}

/**
删除数据
*/
func (db *DB) Delete(table string, where interface{}, args ...interface{}) (sql.Result, error) {
	var temps []interface{}
	whereSql := ""
	switch where.(type) {
	case int, int64, int32, uint32, uint64:
		temps = append(temps, where)
		whereSql = "id=?"
		break
	default:
		whereSql = where.(string)
		break
	}
	if len(whereSql) == 0 {
		return nil, errors.New("更新，缺少查询语句")
	}
	temps = append(temps, args...)
	sql := "delete from `" + table + "` where " + whereSql
	return db.Exec(sql, temps...)
}

/**
开启事务
*/
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	ntx := &Tx{Tx: tx}
	return ntx, nil
}

/**
在事务中执行
*/
func (db *DB) Transaction(fn func(*Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	err = fn(tx)
	if err != nil {
		tx.Rollback()
		return
	}
	tx.Commit()
	return
}

/**
查询多行
*/
func (tx *Tx) Query(sql string, args ...interface{}) ([]H, error) {
	rows, err := tx.Tx.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	return fetch(rows)
}

/**
查询1行
*/
func (tx *Tx) QueryRow(sql string, args ...interface{}) (H, error) {
	rows, err := tx.Tx.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	list, err := fetch(rows)
	if err != nil {
		return nil, err
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return nil, nil
}

func (tx *Tx) LastInsertId() (int, error) {
	rows, err := tx.Tx.Query("SELECT LAST_INSERT_ID()")
	if err != nil {
		return 0, err
	}
	list, err := fetch(rows)
	if err != nil {
		return 0, err
	}
	if len(list) > 0 {
		return list[0]["LAST_INSERT_ID()"].(int), nil
	}
	return 0, nil
}

/**
插入数据集
*/
func (tx *Tx) Insert(table string, data H) (sql.Result, error) {
	var names []string
	var temps []string
	var values []interface{}
	for key, value := range data {
		names = append(names, "`"+key+"`")
		switch value.(type) {
		case *Frame:
			temps = append(temps, value.(*Frame).Format())
			break
		default:
			temps = append(temps, "?")
			values = append(values, value)
			break
		}
	}
	if len(names) == 0 {
		return nil, errors.New("插入失败，没有相应的数据")
	}
	sql := "insert into `" + table + "` (" + strings.Join(names, ",") + ") values (" + strings.Join(temps, ",") + ")"
	return tx.Exec(sql, values...)
}

/**
替换数据集
*/
func (tx *Tx) Replace(table string, data H) (sql.Result, error) {
	var names []string
	var temps []string
	var values []interface{}
	for key, value := range data {
		names = append(names, "`"+key+"`")
		switch value.(type) {
		case *Frame:
			temps = append(temps, value.(*Frame).Format())
			break
		default:
			temps = append(temps, "?")
			values = append(values, value)
			break
		}
	}
	if len(names) == 0 {
		return nil, errors.New("插入失败，没有相应的数据")
	}
	sql := "replace into `" + table + "` (" + strings.Join(names, ",") + ") values (" + strings.Join(temps, ",") + ")"
	//log.Println(Sql)
	return tx.Exec(sql, values...)
}

/**
更新数据集合
*/
func (tx *Tx) Update(table string, data H, where interface{}, args ...interface{}) (sql.Result, error) {
	var names []string
	var values []interface{}
	var temps []interface{}
	whereSql := ""
	switch where.(type) {
	case int, int64, int32, uint32, uint64:
		temps = append(temps, where)
		whereSql = "id=?"
		break
	default:
		whereSql = where.(string)
		break
	}
	if len(whereSql) == 0 {
		return nil, errors.New("更新，缺少查询语句")
	}
	for key, value := range data {
		switch value.(type) {
		case *Frame:
			names = append(names, "`"+key+"`="+value.(*Frame).Format())
			break
		default:
			names = append(names, "`"+key+"`=?")
			values = append(values, value)
			break
		}
	}
	if len(names) == 0 {
		return nil, errors.New("更新，没有相应的数据")
	}
	values = append(values, temps...)
	values = append(values, args...)
	sql := "update `" + table + "` set " + strings.Join(names, ",") + " where " + whereSql
	return tx.Exec(sql, values...)
}

/**
删除数据
*/
func (tx *Tx) Delete(table string, where interface{}, args ...interface{}) (sql.Result, error) {
	var temps []interface{}
	whereSql := ""
	switch where.(type) {
	case int, int64, int32, uint32, uint64:
		temps = append(temps, where)
		whereSql = "id=?"
		break
	default:
		whereSql = where.(string)
		break
	}
	if len(whereSql) == 0 {
		return nil, errors.New("更新，缺少查询语句")
	}
	temps = append(temps, args...)
	sql := "delete from `" + table + "` where " + whereSql
	return tx.Exec(sql, temps...)
}

/**
遍历数据
*/
func fetch(rows *sql.Rows) ([]H, error) {
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength)
	if err != nil {
		return nil, err
	}
	for index, column := range columns {
		typeName := column.DatabaseTypeName()
		if typeName == "INT" ||
			typeName == "TINYINT" ||
			typeName == "INTEGER" ||
			typeName == "BIGINT" ||
			typeName == "SMALLINT" ||
			typeName == "MEDIUMINT" {
			var a sql.NullInt64
			cache[index] = &a
		} else if typeName == "FLOAT" ||
			typeName == "DOUBLE" ||
			typeName == "DECIMAL" {
			var a sql.NullFloat64
			cache[index] = &a
		} else if typeName == "SMALLDATETIME" ||
			typeName == "DATETIME" ||
			typeName == "DATE" {
			var a sql.NullTime
			cache[index] = &a
		} else {
			var a sql.NullString
			cache[index] = &a
		}
	}
	list := make([]H, 0)
	for rows.Next() {
		err = rows.Scan(cache...)
		if err != nil {
			return nil, err
		}
		item := make(H)
		for i, data := range cache {
			key := columns[i].Name()
			switch data.(type) {
			case *sql.NullString:
				a := *data.(*sql.NullString)
				if a.Valid {
					item[key] = a.String
				} else {
					item[key] = ""
				}
				break
			case *sql.NullInt64:
				a := *data.(*sql.NullInt64)
				if a.Valid {
					item[key] = int(a.Int64)
				} else {
					item[key] = 0
				}
				break
			case *sql.NullTime:
				a := *data.(*sql.NullTime)
				if a.Valid {
					item[key] = a.Time
				} else {
					item[key] = time.Time{}
				}
				break
			case *sql.NullFloat64:
				a := *data.(*sql.NullFloat64)
				if a.Valid {
					item[key] = a.Float64
				} else {
					item[key] = float64(0)
				}
				break
			default:
				item[key] = *data.(*interface{})
				break
			}
		}
		list = append(list, item)
	}
	return list, nil
}
