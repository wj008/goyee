package config

import (
	"github.com/joho/godotenv"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var zone *time.Location

func init() {
	path, err := os.Executable()
	if err != nil {
		log.Fatal("Error loading app.env file")
	}
	dir := filepath.Dir(path)
	err = godotenv.Load(dir + "/app.env")
	if err != nil {
		log.Fatal("Error loading app.env file")
	}
}


/**
获取字符串
*/
func String(key string, def string) string {
	str := os.Getenv(key)
	if len(str) == 0 {
		return def
	}
	return str
}

/**
获取整数
*/
func Int(key string, def int) int {
	str := os.Getenv(key)
	if len(str) == 0 {
		return def
	}
	val, err := strconv.Atoi(str)
	if err != nil {
		return def
	}
	return val
}

func Int64(key string, def int64) int64 {
	str := os.Getenv(key)
	if len(str) == 0 {
		return def
	}
	val, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return def
	}
	return val
}

/**
获取bool
*/
func Bool(key string, def bool) bool {
	str := os.Getenv(key)
	if len(str) == 0 {
		return def
	}
	str = strings.ToLower(str)
	if str == "true" || str == "on" || str == "1" {
		return true
	}
	return false
}

/**
时区参数
*/
func CstZone() *time.Location {
	if zone != nil {
		return zone
	}
	cst := Int("cst_timezone_set", 8*3600)
	zone = time.FixedZone("CST", cst)
	return zone
}
