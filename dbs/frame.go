package dbs

import (
	"encoding/json"
	"errors"
	"github.com/wj008/goyee/config"
	"strconv"
	"strings"
	"time"
)

type Frame struct {
	Sql  string
	Args []any
	Typ  string
}

func NewFrame(sql string, typ string, args ...any) *Frame {
	return &Frame{
		Sql:  sql,
		Args: args,
		Typ:  typ,
	}
}

func (frame *Frame) Add(sql string, args ...any) *Frame {
	frame.Sql += " " + strings.TrimSpace(sql)
	if len(args) == 0 {
		return frame
	}
	if len(args) > 0 {
		frame.Args = append(frame.Args, args...)
	}
	return frame
}

func (frame *Frame) Format() string {
	str, err := Escape(frame.Sql, frame.Args...)
	if err != nil {
		return ""
	}
	return str
}

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func Escape(query string, args ...any) (string, error) {
	if strings.Count(query, "?") != len(args) {
		return "", errors.New("number of ? should be same to len(args)")
	}
	if len(args) == 0 {
		return query, nil
	}
	buf := make([]byte, 0)
	argPos := 0
	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q
		arg := args[argPos]
		argPos++
		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}
		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				localBuf := []byte(v.In(config.CstZone()).Format("2006-01-02 15:04:05"))
				buf = append(buf, localBuf[:]...)
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			buf = escapeBytesBackslash(buf, v)
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				buf = escapeBytesBackslash(buf, v)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = escapeStringBackslash(buf, v)
			buf = append(buf, '\'')
		default:
			return "", errors.New("error var type")
		}
	}
	if argPos != len(args) {
		return "", errors.New("error number of ? ")
	}
	return string(buf), nil
}
