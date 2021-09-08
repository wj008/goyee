package tcp

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
)

type Conn struct {
	net.Conn
	Id        string
	closeFunc func()
}

type Server struct {
	net.Addr
	Connes chan *Conn
}

// NewServer 创建服务
func NewServer(addr string) (serv *Server, err error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}
	serv = &Server{
		Addr:   listener.Addr(),
		Connes: make(chan *Conn),
	}
	go func() {
		for {
			rawConn, err2 := listener.Accept()
			if err2 != nil {
				continue
			}
			rmAddr := rawConn.RemoteAddr()
			c := wrapConn(rawConn, rmAddr.String())
			serv.Connes <- c
		}
	}()
	return
}

// OnConnect 链接进入
func (serv *Server) OnConnect(f func(c *Conn)) {
	for c := range serv.Connes {
		go f(c)
	}
}

// NewConn 创建客户端链接
func NewConn(addr string) (conn *Conn, err error) {
	rawConn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	rmAddr := rawConn.RemoteAddr()
	conn = wrapConn(rawConn, rmAddr.String())
	return
}

//WriteMsg 写入消息
func (c *Conn) WriteMsg(buffer []byte) (err error) {
	l := 0
	if buffer != nil {
		l = len(buffer)
	}
	err = binary.Write(c, binary.LittleEndian, int32(l))
	if err != nil {
		return
	}
	if l == 0 {
		return
	}
	if _, err = c.Write(buffer); err != nil {
		c.Close()
		return
	}
	return nil
}

func gzipEncode(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer, _ := gzip.NewWriterLevel(&buffer, gzip.BestCompression)
	_, err := writer.Write(data)
	if err != nil {
		writer.Close()
		writer.Flush()
		return buffer.Bytes(), nil
	}
	writer.Close()
	return nil, err
}

func gzipDecode(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return ioutil.ReadAll(reader)
}

func (c *Conn) WriteZip(data []byte) (err error) {
	if data == nil {
		c.WriteMsg(data)
		return
	}
	data, err = gzipEncode(data)
	if err != nil {
		return
	}
	c.WriteMsg(data)
	return
}

//ReadMsg 读取消息
func (c *Conn) ReadMsg() (buffer []byte, err error) {
	var sz int32
	err = binary.Read(c, binary.LittleEndian, &sz)
	if err != nil {
		return
	}
	iz := int(sz)
	if iz == 0 {
		buffer = make([]byte, 0)
		return
	}
	buffer = make([]byte, iz)
	temp := buffer[0:iz]
	reTry := 0
	nLen := 0
	for {
		reTry++
		if reTry > 1000 {
			err = errors.New(fmt.Sprintf("Expected to read %d bytes, but only read %d", sz, nLen))
			return
		}
		n, err1 := c.Read(temp)
		if err1 != nil {
			err = err1
			return
		}
		nLen += n
		if n < iz {
			temp = buffer[n:iz]
			iz = iz - n
			continue
		} else {
			break
		}
	}
	return
}

//OnData 读取消息
func (c *Conn) OnData(f func(data []byte)) {
	go func() {
		for {
			data, err := c.ReadMsg()
			if err != nil {
				log.Println(err)
				c.Close()
				return
			}
			go f(data)
		}
	}()
}

//OnDataZip 读取消息
func (c *Conn) OnDataZip(f func(data []byte)) {
	go func() {
		for {
			data, err := c.ReadMsg()
			if err != nil {
				log.Println(err)
				c.Close()
				return
			}
			if len(data) == 0 {
				go f(data)
				continue
			}
			data, err = gzipEncode(data)
			if err != nil {
				log.Println(err)
				continue
			}
			go f(data)
		}
	}()
}

//OnClose 监听关闭
func (c *Conn) OnClose(f func()) {
	c.closeFunc = f
}

func (c *Conn) Close() error {
	if c.closeFunc != nil {
		c.closeFunc()
	}
	return c.Conn.Close()
}

func wrapConn(conn net.Conn, id string) *Conn {
	switch c := conn.(type) {
	case *Conn:
		return c
	case *net.TCPConn:
		return &Conn{conn, id, nil}
	}
	return nil
}
