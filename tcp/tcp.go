package tcp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

type Conn struct {
	net.Conn
	Context    any
	MaxReadLen int
	IsConnect  bool
	closeFunc  func()
}

type Server struct {
	net.Listener
	Connes chan *Conn
	ctx    context.Context
	cancel func()
}

func NewTcpServer(network, addr string) (serv *Server, err error) {
	listener, err := net.Listen(network, addr)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	serv = &Server{
		Listener: listener,
		Connes:   make(chan *Conn),
		ctx:      ctx,
		cancel:   cancel,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:

			}
			rawConn, err := listener.Accept()
			if err != nil {
				continue
			}
			c := wrapConn(rawConn)
			serv.Connes <- c
		}
	}()
	return
}

// NewServer 创建服务
func NewServer(addr string) (serv *Server, err error) {
	return NewTcpServer("tcp", addr)
}

// OnConnect 链接进入
func (serv *Server) OnConnect(f func(c *Conn)) {
	for c := range serv.Connes {
		go f(c)
	}
}

// Close 关闭链接
func (serv *Server) Close() error {
	close(serv.Connes)
	serv.cancel()
	return serv.Listener.Close()
}

// NewConn 创建客户端链接
func NewConn(addr string) (conn *Conn, err error) {
	return NewTcpConn("tcp", addr)
}

func NewTcpConn(network, addr string) (conn *Conn, err error) {
	rawConn, err := net.Dial(network, addr)
	if err != nil {
		return
	}
	conn = wrapConn(rawConn)
	return
}

func GzipEncode(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	_, err := writer.Write(data)
	if err != nil {
		return nil, err
	}
	writer.Close()
	writer.Flush()
	out := buffer.Bytes()
	return out, nil
}

func GzipDecode(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// WriteMsg 写入消息
func (c *Conn) WriteMsg(buffer []byte) (err error) {
	l := 0
	if buffer != nil {
		l = len(buffer)
	}
	err = binary.Write(c, binary.LittleEndian, uint32(l))
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

// WriteZip 压缩并写入消息
func (c *Conn) WriteZip(data []byte) (err error) {
	if data == nil {
		c.WriteMsg(data)
		return
	}
	data, err = GzipEncode(data)
	if err != nil {
		return
	}
	c.WriteMsg(data)
	return
}

// readBytes 读取套接字字节
func (c *Conn) readBytes(length int) ([]byte, error) {
	if length > c.MaxReadLen {
		return nil, errors.New(fmt.Sprintf("Expected to read %d bytes, but only max read %d", length, c.MaxReadLen))
	}
	temp := make([]byte, length)
	buffer := make([]byte, 0)
	maxTry := length / 100
	if maxTry < 10 {
		maxTry = 10
	}
	nLen := 0
	for i := 0; i < maxTry; i++ {
		n, err := c.Read(temp)
		if err != nil {
			return nil, err
		}
		buffer = append(buffer, temp[:n]...)
		nLen += n
		if nLen >= length {
			return buffer, nil
		}
		temp = temp[n:]
	}
	return nil, errors.New(fmt.Sprintf("Expected to read %d bytes, but only read %d", length, nLen))
}

// ReadMsg 读取消息
func (c *Conn) ReadMsg() (buffer []byte, err error) {
	lenBytes, err2 := c.readBytes(4)
	if err2 != nil {
		return nil, err2
	}
	iz := int(binary.LittleEndian.Uint32(lenBytes))
	if iz == 0 {
		buffer = make([]byte, 0)
		return
	}
	buffer, err = c.readBytes(iz)
	return
}

// OnData 读取消息
func (c *Conn) OnData(f func(data []byte)) {
	if c.MaxReadLen == 0 {
		c.MaxReadLen = 64 * 1024 //64k
	}
	go func() {
		for {
			data, err := c.ReadMsg()
			if err != nil {
				log.Println(err)
				c.Close()
				return
			}
			f(data)
		}
	}()
}

// OnDataZip 读取消息
func (c *Conn) OnDataZip(f func(data []byte)) {
	if c.MaxReadLen == 0 {
		c.MaxReadLen = 64 * 1024 //64k
	}
	go func() {
		for {
			data, err := c.ReadMsg()
			if err != nil {
				log.Println(err)
				c.Close()
				return
			}
			if len(data) == 0 {
				f(data)
				continue
			}
			buf, err := GzipDecode(data)
			if err != nil {
				log.Println(err)
				continue
			}
			f(buf)
		}
	}()
}

// OnClose 监听关闭
func (c *Conn) OnClose(f func()) {
	c.closeFunc = f
}

func (c *Conn) Close() error {
	if c.closeFunc != nil {
		c.closeFunc()
	}
	c.IsConnect = false
	return c.Conn.Close()
}

func wrapConn(conn net.Conn) *Conn {
	switch c := conn.(type) {
	case *Conn:
		return c
	case *net.TCPConn:
		return &Conn{conn, nil, 65536, true, nil}
	}
	return nil
}
