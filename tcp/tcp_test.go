package tcp

import (
	"log"
	"testing"
	"time"
)

func TestGzip(t *testing.T) {
	s := "你好，我是中国人,你好，我是中国人,你好，我是中国人，你好，我是中国人，你好，我是中国人，你好，我是中国人"
	b, _ := GzipEncode([]byte(s))
	u, _ := GzipDecode(b)
	log.Println("OUT" + string(u))
}

func TestClient(t *testing.T) {
	log.Println("client")
	client, err := NewConn("127.0.0.1:8000")
	if err != nil {
		log.Println(err.Error())
		return
	}
	client.OnClose(func() {
		log.Println("client:客户端已经关闭")
	})
	client.OnDataZip(func(data []byte) {
		log.Println(string(data))
	})
	client.WriteZip([]byte("您好"))
	//发生心跳
	go func() {
		for {
			time.Sleep(3 * time.Second)
			if err = client.WriteZip(nil); err != nil {
				break
			}
		}
	}()
	select {}
}

func TestServer(t *testing.T) {
	serv, err := NewServer("0.0.0.0:8000")
	if err != nil {
		log.Println(err.Error())
		return
	}
	serv.OnConnect(func(c *Conn) {
		log.Println("server:有客户端连入")
		c.OnClose(func() {
			log.Println("server:客户端已经关闭")
		})
		c.OnDataZip(func(data []byte) {
			log.Println("server:" + string(data))
			c.WriteZip([]byte("您好"))
		})
	})
	select {}
}
