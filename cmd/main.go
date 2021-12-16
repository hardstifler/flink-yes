package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)

/**
模拟socket数据流
*/

type person struct {
	Key       string `json:"key"`
	Age       int    `json:"age"`
	Name      string `json:"name"`
	Address   string `json:"address"`
	TimeStamp int64  `json:"timeStamp"`
}

const (
	//绑定IP地址
	ip = "127.0.0.1"
	//绑定端口号
	port = 3333
)

func main() {
	listen, err := net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(ip), port, ""})
	if err != nil {
		fmt.Println("监听端口失败:", err.Error())
		return
	}
	fmt.Println("已初始化连接，等待客户端连接...")
	Server(listen)
}

func Server(listen *net.TCPListener) {
	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			fmt.Println("接受客户端连接异常:", err.Error())
			continue
		}
		fmt.Println("客户端连接来自:", conn.RemoteAddr().String())
		defer conn.Close()
		go func() {
			for {
				p := randomP()
				data, err := json.Marshal(p)
				if err != nil {
					continue
				}
				n, err := conn.Write(data)
				if err != nil {
					fmt.Println("发送方数据失败")
				}
				_, _ = conn.Write([]byte("\n"))
				fmt.Println(fmt.Sprintf("send %d ", n))
				time.Sleep(1 * time.Second)
			}

		}()
	}
}

func Client(conn net.Conn) {
	for {
		p := randomP()
		data, err := json.Marshal(p)
		if err != nil {
			continue
		}
		_, err = conn.Write(data)
		if err != nil {
			fmt.Println("发送成功")
		}
		buf := make([]byte, 1024)
		c, err := conn.Read(buf)
		if err != nil {
			fmt.Println("读取服务器数据异常:", err.Error())
		}
		fmt.Println(string(buf[0:c]))
	}

}

func randomP() *person {
	now := time.Now().Unix()*1000
	r := rand.Intn(500)
	r1 := rand.Intn(100)
	return &person{
		Key:       fmt.Sprintf("%d", r),
		Age:       r,
		Name:      fmt.Sprintf("%d", r+r1),
		Address:   fmt.Sprintf("%d", r-r1),
		TimeStamp: now - int64(r1),
	}
}
