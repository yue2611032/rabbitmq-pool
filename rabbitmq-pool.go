package rabbitmqpool

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

var (
	retryCount      = 5               //重试次数
	waitConfirmTime = 5 * time.Second //等待重试时间
)

//ConnectionPool 连接池
type ConnectionPool struct {
	Lock          sync.Mutex                    //连接池锁
	Connections   map[string]*ConnectionContext //连接对象池
	MaxConnection uint                          //最大连接数
	MaxChannel    uint                          //最大信道数
}

//ConnectionContext 连接上下文
type ConnectionContext struct {
	Lock           sync.RWMutex
	Host           string
	User           string
	Password       string
	Port           uint
	VirtualHost    string                     //默认为空，则访问/
	Connection     *amqp.Connection           //连接对象
	RetryCount     uint                       //重试次数
	ConnectionTime time.Time                  //连接时间
	Channels       map[string]*ChannelContext //信道集
	ConnectionID   string                     //连接ID
	retryFlag      bool                       //是否可以重连
}

//ChannelContext 信道对象
type ChannelContext struct {
	Exchange     string //交换机名称
	ExchangeType string //交换机类型 fanout广播,direct路由直连，topic通配符匹配,header head匹配
	QueueName    string //队列名称
	BindingKey   string //交换器和队列绑定的 Key
	RoutingKey   string //生产者发送给交换器绑定的 Key
	Reliable     bool   //是否断线重连
	Durable      bool   //是否持久化
	AutoDelete   bool   //是否自动删除,若未有队列绑定该交换机，则自动删除exchange
	NoWait       bool   //是否阻塞队列
	ChannelID    string
	Channel      *amqp.Channel
	retryFlag    bool                  //是否可以重连
	Queues       map[string]amqp.Queue //队列组
}

var (
	once sync.Once
	cp   *ConnectionPool
)

//SetRetry 设置重连次数和时间
func SetRetry(count int, t time.Duration) {
	retryCount = count
	waitConfirmTime = t
}

//Init 初始化连接池,仅会执行一次
func Init(MaxConnection, MaxChannel uint) *ConnectionPool {
	once.Do(func() {
		cp = &ConnectionPool{
			MaxChannel:    20,
			MaxConnection: 5,
			Connections:   make(map[string]*ConnectionContext, int(MaxConnection)),
		}
		if MaxConnection > 0 {
			cp.MaxConnection = MaxConnection
		}
		if MaxChannel > 0 {
			cp.MaxChannel = MaxChannel
		}
	})
	return cp
}

//generateID 创建ID
func generateID(args ...interface{}) string {
	str := ""
	for _, v := range args {
		str = fmt.Sprintf("%s:%s", str, fmt.Sprintf("%v", v))
	}
	hasher := md5.New()
	hasher.Write([]byte(str))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (c *ConnectionContext) generateID() string {
	return generateID(c.Host, c.User, c.Password, c.Port, c.VirtualHost)
}

func (c *ChannelContext) generateID() string {
	return generateID(c.Exchange, c.ExchangeType, c.BindingKey, c.RoutingKey, c.Durable, c.Reliable, c.AutoDelete)
}

//CreateConnection 创建一个新连接
func (c *ConnectionPool) CreateConnection(host, user, password, vhost string, port uint) (*ConnectionContext, error) {
	//创建连接需要开启读锁
	c.Lock.Lock()
	defer c.Lock.Unlock()
	cc := &ConnectionContext{
		Host:        host,
		User:        user,
		Password:    password,
		VirtualHost: vhost,
		Port:        port,
	}
	id := cc.generateID()
	if c.Connections[id] != nil && !c.Connections[id].Connection.IsClosed() {
		return c.Connections[id], nil
	}
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", user, password, host, port, vhost))
	if err != nil {
		return nil, fmt.Errorf("创建连接时错误：%v", err)
	}
	cc.ConnectionID = id
	cc.Connection = conn
	cc.Channels = make(map[string]*ChannelContext)
	c.Connections[id] = cc
	cc.retryFlag = true
	go cc.reconnect()
	return cc, nil
}

func (c *ConnectionContext) reconnect() {
	closeChan := make(chan *amqp.Error, 1)
	c.Connection.NotifyClose(closeChan)
	select {
	case e := <-closeChan:
		if c.retryFlag {
			log.Println("连接失败,开启重连", c.ConnectionID, e)
			for i := 0; retryCount == 0 || i < retryCount; i++ {
				conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.VirtualHost))
				if err != nil {
					log.Println("重新连接时错误,等待下次重连：", err)
					time.Sleep(waitConfirmTime)
					continue
				}
				c.Connection = conn
				if c.Connection != nil && !c.Connection.IsClosed() {
					break
				}
			}
		}
		// close(closeChan)
	}
}

//Destory 销毁连接池
func (c *ConnectionPool) Destory() error {
	for ck, v := range c.Connections {
		v.retryFlag = false
		for k, v2 := range v.Channels {
			v2.retryFlag = false
			if err := v2.Channel.Close(); err != nil {
				return fmt.Errorf("关闭Channel[%s]错误，%v", v2.ChannelID, err)
			}
			delete(v.Channels, k)
		}
		if err := v.Connection.Close(); err != nil {
			return fmt.Errorf("关闭Connection[%s]错误，%v", v.ConnectionID, err)
		}
		delete(c.Connections, ck)
	}
	return nil
}

//Destory 销毁连接
func (c *ConnectionContext) Destory() error {
	c.retryFlag = false
	for k, v2 := range c.Channels {
		v2.retryFlag = false
		if err := v2.Channel.Close(); err != nil {
			return fmt.Errorf("关闭Channel[%s]错误，%v", v2.ChannelID, err)
		}
		delete(c.Channels, k)
	}
	if err := c.Connection.Close(); err != nil {
		return fmt.Errorf("关闭Connection[%s]错误，%v", c.ConnectionID, err)
	}
	return nil
}

//Destory 销毁信道
func (c *ChannelContext) Destory() error {
	c.retryFlag = false
	if err := c.Channel.Close(); err != nil {
		return fmt.Errorf("关闭Channel[%s]错误，%v", c.ChannelID, err)
	}
	return nil
}

//CreateChannel 创建一个信道
func (c *ConnectionContext) CreateChannel(exchange, exchangeType, queueName, bindingKey, routingKey string, durable, autoDelete, reliable, noWait bool) (*ChannelContext, error) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	cc := &ChannelContext{
		Exchange:     exchange,
		ExchangeType: exchangeType,
		BindingKey:   bindingKey,
		RoutingKey:   routingKey,
		Reliable:     reliable,
		Durable:      durable,
		AutoDelete:   autoDelete,
		NoWait:       noWait,
		QueueName:    queueName,
		Queues:       make(map[string]amqp.Queue, 0),
	}
	id := cc.generateID()
	//创建channel
	channel, err := c.Connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("Connection[%s]创建Channel错误%v", c.ConnectionID, err)
	}
	cc.ChannelID = id
	cc.Channel = channel
	c.Channels[id] = cc
	cc.retryFlag = true
	//判断是否开启高可用
	if cc.Reliable {
		//开启断线重连
		go func() {
			closeChan := make(chan *amqp.Error, 1)
			cc.Channel.NotifyClose(closeChan)
			select {
			case e := <-closeChan:
				if c.retryFlag {
					log.Println("信道连接断开,开始重新建立连接", e)
					for i := 0; retryCount == 0 || i < retryCount; i++ {
						chn, err := c.Connection.Channel()
						if err != nil {
							log.Println("重新创建Channel错误，等待下次重连:", err)
						} else {
							c.Channels[id].Channel = chn
							log.Println("重新连接成功,重新绑定Queue")
							c.Channels[id].QueueBind()
							break
						}
						time.Sleep(waitConfirmTime)
					}
				}
				// close(closeChan)
			}
		}()
	}
	return cc, nil
}

//ExchangeDeclare 创建交换机
func (c *ChannelContext) ExchangeDeclare() error {
	if err := c.Channel.ExchangeDeclare(c.Exchange, c.ExchangeType, c.Durable, c.AutoDelete, false, c.NoWait, nil); err != nil {
		return fmt.Errorf("创建交换机错误:%v", err)
	}
	return nil
}

//QueueDeclare 创建一个队列,QueueName为空则创建随机队列
func (c *ChannelContext) QueueDeclare() (amqp.Queue, error) {
	q, err := c.Channel.QueueDeclare(c.QueueName, c.Durable, c.AutoDelete, false, c.NoWait, nil)
	if err != nil {
		return q, fmt.Errorf("创建交换机错误:%v", err)
	}
	c.Queues[q.Name] = q
	return c.Queues[q.Name], nil
}

//QueueBind 绑定队列
func (c *ChannelContext) QueueBind() error {
	if err := c.Channel.QueueBind(c.QueueName, c.BindingKey, c.Exchange, c.NoWait, nil); err != nil {
		return fmt.Errorf("绑定交换机错误:%v", err)
	}
	return nil
}

//ExchangeBind 绑定交换机
func (c *ChannelContext) ExchangeBind(destination, key, source string, noWait bool) error {
	if err := c.Channel.ExchangeBind(destination, key, source, noWait, nil); err != nil {
		return fmt.Errorf("绑定交换机错误:%v", err)
	}
	return nil
}

//Publish 发送消息
func (c *ChannelContext) Publish(body string) error {
	return c.Channel.Publish(c.Exchange, c.QueueName, false, false, amqp.Publishing{
		//类型
		ContentType: "text/plain",
		//消息
		Body: []byte(body),
	})
}

//Resome 消费队列
func (c *ChannelContext) Resome(queueName string, receive func(interface{})) error {
	//消费消息
	message, err := c.Channel.Consume(
		queueName,
		"", //可以执行对应消费者
		true,
		false,
		false,
		c.NoWait,
		nil,
	)
	if err != nil {
		return err
	}
	forever := make(chan bool)
	go func() {
		for d := range message {
			receive(&d)
		}
	}()
	<-forever
	return err
}
