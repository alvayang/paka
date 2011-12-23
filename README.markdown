## 作用
这是一个对pika的扩展。它唯一比pika多的参数是：servers，其余的参数完全可以用pika的链接参数。

## 用法
用法很简单:
import pika.spec as spec

if  __name__ == "__main__":
    import time
    # 建立对象，放入参数
    pc = blocking_connection(servers = [{'host' : '10.11.150.141', 'port' : spec.PORT}, {'host' : '10.11.150.153', 'port' : spec.PORT}])
    # 链接
    pc.blocking_connect(exchange = 'test')

    i = 0
   
    while 1:
        print str(i)
        # 发消息
        pc.channel.basic_publish(exchange = 'test', routing_key = '', body = str(i))
        # time.sleep(100 / 1000000)
        i+= 1
   

在pika的基础上多出一个参数:
servers = [{'host' : '10.11.150.141', 'port' : spec.PORT}, {'host' : '10.11.150.153', 'port' : spec.PORT}]


## 


