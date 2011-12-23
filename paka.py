
#-*- coding:utf-8 -*-
# /**
# * @file   paka.py
# * @author alvayang <alvayang@sohu-inc.com>
# * @date   Fri Dec 23 14:18:30 2011
# * 
# * @brief  pika 多服务器链接扩展,考虑到发布是同步的，收取的时候又是可以直接异步的，因此，不在封装异步扩展
# * 
# * 
# */

import pika
import pika.spec as spec
from pika.exceptions import *
import traceback, sys
from socket import error
from threading import Condition
from paka_parameter import *
from Queue import Queue


class paka(object):
    # /**
    # * 让链接的参数直接传递给connection
    # * 
    # */
    def __init__(self, *args, **argv):
        self.parameters = paka_param(*args, **argv)
        self.routing_key = ''
        self._typ = ''
        self.queue = ''
        self.exchange = ''
        self.connection = None
        self.channel = None
        self.running = False
        self._queue = Queue()

    def blocking_error_out(self, *args, **argv):
        if self.connection:
            self.connection.socket.close()
            self.connection._on_connection_closed(None, True)
            self.connection = None

        func = object.__getattribute__(self, 'connect')
        routing_key = object.__getattribute__(self, 'routing_key')
        _typ = object.__getattribute__(self, '_typ')
        exchange = object.__getattribute__(self, 'exchange')
        func(exchange, routing_key, _typ)

    def basic_publish(self, exchange, routing_key, body,
                      properties=None, mandatory=False, immediate=False):
        if not self.running:
            self._queue.put_nowait({'exchange' : exchange, 'routing_key' : routing_key, 'body' : body, 'properties' : properties, 'mandatory' : mandatory, 'immediate' : immediate})
        try:
            return self.connection.basic_publish(exchange, routing_key, body, properties, mandatory, immediate)
        except:
            self._queue.put({'exchange' : exchange, 'routing_key' : routing_key, 'body' : body, 'properties' : properties, 'mandatory' : mandatory, 'immediate' : immediate})


    def connect(self, exchange = '', routing_key = '', _typ = 'fanout', queue = 'OtherQueueTest'):
        self.routing_key = routing_key
        self._typ = _typ
        self.queue = queue
        self.exchange = exchange
        try:
            self.connection = pika.BlockingConnection(self.parameters.get_param())
            self.connection._handle_disconnect = self.blocking_error_out
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange = exchange, type = _typ)
            self.channel.queue_bind(exchange = exchange, queue = queue)
            self.running = True
            while not self._queue.empty():
                info = self._queue.get_nowait()
                if info:
                    self.basic_publish(info['exchange'], info['routing_key'], info['body'], info['properties'], info['mandatory'], info['immediate'])
        except AMQPChannelError:
            self.running = False
            self.blocking_error_out()
        except error, e:
            self.running = False
            # 链接被中断，重新连一次
            self.blocking_error_out()
        except:
            raise TypeError("Unknow Exception")
        
if  __name__ == "__main__":
    import time
    pc = paka(servers = [{'host' : '10.11.150.141', 'port' : spec.PORT}, {'host' : '10.11.150.153', 'port' : spec.PORT}], heartbeat = True)
    pc.connect(exchange = 'test')
    i = 0
    
    while 1:
        pc.channel.basic_publish(exchange = 'test', routing_key = '', body = "1111")
        time.sleep(100 / 1000000)
        i+= 1
    
