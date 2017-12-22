#!/usr/bin/env python
from rabbitmq import RabbitMQ, Consumer, Publisher
import time
import json
import pika
import random
import threading
import sys
from datetime import datetime
from tinydb import TinyDB, Query

QUEUE_URL = '152.118.148.103'
QUEUE_PORT = '5672'
USERNAME = '1306398983'
PASSWORD = '446167'
VHOST = '1306398983'

EX_PAXOS = 'EX_PAXOS'

DIRECT = 'direct'
FANOUT = 'fanout'

class Learner(object):
    def __init__(self):
        self.publisher = Publisher(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        self.consumer = Consumer(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

    def callback(self, ch, method, properties, body):
        print('Callback called! body={}'.format(body))
        pass

    def learn_callback(self, ch, method, properties, body):
        print('LEARN Callback called! body={}'.format(body))

    def consume_learn(self):
        routing_key = 'LEARN'
        self.consumer.consume(
            ex_name=EX_PAXOS,
            routing_key=routing_key,
            type=DIRECT,
            callback=self.learn_callback
        )

