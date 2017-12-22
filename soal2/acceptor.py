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

class Acceptor(object):
    def __init__(self, acceptor_id):
        self.acceptor_id = acceptor_id
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

# PARSING ACCEPTOR_ID DARI CLI PARAMETER & Jalankan Proposer
args = sys.argv

if len(args) > 1:
    acceptor_id = args[1]
    print 'ACCEPTOR_ID = {}'.format(acceptor_id)
    proposer = Acceptor(acceptor_id)
else:
    print('Usage: python proposer.py [ACCEPTOR_ID]')
