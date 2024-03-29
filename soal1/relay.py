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

EX_WRITE = 'EX_WRITE'
EX_READ = 'EX_READ'

DIRECT = 'direct'
FANOUT = 'fanout'

class Relay(object):
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

        # daftar Node ID, jangan lupa menambahkan ke sini jika menjalankan node baru
        self.nodes = [1, 2, 3]

    def consume(self):
        consume_ack_thread = threading.Thread(
            target=self.consume_ack
        )
        consume_ack_thread.start()

    def callback(self, ch, method, properties, body):
        print('Callback called! body={}'.format(body))
        pass

    # Relay menerima pesan ACK dari suatu node dan mem-broadcast ke node-node lain
    def consume_ack_callback(self, ch, method, properties, body):
        print('ACK Callback called! body={}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']
        message = body['message']

        self.broadcast(message, sender_id)

    # Relay menunggu adanya pesan ACK dari suatu node untuk dibroadcast ke node-node lain
    def consume_ack(self):
        routing_key = 'ACK_RELAY'
        self.consumer.consume(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            type=DIRECT,
            callback=self.consume_ack_callback
        )

    # Relay mengirimkan pesan WRITE dengan tipe DIRECT ke suatu node
    def publish_write(self, message, receiver_node):

        routing_key = 'WRITE_{}'.format(receiver_node)
        self.publisher.publish(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            message=message,
            type=DIRECT
        )

    # Fungsi Publish broadcast ke suatu node, tapi delay secara random antara 1-5 detik
    def publish_broadcast(self, message, receiver_node):
        def broadcast_sleep():
            rand_seconds = random.randrange(1000, 5000) / 1000
            time.sleep(rand_seconds)

            routing_key = 'BROADCAST_{}'.format(receiver_node)
            self.publisher.publish(
                ex_name=EX_WRITE,
                routing_key=routing_key,
                message=message,
                type=DIRECT
            )

        broadcast_thread = threading.Thread(
            target=broadcast_sleep
        )
        broadcast_thread.start()

    # Untuk setiap node selain SOURCE_NODE, publish broadcast
    def broadcast(self, message, source_node):
        for node in self.nodes:
            if str(node) != source_node:
                self.publish_broadcast(
                    message=message,
                    receiver_node=node)

    # Publish READ secara fanout ke semua node
    def publish_read(self):
        routing_key = 'READ'
        message = 'read'
        self.publisher.publish(
            ex_name=EX_READ,
            routing_key=routing_key,
            message=message,
            type=FANOUT
        )

args = sys.argv

if len(args) > 1:
    action = args[1]
    print 'ACTION = {}'.format(action)
    relay = Relay()
    if action == 'CONSUME':
        relay.consume()
    elif action == 'READ':
        relay.publish_read()
    elif action == 'WRITE' and len(args) > 3:
        receiver_node = int(args[2])
        message = args[3]
        relay.publish_write(
            message=message,
            receiver_node=receiver_node
        )
else:
    print('Usage: python relay.py [ACTIONS]')
    print('Example: ')
    print('> python relay.py CONSUME')
    print('> python relay.py READ')
    print('> python relay.py WRITE 1 \'Hello world!\'')