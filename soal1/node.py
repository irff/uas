#!/usr/bin/env python
from rabbitmq import RabbitMQ, Consumer, Publisher
import time
import json
import pika
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

class Node(object):
    def __init__(self, node_id):
        self.node_id = node_id

        self.publisher = Publisher(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        self.consumer_write = Consumer(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        self.consumer_broadcast = Consumer(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        self.consumer_read = Consumer(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        self.db = TinyDB('db.json')
        self.DB = Query()

        consume_write_thread = threading.Thread(
            target=self.consume_write
        )
        consume_write_thread.start()

        consume_broadcast_thread = threading.Thread(
            target=self.consume_broadcast
        )
        consume_broadcast_thread.start()

        consume_read_thread = threading.Thread(
            target=self.consume_read
        )
        consume_read_thread.start()

    def attach_sender_id(self, message):
        message = {
            'sender_id': self.node_id,
            'message': message
        }

        return json.dumps(message)

    def callback(self, ch, method, properties, body):
        print('Callback called! body={}'.format(body))
        pass

    def read_db(self):
        try:
            result = self.db.get(self.DB.node_id == self.node_id)
            if result is not None:
                print('Node {} contains message: {}'.format(self.node_id, result))
            else:
                print('Node {} empty.'.format(self.node_id))
        except Exception as e:
            print('Error reading from DB. {}'.format(e.message))

    def update_db(self, message):
        try:
            result = self.db.get(self.DB.node_id == self.node_id)
            if result is not None:
                self.db.update({
                    'message': message
                }, self.DB.node_id == self.node_id)
                print("DB updated: {}".format(message))
            else:
                self.db.insert({
                    'message': message,
                    'node_id': self.node_id
                })
                print("DB inserted: {}".format(message))

        except Exception as e:
            print('Error writing to DB. {}'.format(e.message))


    def consume_write_callback(self, ch, method, properties, body):
        print('WRITE Callback called! body={}'.format(body))

        self.update_db(message=body)
        self.publish_ack(body)

    def consume_broadcast_callback(self, ch, method, properties, body):
        print('BROADCAST Callback called! body={}'.format(body))
        self.update_db(message=body)

    def consume_read_callback(self, ch, method, properties, body):
        print('READ Callback called! body={}'.format(body))
        self.read_db()

    # Lakukan consume WRITE_[NODE_ID]
    # Apabila menerima, langsung menulis message di DB
    # Lalu publish response ACK_RELAY
    def consume_write(self):
        routing_key = 'WRITE_{}'.format(self.node_id)
        self.consumer_write.consume(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            type=DIRECT,
            callback=self.consume_write_callback
        )

    # Lakukan Consume BROADCAST_[NODE_ID]
    # Apabila menerima, langsung tulis message di DB
    def consume_broadcast(self):
        routing_key = 'BROADCAST_{}'.format(self.node_id)
        self.consumer_broadcast.consume(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            type=DIRECT,
            callback=self.consume_broadcast_callback
        )

    # Lakukan Consume READ
    # Apabila menerima, langsung baca di DB dan outputkan ke terminal
    def consume_read(self):
        routing_key = 'READ'
        self.consumer_read.consume(
            ex_name=EX_READ,
            routing_key=routing_key,
            type=FANOUT,
            callback=self.consume_read_callback
        )

    # Publish ACK_RELAY ke node relay bila selesai menerima input WRITE dan selesai menuliskan ke DB
    def publish_ack(self, message):
        routing_key = 'ACK_RELAY'
        self.publisher.publish(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            message=self.attach_sender_id(message),
            type=DIRECT
        )


args = sys.argv

if len(args) > 1:
    node_id = args[1]
    print 'NODE_ID = {}'.format(node_id)
    node = Node(node_id)
else:
    print('Usage: python node.py [NODE_ID]')
