# coding=utf-8
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
producer = KafkaProducer(bootstrap_servers=['127.0.0.1：9092','127.0.0.1:9093','127.0.0.1:9094'])
producer.send(topic='test',value=b'testbootstrap_servers',key=b'2')
producer.flush()