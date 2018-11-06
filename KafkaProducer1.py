
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

KAFAKA_HOST = "127.0.0.1"  # 服务器端口地址
KAFAKA_PORT = 9092  # 端口号
KAFAKA_TOPIC = "gpstracks"  # topic

data = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\1.txt",encoding='utf-8',engine='python',names=['gpstrack'])


class Kafka_producer():
    '''
    生产模块：根据不同的key，区分消息
    '''

    def __init__(self, kafkahost, kafkaport, kafkatopic, key):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.key = key
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort)
        )

    def sendjsondata(self, params):
        try:
            parmas_message = params  # 注意dumps
            producer = self.producer
            producer.send(self.kafkatopic, key=self.key, value=parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)


# class Kafka_consumer():
#
#     def __init__(self, kafkahost, kafkaport, kafkatopic, groupid, key):
#         self.kafkaHost = kafkahost
#         self.kafkaPort = kafkaport
#         self.kafkatopic = kafkatopic
#         self.groupid = groupid
#         self.key = key
#         self.consumer = KafkaConsumer(self.kafkatopic, group_id=self.groupid,
#                                       bootstrap_servers='{kafka_host}:{kafka_port}'.format(
#                                           kafka_host=self.kafkaHost,
#                                           kafka_port=self.kafkaPort)
#                                       )
#
#     def consume_data(self):
#         try:
#             for message in self.consumer:
#                 yield message
#         except KeyboardInterrupt as e:
#             print(e)


def sortedDictValues(adict):
    items = adict.items()
    items = sorted(items, reverse=False)
    return [value for key, value in items]


def main(xtype, group, key):
    '''
    测试consumer和producer
    '''
    while( True ):
        for index, row in data.iterrows():
            # 生产模块
            producer = Kafka_producer(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_TOPIC, key)
            print("Start : %s" % time.ctime())
            print("===========> producer:", producer)
            time.sleep(1)
            params = row['gpstrack']
            producer.sendjsondata(params)

    # if xtype == 'c':
    #     # 消费模块
    #     consumer = Kafka_consumer(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_TOPIC, group, key)
    #     print("===========> consumer:", consumer)
    #
    #     message = consumer.consume_data()
    #     for msg in message:
    #         msg = msg.value.decode('utf-8')
    #         python_data = json.loads(msg)  ##这是一个字典
    #         key_list = list(python_data)
    #         test_data = pd.DataFrame()
    #         for index in key_list:
    #             print(index)
    #             if index == 'Month':
    #                 a1 = python_data[index]
    #                 data1 = sortedDictValues(a1)
    #                 test_data[index] = data1
    #             else:
    #                 a2 = python_data[index]
    #                 data2 = sortedDictValues(a2)
    #                 test_data[index] = data2
    #                 print(test_data)
    #
    #         # print('value---------------->', python_data)
    #         # print('msg---------------->', msg)
    #         # print('key---------------->', msg.kry)
    #         # print('offset---------------->', msg.offset)


if __name__ == '__main__':
    main(xtype='p', group='py_test', key=None)
