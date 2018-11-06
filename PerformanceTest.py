
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import multiprocessing
import time

KAFAKA_HOST = "127.0.0.1"  # 服务器端口地址
KAFAKA_PORT = 9092  # 端口号
KAFAKA_TOPIC = "gpstracks"  # topic

data1 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\16.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data2 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\100.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data3 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\118.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data4 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\131.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data5 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\132.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data6 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\133.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data7 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\134.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data8 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\135.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data9 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\136.txt",encoding='utf-8',engine='python',names=['gpstrack'])
data10 = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\137.txt",encoding='utf-8',engine='python',names=['gpstrack'])



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
            #print(producer.partitions_for("gpstracks"))
            producer.flush()
        except KafkaError as e:
            print(e)



def sortedDictValues(adict):
    items = adict.items()
    items = sorted(items, reverse=False)
    return [value for key, value in items]


def main(xtype, group, key, data):
    '''
    测试consumer和producer
    '''
    while( True ):
        for index, row in data.iterrows():
            # 生产模块
            producer = Kafka_producer(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_TOPIC, key)
            print("Start" + xtype+": %s" % time.ctime())
            print("===========> producer:", producer)
            params = row['gpstrack']
            producer.sendjsondata(params)
            time.sleep(1)






if __name__ == '__main__':
    p1 = multiprocessing.Process(target=main, args=('p1', 'py_test', None, data1, ))
    p2 = multiprocessing.Process(target=main, args=('p2', 'py_test', None, data2, ))
    p3 = multiprocessing.Process(target=main, args=('p3', 'py_test', None, data3,))
    p4 = multiprocessing.Process(target=main, args=('p4', 'py_test', None, data4,))
    p5 = multiprocessing.Process(target=main, args=('p5', 'py_test', None, data5,))
    p6 = multiprocessing.Process(target=main, args=('p6', 'py_test', None, data6,))
    p7 = multiprocessing.Process(target=main, args=('p7', 'py_test', None, data7,))
    p8 = multiprocessing.Process(target=main, args=('p8', 'py_test', None, data8,))
    p9 = multiprocessing.Process(target=main, args=('p9', 'py_test', None, data9,))
    p10 = multiprocessing.Process(target=main, args=('p10', 'py_test', None, data10,))
    # for i in range(1,11):
    #     p[i]=multiprocessing.Process(target=main, args=('p'+i, 'py_test', None, data1, ))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
