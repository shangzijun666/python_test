import traceback,uuid,random,threading,json,time
from kafka.errors import kafka_errors
from pykafka import KafkaClient
from kafka import KafkaProducer, KafkaConsumer

hosts = '123.57.236.115:9092'

client = KafkaClient(hosts=hosts)
topic = client.topics['spark_test']
producer = topic.get_producer()


def work():

    while 1:

        # msg = json.dumps({
        # "entid": str(uuid.uuid4()).replace('-', ''),
        #
        # "entname": '北京基智科技'+str(random.randint(1, 30)),
        #
        # "uniscid": 'abcdefghigklmn'+str(random.randint(30, 100))
        # })


        msg = json.dumps({
            "entid": '53fcdf36167c48388f7fa5d059a9c92b',

            "entname": '北京基智科技有限公司' + str(random.randint(1, 30)),

            "uniscid": 'UNIASDIANSDIHAS' + str(random.randint(30, 100))
        })
        producer.produce(bytes(msg, encoding='utf-8'))
# 多线程执行
if __name__ == '__main__':

    thread_list = [threading.Thread(target=work) for i in range(1)]

    for thread in thread_list:
        thread.setDaemon(True)
        thread.start()
        time.sleep(1)
    # 关闭句柄, 退出
    producer.stop()