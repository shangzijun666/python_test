#encoding=utf-8
import datetime,configparser,re,pymysql,vertica_python,json,csv,logging,os,time,signal,sys

import kafka
from vertica_python import connect
from kafka import KafkaConsumer, TopicPartition

# 写日志
logging.basicConfig(filename=os.path.join(os.getcwd(), 'log_tracking.txt'), level=logging.WARN, filemode='a',
                    format='%(asctime)s - %(levelname)s: %(message)s')
#错误日志
def writeErrorLog(errSrc, errType, errMsg):
    current_time = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
    try:
        v_log_file = 'err_tracking.log';
        v_file = open(v_log_file, 'a')
        v_file.write(current_time + " : " + errSrc + " - " + errType +" => " + errMsg + '\n')
        v_file.flush()
    except Exception as data:
        v_err_file = open('err_tracking.log', 'a')
        v_err_file.write(str(data) + '\n')
        v_err_file.write(current_time + " : " + errSrc + " - " + errType + " => " + errMsg + '\n')
        v_err_file.flush()
        v_err_file.close()
    finally:
        v_file.close()


class RH_Consumer:
    # 读取配置文件的配置信息，并初始化一些类需要的变量
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.mysql_host = self.config.get('Global', 'mysql_host')
        self.mysql_user = self.config.get('Global', 'mysql_user')
        self.mysql_passwd = self.config.get('Global', 'mysql_passwd')
        self.schema = self.config.get('Global', 'schema')
        self.mysql_port = int(self.config.get('Global', 'mysql_port'))
        self.kafka_server = self.config.get('Global', 'kafka_server')
        self.kafka_topic = self.config.get('Global', 'kafka_topic')
        self.consumer_group = self.config.get('Global', 'consumer_group')
        self.operation_time = datetime.datetime.now()
        self.stop_flag = 0
        self.__init_db()

    # 连接写入目标数据库
    def __init_db(self):
        try:
            self.conn_info = {'host': self.mysql_host, 'port': self.mysql_port, 'user': self.mysql_user, 'password': self.mysql_passwd,
                              'db': 'test'}
            self.mysql_db = pymysql.connect(**self.conn_info, charset="utf8")
            self.mysql_cur = self.mysql_db.cursor()
        except Exception as data:
            writeErrorLog('__init_db', 'Error', str(data))

    # 关闭数据库
    def _release_db(self):
        self.mysql_cur.close()
        self.mysql_db.close()

    # 主方法的入口
    def _do(self):
        try:
            # 配置consumer的信息，可以配置很多其他信息

            c = KafkaConsumer(self.kafka_topic,bootstrap_servers=[self.kafka_server],group_id=self.consumer_group,auto_offset_reset='latest')

            # c = KafkaConsumer(
            #     'spark_test',
            #     group_id='streaming01',
            #     bootstrap_servers=['123.57.236.115:9092'],  # 要发送的kafka主题
            #     auto_offset_reset='earliest',  # 有两个参数值，earliest和latest，如果省略这个参数，那么默认就是latest
            # )
            #定义消费kafka中的主题
            # start = 423600
            # end = 423677
            c.seek_to_beginning([TopicPartition(self.kafka_topic, 0)])
            for msg in c:
                print(msg)
                print(f"topic = {msg.topic}")  # topic default is string
                print(f"partition = {msg.partition}")
                print(f"value = {msg.value.decode()}")  # bytes to string
                print(f"timestamp = {msg.timestamp}")
                print("time = ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.timestamp / 1000)))
            #     # 如果停止程序
            #     if self.stop_flag == 1:
            #         self._exit_consumer()
            c.close()
        except Exception as data:
            print(data)
            writeErrorLog('_do', 'Error', str(data))
            logging.error('_do: ' + str(data))

    # # 此方法用来获取kafka中的数据，
    # def _get_rh_from_kafka(self, kfk_text):
    #     try:
    #         # 解析获取到的kfk中的数据流
    #         self.kfk_tb_schema = kfk_text["database"]  # schema
    #         self.kfk_tb_name = kfk_text["table"]  # table_name
    #         self.kfk_data = kfk_text['data'][0]  # data
    #         self.kfk_type = kfk_text['type']  # 数据类型type
    #         self.kfk_es = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(kfk_text['es'] / 1000)))  # 数据表更的时间
    #
    #
    #     except Exception as data:
    #         writeErrorLog('_get_rh_from_kafka', 'Error', str(data))
    #         logging.error('_get_rh_from_kafka: ' + str(data))


    # # 退出消费消息
    # def _exit_consumer(self):
    #     self._release_db()
    #     sys.exit()


# def exit_program(signum, frame):
#     logging.info("Received Signal: %s at frame: %s" % (signum, frame))
#     p.stop_flag = 1
#

def main():
    # 实例化对象
    p = RH_Consumer()
    # signal.signal(signal.SIGTERM, exit_program)
    # while True:
    p._do()


if __name__ == '__main__':
    main()