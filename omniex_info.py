import requests
import json
import logging
import yaml, os
from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer

# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path, 'rb'))
kafka_con = x['QUEUES']['KAFKA']['HOST']
kafka_topic = x['QUEUES']['KAFKA']['TOPIC']


# 日志配置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='omniec_info.log',
                    filemode='a')


def crawl():
    producer = KafkaProducer(bootstrap_servers=kafka_con, api_version=(0, 10, 1),
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    ls = []
    data = {"addr": "3MbYQMMmSkC3AgWkj9FMo5LsPTW1zBTwXL"}
    try:
        res = requests.post('https://api.omniexplorer.info/v1/transaction/address/0', data=data)
        det = json.loads(res.text)['transactions']
        for d in det[0:3]:
            dic = {
                'usdtOnlykey': d['txid'],
                'timeStamp': d['blocktime'],
                'amount': d['amount'],
                'typeValue': d['type'],
                'addressOut': d['sendingaddress'],
                'addressIn': d['referenceaddress'],
                'statusValue': 'CONFIRMED' if d['valid'] is True else "INVALID",
                'tab': d['propertyname'],
            }
            ls.append(dic)
        ls = json.dumps(ls)
        producer.send(kafka_topic, ls)
    except Exception as e:
        logging.info(e)
    producer.flush()
    logging.info('send to kafka=====================================================>')
    producer.close()

SCHEDULER = BlockingScheduler()
if __name__ == '__main__':
    SCHEDULER.add_job(func=crawl, trigger='interval', minutes=3)
    SCHEDULER.start()
    # 测试
    # crawl()
