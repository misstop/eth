import requests
import json
import logging
import pymysql
import os
import yaml
import time
import datetime


'''
表字段顺序
tx_id, type, time, from, to, amount, property_name, property_id, valid
'''
# 日志配置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='omniexplorer4.log',
                    filemode='a')


# 时间戳转换为时间
def cur_time(timestamp):
    now = time.localtime(timestamp)
    cur_time = time.strftime("%Y-%m-%d %H:%M:%S", now)
    return cur_time


# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path, encoding='UTF-8'))
# 数据库
host = x['DATADBO']['MYSQL']['HOST']
username = x['DATADBO']['MYSQL']['UNAME']
pwd = x['DATADBO']['MYSQL']['PWD']
database = x['DATADBO']['MYSQL']['DNAME']


# 数据库连接
def connect_db():
    logging.info('start to connect mysql')
    db = pymysql.connect('{}'.format(host), '{}'.format(username), '{}'.format(pwd), '{}'.format(database))
    logging.info('connect success')
    return db


def insert_db(db, tx_id, type, time, _from, to, amount, property_name, valid, property_id):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 插入语句
    sql = "INSERT INTO omni_explorer_3M VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}') on duplicate key update tx_id = values(tx_id)".format(
        tx_id, type, time, _from, to, amount, property_name, valid, property_id)
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
        logging.info("insert success")
    except Exception as e:
        # Rollback in case there is any error
        logging.error(e)
        db.rollback()


# 关闭数据库
def close_db(db):
    db.close()


# 获取页数
def get_pages():
    data = {'addr': '3MbYQMMmSkC3AgWkj9FMo5LsPTW1zBTwXL'}
    try:
        re = requests.post('https://api.omniexplorer.info/v1/transaction/address/0',
                           data=data, timeout=20)
    except Exception as e:
        logging.error(e)
    detail = json.loads(re.text)
    return detail['pages']


# 循环爬取
def crawl_page(db, i):
    data = {'addr': '3MbYQMMmSkC3AgWkj9FMo5LsPTW1zBTwXL'}
    try:
        re = requests.post('https://api.omniexplorer.info/v1/transaction/address/%s' % i,
                           data=data, timeout=20)
    except Exception as e:
        logging.error(e)
    d = json.loads(re.text)['transactions']
    for _ in d:
        d_time = cur_time(_['blocktime'])
        if 'valid' in _.keys():
            valid = 'CONFIRMED'
        else:
            valid = 'UNCONFIRMED'
        if 'amount' in _.keys():
            amount = _['amount']
        else:
            amount = ''
        insert_db(db, _['txid'], _['type'], d_time, _['sendingaddress'], _['referenceaddress'], amount,
                  _['propertyname'], valid, _['propertyid'])


# 启动函数
def run():
    i = get_pages()
    logging.info(i)
    db = connect_db()  # 连接MySQL数据库
    for x in range(i):
        try:
            crawl_page(db, x+1)
            time.sleep(0.1)
        except Exception as e:
            logging.error(e)
            continue
    db.close()

if __name__ == '__main__':
    run()