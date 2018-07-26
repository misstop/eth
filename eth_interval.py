import pymysql
import requests
import time
import os
import yaml
import logging
from lxml import etree
from apscheduler.schedulers.blocking import BlockingScheduler
"""
此项目用来抓取以太坊三千币种token页面中的时间间隔
"""
# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='eth_interval.log',
                    filemode='a')


# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path))
# 数据库
host = x['DATADB']['MYSQL']['HOST']
username = x['DATADB']['MYSQL']['UNAME']
pwd = x['DATADB']['MYSQL']['PWD']
database = x['DATADB']['MYSQL']['DNAME']
# 时间间隔
time_interval = x['INTERVAL']['TIME']
time_sleep = x['INTERVAL']['SLEEP']


# 数据库连接
def connect_db():
    logging.info('start to connect mysql')
    db = pymysql.connect('{}'.format(host), '{}'.format(username), '{}'.format(pwd), '{}'.format(database))
    logging.info('connect success')
    return db


# 查询语句
def query_db(db):
    cursor = db.cursor()
    sql = "select id, contract0, contract from coin_contrast"
    ls = []
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except Exception as e:
        logging.info(e)
    for f in results:
        c_id = f[0]
        contra = f[1] if f[1] else f[2]
        ls.append((c_id, contra))
    return ls


# 更新数据库字段
def update_db(db, c_id, tInterval):
    cursor = db.cursor()
    sql = " UPDATE coin_contrast SET transfer_interval ='{}' WHERE id = '{}'".format(tInterval, c_id)
    try:
        cursor.execute(sql)
        db.commit()
        logging.info("id--%s update success" % c_id)
        
    except Exception as e:
        logging.info(e)
        db.rollback()


# 关闭数据库
def close_db(db):
    db.close()


# 天数字符串截取
def str_cap(a):
    a = a.replace(' ', '')
    b = 'day'
    if b in a:
        c = a.split(b)[0]
        return c
    else:
        return 0


# 抓取清洗数据
def crawl_eth(contract):
    try:
        re = requests.get('https://etherscan.io/token/generic-tokentxns2?contractAddress=%s' % contract, timeout=20)
    except Exception as e:
        logging.info(e)
    html = etree.HTML(re.content)
    time_ls = html.xpath('//tr/td[2]/span')
    time_index = time_ls[0].text
    time_last = time_ls[-1].text
    return str_cap(time_index), str_cap(time_last)


# 启动函数
def run():
    db = connect_db()  # 连接MySQL数据库
    for _ in query_db(db):
        try:
            time_index, time_last = crawl_eth(_[1])
            time.sleep(time_sleep)
            tInterval = '(%s, %s)' % (time_index, time_last)
            # logging.info(tInterval)
            # logging.info(type(tInterval))
            update_db(db, _[0], tInterval)
        except Exception as e:
            logging.info(e)
            continue
    close_db(db)  # 关闭数据库


SCHEDULER = BlockingScheduler()
if __name__ == '__main__':
    run()
    SCHEDULER.add_job(func=run, trigger='interval', hours=time_interval)
    SCHEDULER.start()

