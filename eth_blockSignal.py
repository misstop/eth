import re
import lxml
import requests
import json
import logging
import pymysql
import os
import yaml
import time
import datetime
from lxml import etree

# 日志配置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='eth_blockSignal.log',
                    filemode='a')

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


def insert_db(db, txHash, block, age, _from, to, value):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 插入语句
    sql = "INSERT INTO eth_blockSignal VALUES (%s, %s, %s, %s, %s, %s) on duplicate key update txHash = values(txHash)"
    par = (txHash, block, age, _from, to, value)
    try:
        # 执行sql语句
        cursor.execute(sql, par)
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
def get_num(url):
    try:
        html = requests.get(url)
    except Exception as e:
        logging.error(e)
    html = str(html.content, encoding='utf-8')
    num = re.search(r'of <b>(\d)</b>', html)
    return int(num.group(1))


# 获取txHash block age _from to value
def get_msg(db, url):
    try:
        html = requests.get(url)
    except Exception as e:
        logging.error(e)
    html = etree.HTML(html.content)
    txHash_ls = html.xpath("//tr/td[1]/span[contains(@class, 'address-tag')]/a")
    block_ls = html.xpath("//tr/td[contains(@class, 'hidden-sm')]/a")
    age_ls = html.xpath("//tr/td[3]/span")
    _from_ls = html.xpath("//tr/td[4]/span[contains(@class, 'address-tag')]/a")
    to_ls = html.xpath("//tr/td[6]//span[contains(@class, 'address-tag')]/a")
    value_ls = html.xpath("//tr/td[7]")
    le = len(txHash_ls)
    for l in range(le):
        txHash = txHash_ls[l].text
        block = block_ls[l].text
        age = age_ls[l].text
        _from = _from_ls[l].text
        to = to_ls[l].text
        value = value_ls[0].xpath('string(.)')
        if len(_from) == 42 and len(to) == 42:
            continue
        else:
            insert_db(db, txHash, block, age, _from, to, value)
            logging.info("insert success")


if __name__ == '__main__':
    db = connect_db()
    block = 6078100
    while block > 0:
        try:
            url = 'https://etherscan.io/txs?block=%s' % block
            num = get_num(url)
            time.sleep(1)
            for i in range(num):
                url = 'https://etherscan.io/txs?block=%s&p=%s' % (block, i+1)
                get_msg(db, url)
                time.sleep(2)
            block -= 1
        except Exception as e:
            logging.error(e)
            block -= 1
            continue
    db.close()