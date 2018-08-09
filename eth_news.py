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
from flask import Flask
from flask_cors import *
from flask import jsonify


app = Flask(__name__)
CORS(app, supports_credentials=True)
# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='eth_news.log',
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


# 插入语句
def insert_db(db, txHash, txReceipt, block, timeStamp, From, to, value, limit, used, price, actual, nonce, inputData):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 插入语句
    sql = "INSERT INTO eth_news VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) on duplicate key update txHash = values(txHash)"
    par = (txHash, txReceipt, block, timeStamp, From, to, value, limit, used, price, actual, nonce, inputData)
    try:
        # 执行sql语句
        cursor.execute(sql, par)
        # 提交到数据库执行
        db.commit()
        logging.info("insert success")
    except Exception as e:
        logging.error(e)
        db.rollback()


# 查询语句
def query_db(db, txHash):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    sql = "SELECT * FROM Student WHERE txHash = %s" % txHash
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            return row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]
    except Exception as e:
        logging.error(e)


# 关闭数据库
def close_db(db):
    db.close()


# 提供抓取接口
@app.route('/address/<site>/', methods=["POST"])
def get_msg(site):
    baseurl = 'https://etherscan.io/tx/'
    try:
        res = requests.get(baseurl+site, timeout=30)
    except Exception as e:
        logging.error(e)
        return "请求失败，请重试"
    html = etree.HTML(res.content)
    txHash = html.xpath("//div[@id='tx']")[0].text
    txReceipt = html.xpath("//span/font")[0].text
    block1 = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][3]/a")[0].text
    block2 = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][3]/span")[0].text
    block = block1+'('+block2+')'
    ht = str(res.content, encoding='utf-8')
    strTime = re.search(r'<span id="clock"></span>(.*?)</span>', ht)
    timeStamp = strTime.group(1)
    From = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][5]/a")[0].text
    to = html.xpath("//a[contains(@class, 'wordwrap')]")[0].text
    value_ls = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][7]/span")
    value = value_ls[0].xpath('string(.)').replace("\n", "")
    limit = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][8]/span")[0].text.replace("\n", "")
    used = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][9]/span")[0].text.replace("\n", "")
    price_ls = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][10]/span")
    price = price_ls[0].xpath('string(.)').replace("\n", "")
    actual_ls = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][11]/span")
    actual = actual_ls[0].xpath('string(.)').replace("\n", "")
    nonce1 = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][12]/span[1]")[0].text
    nonce2 = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][12]/span[2]")[0].text
    nonce = nonce1 + nonce2
    inputData = html.xpath("//textarea")[0].text
    try:
        db = connect_db()
        insert_db(db, txHash, txReceipt, block, timeStamp, From, to, value, limit, used, price, actual, nonce, inputData)
    except Exception as e:
        close_db(db)
        logging.info(e)
    close_db(db)
    return "success"


# 从数据库查询
@app.route('/address/<site>/', methods=["GET"])
def select_msg(site):
    # 使用cursor()方法获取操作游标
    db = connect_db()
    cursor = db.cursor()
    sql = "SELECT * FROM eth_news WHERE txHash = '%s'" % site
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        row = cursor.fetchone()
        # print(row)
    except Exception as e:
        logging.error(e)
        return "failed"
    dic = {
        "txHash": row[0],
        "txReceipt": row[1],
        "block": row[2],
        "timeStamp": row[3],
        "From": row[4],
        "to": row[5],
        "value": row[6],
        "limit": row[7],
        "used": row[8],
        "price": row[9],
        "actual": row[10],
        "nonce": row[11],
        "inputData": row[12],
    }
    if dic:
        close_db(db)
        return jsonify(dic)
    else:
        close_db(db)
        return 'dic is null'


if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=5000, debug=True)
    # a = get_msg('0xf4a42c5afca3fc44a119c02d399364d07292bf67d369817496372f08709f6df0')
    # print(a)

