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


app = Flask(__name__)
# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='eth_news.log',
                    filemode='a')


# 提供抓取接口
@app.route('/address/<site>/', methods=['GET', 'POST'])
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
    block = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][3]/a")[0].text
    ht = str(res.content, encoding='utf-8')
    strTime = re.search(r'<span id="clock"></span>(.*?)</span>', ht)
    timeStamp = strTime.group(1)
    From = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][5]/a")[0].text
    to = html.xpath("//a[contains(@class, 'wordwrap')]")[0].text
    value = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][7]/span")[0].text
    limit = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][8]/span")[0].text
    used = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][9]/span")[0].text
    price_ls = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][10]/span")
    price = price_ls[0].xpath('string(.)')
    actual_ls = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][11]/span")
    actual = actual_ls[0].xpath('string(.)')
    nonce = html.xpath("//div[contains(@class, 'col-sm-9 cbs')][12]/span")[0].text
    inputData = html.xpath("//textarea")[0].text
    # private = html.xpath("//div/font")[0].text
    dic = {
        "txHash": txHash,
        "txReceipt": txReceipt,
        "block": block,
        "timeStamp": timeStamp,
        "From": From,
        "to": to,
        "value": value.replace("\n", ""),
        "limit": limit.replace("\n", ""),
        "used": used.replace("\n", ""),
        "price": price.replace("\n", ""),
        "actual": actual.replace("\n", ""),
        "nonce": nonce,
        "inputData": inputData,
        "private": "<To access the private Note feature, you must be logged in>",
    }
    return str(dic)


if __name__ == '__main__':
    # get_msg('0xf4a42c5afca3fc44a119c02d399364d07292bf67d369817496372f08709f6df0')
    app.run(
        host='0.0.0.0',
        port=5000, debug=False)
