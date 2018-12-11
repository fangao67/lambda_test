#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
import urllib.parse
import boto3
import pandas as pd
import numpy as np
import os
import time

import logging
import pymysql
from DBUtils.PooledDB import PooledDB

logger = logging.getLogger()
logger.setLevel(logging.INFO)

print('Loading function')
start = time.time()
s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')

try:
    with open('config.json', 'r') as f:
        conf = f.read()
    conf = json.loads(conf)
    print(conf)
    master_host = conf['master_host']
    master_username = conf['master_username']
    master_password = conf['master_password']
    master_db = conf['master_db']
    master_table1_columns = conf['master_table1_columns']
    master_table2_columns = conf['master_table2_columns']
    
    master_table1 = conf['master_table1']
    master_table2 = conf['master_table2']
    tn1 = ','.join(master_table1_columns)
    tn2 = ','.join(master_table2_columns)

except Exception as e:
    logger.exception('parse config.json failed.')
    exit(-1)

master_pool = PooledDB(creator=pymysql, mincached=1, maxcached=1, charset='utf8',
                       host=master_host, port=3306, user=master_username,
                       passwd=master_password, db=master_db, autocommit=True)

# 从事件信息中获取bucket和文件路径key
def s3_info(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    return bucket,key

def sqs_info(event):
    sqs_records = event['Records'][0]['body']

    bucket = sqs_records.split('"name":"')[1].split(',')[0].replace('"','')
    key = urllib.parse.unquote_plus(sqs_records.split('"key":"')[1].split(',')[0].replace('"',''), encoding='utf-8')

    return bucket, key


# 通过s3_select 进行文件读取
def s3_select(bucket, key):
    r = s3_client.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        Expression='select * from s3object',
        InputSerialization={
            "CSV": {
                "FileHeaderInfo": "None"
            }
        },
        OutputSerialization={
            "CSV": {
                'QuoteFields': 'ALWAYS',
            }
        },
    )
    str1 = ''
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            str1 += records
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Stats details bytesScanned: ")
            print(statsDetails['BytesScanned'])
            print("Stats details bytesProcessed: ")
            print(statsDetails['BytesProcessed'])

        return str1


# 将s3上文件构造成frame
def create_frame(records,conditions, batchID, projectId, docID,master_cursor):
    lines = records.split('\n')
    names = lines[0].replace('"', '').replace(' ','_').split(',')
    danwei = lines[1].replace('"', '').split(',')
    values = [line.replace('"', '').split(',') for line in lines[2:]]

    try:
        df = pd.DataFrame(values, columns=names)
        tc = df.shape[1]
        df1 = df.astype("float")
        make_frame(names,danwei,df1, conditions, batchID, projectId, docID,tc,master_cursor)
    except Exception as e:
        print('file:%s %s' % (key, e))
        end1 = time.time()
        works1 = end1-start
        tv = [projectId,batchID,conditions,docID,tc,1,e,works1]
        sql = "insert into %s (%s) values (%s);" % (master_table1,tn1,str(tv).replace('[', '').replace(']', ''))
        print(sql)
        master_cursor.execute(sql)

# 将最终frame来求聚合值
def make_frame(names,danwei, df1, conditions, batchID, projectId, docID,tc,master_cursor):
    odf = df1.aggregate(['var', 'mean', 'max', 'min'])

    spped_tu = (odf.ix[0, 0] / odf.ix[1, 0])
    Met_v_hub = ['Met_v_hub', ' m/s',conditions, batchID, projectId, docID, spped_tu, spped_tu, spped_tu, spped_tu]

    values = [[names[i], danwei[i], conditions, batchID, projectId, docID] + list(odf.T.values[i]) for i in range(tc)]
    values.append(Met_v_hub)

    value = str(values).replace('[[', '').replace(']]', '').replace('[', '(').replace(']', ')')
    sql1 = "insert into %s (%s) values (%s);" % (master_table2,tn2, value)
    print(sql1)
    master_cursor.execute(sql1)

    end2 = time.time()
    works2 = end2-start
    tv = [projectId,batchID,conditions,docID,tc,0,'successed',works2]
    sql2 = "insert into %s (%s) values (%s);" % (master_table1,tn1,str(tv).replace('[', '').replace(']', ''))
    print(sql2)
    master_cursor.execute(sql2)


# 时间显示转化
def s2dt(ts, fmt1 = '%Y-%m-%d_%H-%M-%S',fmt2 = '%Y-%m-%d %H:%M:%S'):
    timeArray = time.strptime(ts, fmt1)
    return time.strftime(fmt2, timeArray)

# 从API中读取文件信息
def api_info(url,payload,variable):
    resp = requests.get(url, params=payload)
    data = resp.json()
    return data['Data'][variable]

#lambda 主函数方法
def lambda_handler(event, context):
    # TODO implement
#http://10.32.63.73:7777/api/lambda/all?action=Query&ProjectName=132/2500h90huaxian&BatchName=20180302-20180630&DocName=2018-01-01_01-10-00_DATA.csv&Condition=DLC012    
    bucket,key = s3_info(event)
    print(bucket)
    print(key)
    
    #payload = {'action':'Query','ProjectName':keylist[0],'BatchName':keylist[1],'DocName':keylist[3],'Condition':keylist[2]}
    #url = 'http://10.32.63.73:7777/api/lambda/all'
    #conditions = api_info(url,payload,"Condition")
    #batchID = api_info(url,payload,"BatchID")
    #projectId = api_info(url,payload,"ProjectID")
    #docID = api_info(url,payload,"DocID")

    conditions = 'GW1234'
    batchID = 7
    projectId = 7
    docID = 7

    master_conn = master_pool.connection()
    master_cursor = master_conn.cursor()

    records = s3_select(bucket, key)
    create_frame(records,conditions, batchID, projectId, docID,master_cursor)
    
    master_cursor.close()
    master_conn.close()



