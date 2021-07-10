#!/usr/bin/env python3
import os
import csv
import boto3
import json
import base64
import datetime
import gzip
import shutil
from dateutil import parser, relativedelta
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers

CFG_ENV = 'uat'

def configLoad():
    configClient = boto3.client('appconfig')
    resp = configClient.get_configuration(
        Application='BillingConfig',
        Environment=CFG_ENV,
        Configuration='BillingConfig',
        ClientId='billingLoad'
    )
    return json.loads(resp['Content'].read())

def csvFields(row, fields):
    for f in fields:
        vtype = f['type']
        if vtype.endswith('DateTime'):
            if f['field'] in row and row[f['field']]:
                row[f['field']] = parser.parse(row[f['field']])
            else:
                row[f['field']] = parser.parse(row['lineItem/UsageEndDate']) # set default DateTime to bill item time
        elif vtype.endswith('BigDecimal'):
            if f['field'] in row and row[f['field']]:
                row[f['field']] = float(row[f['field']])
            else:
                row[f['field']] = 0.0 # default value 0.0
        else: # bugfixs for optionalstring but recognized as data or other bugs
            if 'savingsPlan/StartTime' in row and row['savingsPlan/StartTime']: # make it string
                row['savingsPlan/StartTime'] = '"' + row['savingsPlan/StartTime'] + '"'
            if 'savingsPlan/EndTime' in row and row['savingsPlan/EndTime']: # make it string
                row['savingsPlan/EndTime'] = '"' + row['savingsPlan/EndTime'] + '"'


def csvLoad(filename, fields):
    lineitems = []
    with open(filename) as csvf:
        reader = csv.DictReader(csvf)
        for row in reader:
            csvFields(row, fields)
            lineitems.append(row)
    return lineitems

def csvESize(lineitems, index):
    return [{'_op_type': 'update', '_index': index, '_type': 'document', '_id': i['identity/LineItemId']+str(i['lineItem/UsageEndDate'].timestamp()), "doc": i, 'doc_as_upsert': True} for i in lineitems]

def esInit(escfg):
    url = escfg['scheme']+'://'+escfg['user']+':'+base64.b64decode(escfg['password']).decode()+'@'+escfg['host']
    client = Elasticsearch([url])
    return client

def esBulk(es, docs):
    helpers.bulk(es, docs)

def s3download(info, s3dir):
    s3 = boto3.client("s3")
    files = []
    resp = s3.list_objects_v2(Bucket=info["bucket"], Prefix=info['prefix']+'/'+s3dir)
    for o in resp['Contents']:
        if o['Key'].endswith('.gz'):
            save2 = info["bucket"]+"-"+os.path.basename(o['Key'])
            s3.download_file(info["bucket"], o['Key'], save2)
            with gzip.open(save2, 'rb') as f_in:
                with open(save2.rstrip('.gz'), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(save2)
            files.append(save2.rstrip('.gz'))
    return files

def accountsBilling(cfg, s3dir, es):
    for account in cfg['accounts']:
        files = s3download(account, s3dir)
        for f in files:
            docs = csvESize(csvLoad(f, cfg['fields']), account['index'])
            esBulk(es, docs)
            os.remove(f)

def main():
    global CFG_ENV
    CFG_ENV = os.getenv("BILLING_ENV", "uat")
    cfg = configLoad()
    es = esInit(cfg['es'])
    now = datetime.datetime.utcnow()
    s3dir = now.strftime("%Y%m")+"01-"+(now+relativedelta.relativedelta(months=1)).strftime("%Y%m")+"01"
    accountsBilling(cfg, s3dir, es)
    if now.day <= 5: # may change after month day
        s3dir = (now-relativedelta.relativedelta(months=1)).strftime("%Y%m")+"01-"+now.strftime("%Y%m")+"01"
        accountsBilling(cfg, s3dir, es)

if __name__ == '__main__':
    main()
