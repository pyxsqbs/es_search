#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import request
from elasticsearch import Elasticsearch
from app.handlees.index import create_table, batch_insert_es, update_one, delete_one, collapse_search
from app.errorhandler.index import res_success, res_failure, req_params_required
import requests
import json
from utils import logger

""" 商城相关API """

__author__ = '秦宝帅'


def es_search():
    req_json = request.get_json()
    print 'post req params : %s' % str(req_json)
    index_name = req_json.get('index_name') or 'product_name_index_smart'
    keyword = req_json.get('keyword')
    page_no = int(req_json.get('page_no')) or 0
    page_size = int(req_json.get('page_size')) or 10000
    sort_by = req_json.get('sort_by')
    desc = int(req_json.get('desc')) or 1
    not_required = req_params_required(['index_name', 'keyword'], req_json)
    if not_required:
        return res_failure('es search failure, because %s is necessary' % not_required)
    else:
        data = collapse_search(index_name, "fulltext", keyword, page_size, page_no, sort_by, desc)
        return res_success(data, 'es search success')


def es_delete():
    req_json = request.get_json()
    print 'post req params : %s' % str(req_json)
    index_name = req_json.get('index_name') or 'product_name_index_smart'
    sku = req_json.get("sku")
    not_required = req_params_required(['index_name', 'sku'], req_json)
    if not_required:
        return res_failure('es delete failure, because %s is necessary' % not_required)
    else:
        data = delete_one(index_name, 'fulltext', sku)
        return res_success(data, 'es delete success')


def es_update():
    req_json = request.get_json()
    logger.info('Params:' + json.dumps(req_json))
    print 'post req params : %s' % str(req_json)
    index_name = req_json.get('index_name') or 'product_name_index_smart'
    sku = req_json.get('sku')
    spu = req_json.get('spu') or "TmpSKU" + sku
    product_name = req_json.get("product_name")
    product_no = req_json.get("product_no")
    ts_score = req_json.get("ts_score")
    price = req_json.get("price")
    sales_volume = req_json.get("sales_volume")
    data_dict = {"sku": sku, "spu": spu, "product_name": product_name, "product_no": product_no, "ts_score": ts_score}
    not_required = req_params_required(['index_name', 'sku', 'product_name', 'product_no', 'price', 'sales_volume'],
                                       req_json)
    if not_required:
        return res_failure('es update failure, because %s is necessary' % not_required)
    else:
        data = update_one(index_name, 'fulltext', sku, data_dict)
        return res_success(data, 'es update success')


def es_update_index():
    req_json = request.get_json()
    logger.info('Params:' + json.dumps(req_json))
    print 'post req params : %s' % str(req_json)
    index_name = req_json.get('index_name') or 'product_name_index_smart'
    not_required = req_params_required(['index_name'], req_json)
    if not_required:
        return res_failure('es update index failure, because %s is necessary' % not_required)
    else:
        es_domain = 'http://localhost:9200/'
        es = Elasticsearch(hosts=[es_domain], timeout=5000)
        db_name = index_name
        table_name = 'fulltext'
        create_table(db_name, table_name, [
            ['product_name', 'text'],
            ['product_no', 'keyword'],
            ['ts_score', 'keyword'],
            ['sku', 'keyword'],
            ['spu', 'keyword'],
        ])
        count = batch_insert_es(es, db_name, table_name)
        return res_success(count, 'es update index success')


if __name__ == '__main__':
    pass
