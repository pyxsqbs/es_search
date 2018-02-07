#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import MySQLdb
from app.getenvconf.index import GetEnvConf
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
from app.errorhandler.index import res_failure, res_success

""" ES DIY API """

es_domain = 'http://localhost:9200/'
analyzer = "ik_max_word"
env = GetEnvConf()


def collapse_search(index_name, type_name, keyword, result_max_size, result_from, sort_by, desc):
    data_dict = {
        "query": {"match": {"product_name": keyword}},
        "size": result_max_size,
        "highlight": {
            "pre_tags": ["<tag1>", "<tag2>"],
            "post_tags": ["</tag1>", "</tag2>"],
            "fields": {
                "product_name": {},
                "sku": {},
                "spu": {},
                "product_no": {},
                "ts_score": {},
                "sales_volume": {},
                "price": {}
            }
        },
        "collapse": {
            "field": "spu" # 折叠去重操作 ，根据spu ,每一种spu只查询评分最高的一条数据 参考资料：https://elasticsearch.cn/article/132
        },
        "from": result_from
    }

    response = requests.post("%s%s/%s/_search" % (es_domain, index_name, type_name), json=data_dict)

    spu_dict = set()

    if response.status_code == 200 and 'error' not in response.json():
        res_obj = response.json()
        if res_obj['hits'] and res_obj['hits']['hits']:
            for item in res_obj['hits']['hits']:
                if sort_by not in item['_source'] or item['_source'][sort_by] is None:
                    item['_source'][sort_by] = 0.0
                else:
                    try:
                        item['_source'][sort_by] = float(item['_source'][sort_by])
                    except Exception as e:
                        raise e

            def _get_cmp_func(_sort_by, _desc):
                def _cmp(x, y):
                    if x['_source'][_sort_by] < y['_source'][_sort_by]:
                        return 1 if _desc else -1
                    elif x['_source'][_sort_by] == y['_source'][_sort_by]:
                        return 0
                    else:
                        return -1 if _desc else 1

                return _cmp

            res_obj['hits']['hits'].sort(_get_cmp_func(_sort_by=sort_by, _desc=int(desc)))

            ans_data = list()
            for item in res_obj['hits']['hits']:
                spu = item['_source']['spu']
                if spu not in spu_dict:
                    spu_dict.add(spu)
                    tmp_dict = item['_source']
                    tmp_dict['_score'] = item['_score']
                    ans_data.append(tmp_dict)

            page_no = int(result_from)
            page_size = int(result_max_size)

            return json.dumps(res_success(ans_data[page_no * page_size:(page_no + 1) * page_size], "es query success"))

    return json.dumps(res_failure("es query failure"))


def update_one(index_name, type_name, index, data_dict):
    return requests.post("%s%s/%s/%s" % (es_domain, index_name, type_name, index), json=data_dict).text


def delete_one(index_name, type_name, index):
    return requests.delete("%s%s/%s/%s" % (es_domain, index_name, type_name, index)).text


def create_table(index_name, type_name, args):
    global analyzer
    if index_name == "product_name_index_smart":
        analyzer = "ik_smart"

    # 使参数选项中  只要type为text 就插入analyzer、 search_analyzer项, 其他情况不变
    dsl = dict()
    dsl['properties'] = dict()
    dsl_props = dsl['properties']
    for i in args:
        dsl_props[i[0]] = dict()
        dsl_props_name = dsl_props[i[0]]
        dsl_props_name['type'] = i[1]
        if i[1] == 'text':
            dsl_props_name['analyzer'] = analyzer
            dsl_props_name['search_analyzer'] = analyzer
    # print dsl
    try:
        requests.delete('%s%s' % (es_domain, index_name))
    except StandardError, e:
        print e
    else:
        requests.put('%s%s' % (es_domain, index_name))
        requests.post('%s%s/%s/_mapping' % (es_domain, index_name, type_name), json=dsl)
        print 'create table %s success from %s' % (type_name, index_name)


def batch_insert_es(es, index_name, type_name):
    conn = MySQLdb.connect(host=env.db_host, user=env.db_user, passwd=env.db_pwd, db=env.db_db, charset="utf8")
    cursor = conn.cursor()
    sql = '''select T.product_name, T.sku, T.spu, T.product_no, t_s.score
                    from ( select t1.id as id,
                                t1.product_name as product_name,
                                t1.sku as sku,
                                t2.spu as spu,
                                t1.product_no as product_no
                                from quality_product t1 left join quality_product_spu t2
                                    on t1.sku=t2.sku
                                where t1.jd_state=1
                                group by t1.sku having count(*)=1 ) AS T
                        left join quality_product_score t_s
                        on T.product_no = t_s.product_no'''
    res = cursor.execute(sql)
    i = 0
    fetch_count = 1000
    row = cursor.fetchmany(fetch_count)
    while row:
        # actions
        actions = []
        for row_i in row:
            data_dict = {"sku": row_i[1], "spu": row_i[2], "product_name": row_i[0], "product_no": row_i[3],
                         "ts_score": row_i[4]}
            if row_i[2] is None:
                data_dict['spu'] = "TmpSKU" + row_i[1]
            action_i = {
                "_index": index_name,
                "_type": type_name,
                "_source": data_dict,
                "_id": row_i[1]
            }
            actions.append(action_i)
        success, _ = bulk(es, actions, index=index_name, raise_on_error=True)
        print('Performed %d actions' % success)
        i += 1
        row = cursor.fetchmany(fetch_count)
    conn.close()
    return str(i)


if __name__ == '__main__':
    # es = Elasticsearch(hosts=[es_domain], timeout=5000)
    # db_name = 'xi'
    # table_name = 'fulltext'
    # create_table(db_name, table_name, [['spu', 'text'], ['sku', 'keyword']])
    # batch_insert_es(es,db_name, table_name)
    pass
