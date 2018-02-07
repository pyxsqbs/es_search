#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask
from app.getenvconf.index import GetEnvConf
import app.controller.mall as mall

app = Flask(__name__)
env = GetEnvConf()
# env.logger()


@app.route('/', methods=['GET'])
def get(): return 'es api should post form data'


@app.route('/es_search', methods=['POST'])
def es_search(): return mall.es_search()


@app.route('/es_delete', methods=['POST'])
def es_delete(): return mall.es_delete()


@app.route('/es_update', methods=['POST'])
def es_update(): return mall.es_update()


@app.route('/es_update_index', methods=['POST'])
def es_update_index(): return mall.es_update_index()


if __name__ == '__main__':
    app.run(host=env.host, port=env.port)
