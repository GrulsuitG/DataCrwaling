# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
import pprint
import json

#elasticsearch 연결하는 method
def connect():
    with open('config.json', 'r') as f:
        config = json.load(f)

    port = config['elaport']
    username = config['elaname']
    pw = config['elapw']
    return Elasticsearch(
        hosts=[{'host': 'localhost', 'port': port}],
        http_auth=(username, pw))

#index를 만드는 method
#중복이면 만들지 않는다.
def create_index(es, index):
    setting = {
        "settings": {
            "analysis": {
                "analyzer":{
                    "nori" : {
                        "tokenizer" : "nori_t",
                        "filter" : [
                            "nori_readingform"
                        ]
                    }
                },
                "tokenizer": {
                    "nori_t": {
                        "type": "nori_tokenizer",
                        "decompound_mode": "mixed",
                        "discard_punctuation": "true",
                    }
                }
            
            }
        },
        "mappings" : {
                "properties" : {
                    "keywordID" : {
                        "type" : "keyword"
                    },
                    "contents" : {
                        "type" : "text",
                        "fields" : {
                            "nori" : {
                                "type" : "text",
                                "analyzer" : "nori"
                            }
                        }
                    },
                    "keyword" : {
                        "type" : "keyword",
                    },
                    "link" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }
                        }
                    },
                    "press" : {
                        "type" : "keyword",
                    },
                    "reporter" : {
                        "type" : "keyword",
                    },
                    "title" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }
                        }
                    },
                    "type" : {
                        "type" : "keyword",
                    },
                    "writeDate" : {
                        "type" : "text",
                        "fields" : {
                            "keyword" : {
                                "type" : "keyword",
                                "ignore_above" : 256
                            }
                        }
                    }
                }
        }
    }            

    if not es.indices.exists(index=index):
        return es.indices.create(index=index,body=setting)
    
#index를 지우는 method
#만약 없다면 지우지 않는다.
def delete_index(es, index):
    if es.indices.exists(index=index):
        return es.indices.delete(index=index)
    
#data를 입력하는 method
def insert(es, index, data):
    #중복된 데이터가 있는지 확인하는 query
    body = (
        {
            "query":{
                "bool":{
                    "must":[
                        {"match":{'keyword': data['keyword']}},
                        {"match":{'title': data['title']}}
                    ]
                }
            }
        }
    )
    res = es.search(index=index, body=body)['hits']['max_score']
    if res is None:
        return es.index(index=index, body=data)
    else:
        return None

#data를 지우는 method
def delete(es, index, data):
    if data is None:
        data = {"match_all": {}}
    else:
        data = {"match": data}
    body = {"query": data}
    return es.delete_by_query(index, body=body)

#data를 검색하는 method
################################################################
#                                                              #      
#                                                              #      
# include = 포함되는 단어들을 list 형태로 입력                 #
# exclude = 들어가면 안되는 단어들을 list 형태로 입력          #
#                                                              #      
#                                                              #      
################################################################
def search(es, index, include = None, exclude = None):
    condition = {}
    if include is not None:
        condition.update(search_include(include))
    if exclude is not None:
        condition.update(search_exclude(exclude))
    
    if condition == {}:
        query = {
            "match_all":{}
        }
    else:
        query = {
            "bool" : condition,
        }
        
    body = {"size":1000,"query":query, }
    res = es.search(index=index, body=body)
    return res
 
def search_include(include_text):
    length = len(include_text)
    body = {
        "must_not":[
            {"match": {"contents":t} for t in include_text },
        ]
    } 
    return body
  
def search_exclude(exclude_text):
    length = len(exclude_text)
    body = {
        "should":[
            {"match": {"contents":t} }for t in exclude_text
        ]
    }
    return body

def get_NonExclude(es, index, keyword, exclude):
    text = ""
    for t in exclude:
        text = text + " " + t
    text = text.strip()
    
    body = {
        "query":{
            "bool":{
                "must_not":[
                    {"match":{"contents.nori":text}}
                ],
                "must":{
                    "match":{"keyword":keyword}}
            }
        },
        "size":1000
    }
    return es.search(index=index,body=body)

def get_exclude(es, index, keyword, exclude):
    text = ""
    for t in exclude:
        text = text + " " + t
    text = text.strip()
    
    body = {
        "query":{
            "bool":{
                "filter":[
                    {"match":{"contents.nori":text}},
                    {"match":{"keyword":keyword}}
                ]
            }
        },
        "size":1000
    }
    return es.search(index=index,body=body)