# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 13:28:46 2019

@author: admin
"""


import json
import os
from elasticsearch import Elasticsearch

es = Elasticsearch()
json_source = 'message_1.json'
# json8 = 'message_1_utf_8.json'

def decode(string):
    return string.encode('iso-8859-1').decode('utf-8')

print('żłćęążń')
class Messages:

    def add_data(self, json_source):
        with open(json_source) as json_file:
            self.data = json.load(json_file)
            data_dict = decode(json.dumps(self.data))
            data_dict = json.loads(data_dict)
            return data_dict

    def elastic(self):
        index = os.path.split(data_dict['thread_path'])[-1]
        for num, row in enumerate(data_dict['messages']):
            es.index(index=index.lower(), doc_type='messages', id=num, body=row)
            print(num)
            # print(ind1)

data = Messages()
data_dict = data.add_data(json_source)
data.elastic()

# print(decode(full_data['title']), full_data['participants'])

# print(full_data['messages'][49986])
def print_elements(full_data):
    for num in range(len(full_data['messages'])):
        # print(num)
        # print(decode(full_data['messages'][num]['sender_name']))
        if 'content' in full_data['messages'][num]:
            pass
            # print('content: ')
            # print(decode(full_data['messages'][num]['content']))
        elif 'photos' in full_data['messages'][num]:
            pass
            # print('photos: ')
            # print(full_data['messages'][num]['photos'][0]['uri'])
        elif 'videos' in full_data['messages'][num]:
            pass
            # print('videos: ')
            # print(full_data['messages'][num]['videos'][0]['uri'])
        elif 'audio_files' in full_data['messages'][num]:
            pass
            # print('audio_files: ')
            # print(full_data['messages'][num]['audio_files'][0]['uri'])
        elif 'gifs' in full_data['messages'][num]:
            pass
            # print('gifs: ')
            # print(full_data['messages'][num]['gifs'][0]['uri'])
        elif 'sticker' in full_data['messages'][num]:
            pass
            # print('sticker: ')
            # print(full_data['messages'][num]['sticker']['uri'])
        elif 'files' in full_data['messages'][num]:
            pass
            # print('files: ')
            # print(full_data['messages'][num]['files']['uri'])
        elif 'plan' in full_data['messages'][num]:
            pass
            # print('plan: ')
            # print(full_data['messages'][num]['plan']['uri'])
        else:
            print(num)
            print(full_data['messages'][num])

# res = es.get(index='index_1', doc_type='users', id=1)
# print(res)

# res = es.delete(index='index_1', doc_type='users', id=10)
# print(res)

# for result in res['hits']['hits']:
#     print(result['_id'], result['_source']['name'])
# print(res['hits']['total'])

# res = es.search(index='data2', body={
#     "query": {
#     "bool": {
#         "must_not": [
#             {"match": {"bool": True}}
#             ],
#     "filter": [
#             { "range": { "age": { "gte": "49" }}}
#             ]
#             }
#         }
#     },
#     size = 100
# )

# for result in res['hits']['hits']:
#     print(result['_id'], result['_source']['name'], result['_source']['age'])
# print(res['hits']['total'])


# res = es.search(index='index_1', size=1000, body={
#     'query': {
#             'match':{'name': 'Thompson'}
#             }
#         }
#     )

# for result in res['hits']['hits']:
#     print(result['_id'], result['_source']['name'], result['_source']['age'])
# print(res['hits']['total'])

# res = es.search(index='index_2',
#                 body={
#                         'query':{
#                                 'match':{'gender': 'female'}}})
# for result in res['hits']['hits']:
#     print(result['_id'], result['_source']['name'])
