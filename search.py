# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 13:28:46 2019

@author: admin
"""


import json
import os
from datetime import datetime
from elasticsearch import Elasticsearch



def decode(string):
    """decode string to utf-8"""
    return string.encode('iso-8859-1').decode('utf-8')


class Messages:

    def add_data(self, json_source):
        """load data from json file"""
        with open(json_source) as json_file:
            self.data = json.load(json_file)
            data_dict = decode(json.dumps(self.data))
            data_dict = json.loads(data_dict)
            return data_dict

    def elastic(self):
        """index data to elasticsearch"""
        index = os.path.split(data_dict['thread_path'])[-1]
        for num, row in enumerate(data_dict['messages']):
            es.index(index=index.lower(), doc_type='messages', id=num, body=row)
            print(num)
            # print(ind1)




def get_members(data_dict):
    """
    get all members of conversation
    returns list
    """
    members =[]
    for element in data_dict['participants']:
        members.append(element['name'])
    return members


def get_other_members(data_dict, members):
    """get other memmbers (for example deleted)"""
    for msg in data_dict['messages']:
        if msg['sender_name'] not in members:
            members.append(msg['sender_name'])
            print(msg['sender_name'])
    return members



def count_msg(data_dict, members):
    """get total text messages send by each member"""
    member_msg = {}
    for member in members:
        member_msg[member] = 0
    for msg in data_dict['messages']:
        author = msg['sender_name']
        member_msg[author] += 1
    return member_msg



def print_report(data, members):
    """print total count of messages send by each member"""
    total=sum(data.values())
    print('=====')
    print('Total: {}'.format(total))
    for member in data:
        print('{}: {} ({}%)'.format(decode(member), data[member], round(data[member]*100/total,1)))
    print('=====')


def count_word(data_dict, word, members):
    """
    get number of messages in which appeard given word
    no case sensivite, input encoding: utf-8
    """
    member_word = {}
    for member in members:
        member_word[member] = 0
    count = 0
    for msg in data_dict['messages']:
        if 'content' in msg:
            if word.lower() in decode(msg['content']).lower():
                count += 1
                member_word[msg['sender_name']] += 1
                # print('{}: {}'.format(decode(msg['sender_name']), decode(msg['content'])))
    print('===')
    print('"{}" appeared: {} times'.format(word, count))
    for member in member_word:
        print('{}: {}  ({}%)'.format(decode(member), member_word[member], round(member_word[member]*100/count,1)))
    return member_word




def count_sticker(data_dict, members):
    """
    get number of stickers send by each member
    """
    member_sticker = {}
    for member in members:
        member_sticker[member] = 0
    count = 0
    for msg in data_dict['messages']:
        if 'sticker' in msg:
            count += 1
            member_sticker[msg['sender_name']] += 1
    print('===')
    print('Total stickers send: {}'.format(count))
    for member in member_sticker:
        print('{}: {}'.format(decode(member), member_sticker[member]))
    return member_sticker



def count_photos(data_dict, members):
    """
    get number of photos send by each member
    """
    member_photos = {}
    for member in members:
        member_photos[member] = 0
    count = 0
    for msg in data_dict['messages']:
        if 'photos' in msg:
            count += 1
            member_photos[msg['sender_name']] += 1
    print('===')
    print('Total photos send: {}'.format(count))
    for member in member_photos:
        print('{}: {}'.format(decode(member), member_photos[member]))
    return member_photos

def get_most_reacted_text(data_dict, members, min_reactions):
    """
    get messages with reactions <= min_reactions
    """
    for msg in data_dict['messages']:
        if 'content' in msg:
            if 'reactions' in msg and len(msg['reactions'])>= min_reactions:
                date = datetime.fromtimestamp(msg['timestamp_ms']/1000).strftime('%Y-%m-%d %H:%M:%S')
                print('{}: {} ({})'.format(decode(msg['sender_name']), decode(msg['content']), date))

def get_most_reacted_photos(data_dict, members, min_reactions):
    """
    get photos with reactions <= min_reactions
    """
    for msg in data_dict['messages']:
        if 'photos' in msg:
            if 'reactions' in msg and len(msg['reactions'])>= min_reactions:
                date = datetime.fromtimestamp(msg['timestamp_ms']/1000).strftime('%Y-%m-%d %H:%M:%S')
                print('{}: {} ({})'.format(decode(msg['sender_name']), msg['photos'][0]['uri'], date))


def print_elements(full_data):
    """
    print all messages
    """
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


es = Elasticsearch()
json_source = 'message_1.json'

data = Messages()
data_dict = data.add_data(json_source)
# data.elastic()

members = get_members(data_dict)
members_msg = count_msg(data_dict, members)
members_word = count_word(data_dict, 'ares', members)
members_stickers = count_sticker(data_dict, members)
members_photos = count_photos(data_dict, members)

# print_report(members_photos, members)
# get_most_reacted_text(data_dict, members, 4)
# get_most_reacted_photos(data_dict, members, 4)
