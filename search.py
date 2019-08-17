# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 13:28:46 2019

@author: admin
"""


import json
import os
import pandas as pd
from collections import Counter
import copy
from datetime import datetime
from elasticsearch import Elasticsearch as es



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
    if count == 0:
        print('"{}" appeared: {} times'.format(word, count))
        return
    print('"{}" appeared: {} times'.format(word, count))
    for member in member_word:
        print('{}: {}  ({}%)'.format(decode(member), member_word[member], round(member_word[member]*100/count,1)))
    return member_word


def count_words(data_dict, words, members):
    """
    get number of messages in which appeard given words
    no case sensivite, input encoding: utf-8
    input: words = list of words
    for example ['apple', 'banana', 'orange']
    returns dict
    """
    member_words = {}
    for member in members:
        member_words[member] = 0
    for word in words:
        member_words = dict(Counter(count_word(data_dict, word, members)) + Counter(member_words))
    total=sum(member_words.values())
    print('===')
    print('Words: {}'.format(words))
    for member in member_words:
        print('{}: {}  ({}%)'.format(decode(member), member_words[member], round(member_words[member]*100/total,1)))
    return member_words


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
    print('Total stickers sent: {}'.format(count))
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
    print('Total photos sent: {}'.format(count))
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


def decode_column(df, column):
    """
    decodes every string value in column to utf-8
    for messages use column = 'content'
    for authors use column = 'sender_name'
    WARNING! for big dataframes it may take long
    """
    for index, row in df.iterrows():
        if isinstance(row[column], str):
            df.at[index, column] = decode(row[column])
    return df


def get_messages_by_day(df):
    """
    returns dataframe with messages summed by day
    """
    df_day = pd.Series(data=df['timestamp_ms'])
    df_day = df_day.dt.floor('D')
    df_day = df_day.value_counts()
    df_day = df_day.rename_axis('date')
    df_day = df_day.reset_index(name='messages')
    df_day = df_day.sort_values(by=['date'])
    return df_day


def plot_by_day(df_day):
    """
    plot chart for messages per day
    """
    df_day[['date','messages']].plot('date',
         legend=True,
         figsize=(15,10),
         title='Total messages per day',
         )


def get_messages_by_month(df):
    """
    returns dataframe with messages summed by year-month
    """
    df_month = get_messages_by_day(df)
    df_month = df_month.sort_index(inplace=False)
    for index, row in df_month.iterrows():

        year = pd.to_datetime(df_month['date'].values)[index].year
        month = pd.to_datetime(df_month['date'].values)[index].month
        if month < 10:
            month = '0' + str(month)
        df_month.at[index, 'year-month'] = str(year) + '-' + str(month)
    df_month = df_month.drop(columns='date')
    df_month = df_month[['year-month', 'messages']]
    df_month = df_month.groupby(['year-month'])['messages'].agg('sum')
    df_month = df_month.rename_axis('date')
    df_month = df_month.reset_index(name='messages')
    df_month = df_month.sort_values(by=['date'])
    return df_month


def plot_by_month(df_month):
    """
    plot chart for messages per month
    """
    df_month[['date','messages']].plot('date',
         legend=True,
         figsize=(15,10),
         title='Total messages per month',
         )


def get_members_stats(df):
    """
    returns dataframe with number of
    different types of messages
    for every member of consersation
    """
    members = list(df.sender_name.unique())
    df_members_stats = pd.DataFrame()
    for member in members:
        df_member = df.loc[df['sender_name'] == member]
        df_member = df_member.drop(columns=['reactions',
                                            'timestamp_ms',
                                            'type',
                                            'users'],
            axis=1)
        count = df_member.count()
        count['sender_name'] = member
        count = count.drop(labels='sender_name')
        total = pd.Series(count.sum(), index=['total'])
        member_series = pd.Series(data=decode(member), index=['member'])
        member_series = member_series.append([count, total])
        df_members_stats = df_members_stats.append(member_series, ignore_index=True)

    df_members_stats = df_members_stats.astype(int, errors='ignore')
    df_members_stats = df_members_stats[['member',
                                         'audio_files',
                                         'content',
                                         'files',
                                         'gifs',
                                         'photos',
                                         'plan',
                                         'share',
                                         'sticker',
                                         'videos',
                                         'total']]
    df_members_stats  = df_members_stats.rename(columns={'content':'text msgs'})
    return df_members_stats


def most_common_words(df, min_chacters, number_of_returned_words):
    """
    get most common words, and their count used in conversation
    df - input dataframe
    min_characters - minimum length of word to count
    number_of_returned_words - number of words to return
    returns list of tuples in format:
        [(word, count), (word2, count), ...]
    """
    c = df['content'].str.cat(sep=';')
    c = decode(c)
    c = c.replace(' ', ';')
    c = c.replace(',', '')
    c = c.replace('!', '')
    c = c.replace('?', '')
    c = c.replace('.', '')
    c = c.split(';')
    c = pd.DataFrame(c, columns=['text'])
    c['len'] = c.text.str.len()
    c = c[c.len >= min_chacters]
    most_com = Counter(c['text'].str.lower())
    most_com = most_com.most_common(number_of_returned_words)
    return most_com


def get_members_stats_monthly(df):
    """
    returns dataframe with number of
    messages sent by every member
    in every month
    """
    df_stats = pd.DataFrame(df, copy=True)
    df_stats['timestamp_ms'] = pd.to_datetime(df_stats['timestamp_ms'], unit='ms')
    df_stats['timestamp_ms'] = df_stats.timestamp_ms.dt.to_period('M')
    df_stats = df_stats.groupby(by=['sender_name', 'timestamp_ms']).size()
    # df_stats = pd.DataFrame(df_stats)
    # df_stats.columns = ['Member', 'Date', 'Count']
    return df_stats


file = 'message_1_full.json'

data_dict = Messages()
data_dict = data_dict.add_data(file)

df = pd.DataFrame(data_dict['messages'])

df_stats = get_members_stats_monthly(df)
print(df_stats.head())

df_stats.plot(legend=True,
     figsize=(15,10),
     title='Total messages per month',
     )