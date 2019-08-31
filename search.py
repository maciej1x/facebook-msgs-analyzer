# -*- coding: utf-8 -*-
"""
Created on Thu Aug  1 13:28:46 2019

@author: admin
"""


import json
import os
import numpy as np
import pandas as pd
from collections import Counter
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

def get_input_dataframe(file):
    """
    get dataframe to functions
    input: file - json file (default: message_1.json)
    """
    data_dict = Messages()
    data_dict = data_dict.add_data(file)
    df = pd.DataFrame(data_dict['messages'])
    return df


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
    df_day[['date','messages']].plot(
            'date',
            legend=True,
            figsize=(15,10),
            title='Total messages per day',
            )


def get_members_stats(df):
    """
    returns dataframe with number of
    different types of messages
    for every member of consersation and total
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
    df_stats = df_stats.drop(columns=['type'])
    df_stats['timestamp_ms'] = pd.to_datetime(df_stats['timestamp_ms'], unit='ms')
    df_stats['timestamp_ms'] = df_stats.timestamp_ms.dt.to_period('M')
    df_stats = df_stats.groupby(by=['timestamp_ms', 'sender_name']).count()
    df_stats = df_stats.loc[:].sum(axis=1)
    df_stats = pd.DataFrame(df_stats)
    df_stats.columns = ['Messages']
    df_stats = df_stats.reset_index(level=['timestamp_ms', 'sender_name'])
    df_stats = decode_column(df_stats, 'sender_name')
    df_stats.columns = ['Month', 'User', 'Messages']

    #create final dataframe
    df_monthly = pd.DataFrame()
    months = np.arange(min(df_stats['Month']),
                       max(df_stats['Month'])+1,
                       dtype='datetime64[M]')
    df_monthly['Month'] = months
    df_monthly['Month'] = df_monthly.Month.dt.to_period('M')

    #add data for every user
    users = df_stats.User.unique()
    for user in users:
        df_temp = df_stats[df_stats['User']==user]
        df_temp = df_temp.drop(columns=['User'])
        df_temp.columns = ['Month', user]
        df_monthly = df_monthly.merge(df_temp, on='Month')
    df_monthly['Total']= df_monthly.sum(axis=1, numeric_only=True)
    df_monthly = df_monthly.fillna(0)
    return df_monthly


def plot_by_month_members(df, without_total, logaritmic):
    """
    plot chart for messages per month
    for every user
    df_monthly - input dataframe from get_members_stats_monthly()
    without_total - if skip Total column (True/False)
    logaritmic - if set yaxis logaritmic (True/False)
    """
    df_monthly = get_members_stats_monthly(df)
    xticks = np.arange(min(df_monthly['Month']),
                     max(df_monthly['Month'])+1,
                     dtype='datetime64[M]')
    if without_total == True:
        df_monthly = df_monthly.drop(columns='Total')
    df_monthly.plot(
            x='Month',
            legend=True,
            figsize=(15,10),
            title='Messages per month',
            xticks=xticks,
            logy=logaritmic
            )


def plot_by_month_total(df, logaritmic):
    """
    plot chart for total messages per month
    df_monthly - input dataframe from get_members_stats_monthly()
    logaritmic - if set yaxis logaritmic (True/False)
    """
    df_monthly = get_members_stats_monthly(df)
    xticks=np.arange(min(df_monthly['Month']),
                     max(df_monthly['Month'])+1,
                     dtype='datetime64[M]')
    df_monthly[['Month', 'Total']].plot(
            x='Month',
            legend=True,
            figsize=(15, 10),
            title='Messages per month',
            xticks=xticks,
            logy=logaritmic
            )


def number_of_reactions_for_members(df):
    """
    returns dataframe with number of
    different reactions sent by every member
    """
    c = df.loc[~pd.isnull(df.reactions)]['reactions'] #get only reactions
    #convert to dataframe with as many columns as max reactions
    #to one message
    d = pd.DataFrame.from_dict(c.array)
    #get working dataframe
    df_reactions = pd.Series(d.values.ravel('F')).dropna()
    df_reactions = pd.DataFrame.from_records(df_reactions)
    df_reactions = decode_column(df_reactions, 'reaction')
    actors = df_reactions.actor.unique()
    reactions = df_reactions.reaction.unique()

    #get final dataframe
    df_mem_react = pd.DataFrame(columns=reactions)
    for actor in actors:
        df_temp = df_reactions[df_reactions['actor']==actor]
        df_temp = df_temp.groupby('reaction').count()
        actor = decode(actor)
        df_temp.columns=[actor]
        df_mem_react = df_mem_react.append(df_temp[actor])
    df_mem_react.index.names = ['Member']
    df_mem_react['Total']= df_mem_react.sum(axis=1, numeric_only=True)
    return df_mem_react


def plot_number_of_reactions_for_member(df):
    """
    bar chart for number_of_reactions_for_member()
    """
    df_mem_react = number_of_reactions_for_members(df)
    df_for_plot = df_mem_react.drop(columns='Total')
    ax = df_for_plot.plot.bar(
        legend=True,
        figsize=(10, 12),
        title='Reactions per member',
        subplots=False,
        logy=False,
        stacked=True,
        table=False,
        rot=0,
        )
    ax.set_ylabel('Number')
    ax.set_xlabel('\nMember')


def total_number_of_reactions(df):
    """
    returns Series with number of
    total usage of every reaction
    """
    df_mem_react = number_of_reactions_for_members(df)
    total_reactions = df_mem_react.sum()
    return total_reactions


def plot_number_of_reactions(df):
    """
    bar chart for total_number_of_reactions()
    """
    total_reactions = total_number_of_reactions(df).drop('Total')
    ax = total_reactions.plot.bar(
        legend=False,
        figsize=(15, 10),
        title='Count of every reaction',
        rot=0,
        )
    ax.set_ylabel('Count')
    ax.set_xlabel('\Reaction')



df = get_input_dataframe('message_1.json')