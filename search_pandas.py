# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 15:33:52 2019

@author: admin
"""

import json
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from pandas import DataFrame
from search import decode, Messages

file = 'message_1_full.json'

# data_dict = Messages()
# data_dict = data_dict.add_data(file)
# msgs = data_dict['messages']

df = pd.DataFrame(msgs)
# df = df.set_index('timestamp_ms')
df['timestamp_ms'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
# print(a)
# content = df['content']
# print(df.info())
# print(df.head())
# print(df['content'][0])

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
    # df_month['year-month'] = str(pd.to_datetime(df_month['date'].values)[0].year) + '-' + str(pd.to_datetime(df_month['date'].values)[0].month)
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

# df_day = get_messages_by_day(df)
# print(df_day.head())
df_month = get_messages_by_month(df)
print(df_month.tail())
plot_by_month(df_month)
print(df_month.info())







