# Facebook Messages Analyzer
Get charts, statistics and pandas DataFrame from messages on facebook

## How to get messages?
You can download them from:<br />
Settings > Your Facebook Information > Download a copy of your information<br />
Then select JSON format and Messages.

## How to run?
Run setup.py
Load JSON file of one conversation (default name: messages_1.json)<br />

## Functions:
- [X] <b>elastic</b> index data to ElasticSearch<br />
- [X] <b>count_msg</b> get total text messages sent by each member<br />
- [X] <b>count_word</b> get number of messages in which appeard given word
- [X] <b>count_words</b> get number of messages in which appeard given words
- [X] <b>count_photos</b> get number of photos sent by each member
- [X] <b>get_most_reacted_text</b> get messages with certain minimum of reactions
- [X] <b>get_most_reacted_photos</b> get photos with certain minimum of reactions
- [X] <b>get_messages_by_day</b> get pandas dataframe with messages summed by day<br />
- [X] <b>plot_by_day</b> plot chart for messages per day<br />
- [X] <b>plot_by_month_members</b> plot chart for messages per month for every member and total(optional)
- [X] <b>plot_by_month_total</b> plot chart for total messages per month
- [X] <b>most_common_words</b> get most common words, and their count used in conversation
- [X] <b>get_members_stats_monthly</b> get number of messages sent by every member and total messages in every month

## To do
- [X] merge setup.py and setup_pandas.py
- [X] update <b>get_messages_by_month</b> (remove iterating by rows) - 19.08.2019 deleted and merged with get_members_stats_monthly
- [X] get pandas dataframe with messages summed year-month grouped by user
- [X] plot chart for messages per month for every user
- [X] count most used words
- [ ] get number of reactions given and received for every member

## Additional info:
If you want to decode string to UTF-8 you can use <b>decode</b> function, or <b>decode_column</b> to decode entire column on dataframe.