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
- [X] <b>plot_by_day</b>plot chart for messages per day<br />
- [X] <b>get_messages_by_month</b>dataframe with messages summed by year-month<br />
- [X] <b>plot_by_month</b> plot chart for messages per month

## To do
- [X] merge setup.py and setup_pandas.py<br />
- [ ] get pandas dataframe with messages summed year-month grouped by user<br />
- [ ] plot chart for messages per month for every user

## Additional info:
If you want to decode string to UTF-8 you can use <b>decode</b> function, or <b>decode_column</b> to decode entire column on dataframe.
