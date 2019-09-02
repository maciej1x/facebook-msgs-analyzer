# Facebook Messages Analyzer
Get charts, statistics and pandas DataFrame from messages on facebook

## How to get messages?
You can download them from:<br />
Settings > Your Facebook Information > Download a copy of your information<br />
Then select JSON format and Messages.

## How to run?
Run setup.py
Load JSON file of one conversation (default name: messages_1.json) with function <b>get_input_dataframe()</b><br />

## Functions:
- [X] <b>get_input_dataframe</b> get pandas dataframe from json file (it is input for most of the functions)
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
- [X] <b>number_of_reactions_for_members</b> - returns dataframe with number of different reactions sent by every member and total
- [X] <b>plot_number_of_reactions_for_member</b> bar chart for number_of_reactions_for_members()
- [X] <b>total_number_of_reactions</b> returns dataframe with number of total usage of every reaction
- [X] <b>plot_number_of_reactions</b> bar chart for total_number_of_reactions()
- [X] <b>plot_messages_by_hour</b> bar chart for total messages sent in every hour

## To do
- [ ] WEBSITE<br />

## Additional info:
If you want to decode string to UTF-8 you can use <b>decode</b> function, or <b>decode_column</b> to decode entire column on dataframe.