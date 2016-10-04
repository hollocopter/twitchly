# Twitchly - Track your chat rooms in real-time.

Twitchly [twitchly.xyz] (http://twitchly.xyz) is a real-time chat room analytics platform powered by Kafka, Flink, Cassandra, Redis, Flask, and AngularJS. Twitchly allows advertisers or content creators to monitor Twitch.tv channel metrics in real-time or historically. These metrics can provide insight into which channels are the most exciting (and possibly most valuable) at any given time.

## Data Pipeline

![Pipeline](http://i.imgur.com/kAYcbO2.png)

## User Interface

![Interface](http://i.imgur.com/rg8HhhR.png)

The UI provides four tabs to view different metrics for each channel. The four tabs are:
 - Most Viewed: Channels with the most viewers.
 - Most Active: Channels with the most chat messages in the past 30 seconds.
 - Most Engaged: Channels with the highest engagement rating (messages per minute, per 1000 viewers).
 - Spam Rating: Channels with the longest average chat messages.

 For each tab, an updating top 10 channels for the given metric are displayed on the left side of the UI. Selecting a channel brings up a graph for that channel's historical data along with real-time updates.

## Ingestion

Ingestion is handled by a NodeJS Kafka producer script which maintains a connection to the 500 most popular irc channels on Twitch.tv at any given time. The raw messages are further augmented with data from Twitch.tv's REST API. The messages are then sent to Kafka for distribution over the cluster.

## Stream Processing

Apache Flink.

## Database

Cassandra was chosen as the database to store a historical time series of channel states. The table is partitioned by the channel name and the date, with entries for each time stamp for the day. This allows for efficient retrieval of a historical day's worth of data to populate our graph.

Redis was chosen as an efficient in-memory way to retain the top channels for each metric. One sorted set is kept for each metric in the application. This allows us to quickly update the top 10 channels for a given metric.
