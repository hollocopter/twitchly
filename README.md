# Twitchly - Track your chat rooms in real-time.

Twitchly is a real-time chat room analytics platform powered by Kafka, Flink, Cassandra, Redis, Flask, and AngularJS. Twitchly allows advertisers or content creators to monitor Twitch.tv channel metrics in real-time or historically. These metrics can provide insight into which channels are the most exciting (and possibly most valuable) at any given time.

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

## Streaming

## Database
