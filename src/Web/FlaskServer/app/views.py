# jsonify creates a json representation of the response
from flask import jsonify
from flask_cors import CORS, cross_origin
import redis
import json

from app import app
CORS(app)

with open('../../main/resources/application.json', 'r') as f:
    config = json.load(f)

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

lbp = WhiteListRoundRobinPolicy(config['CASSANDRA_WHITELIST'])
# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster([config['CASSANDRA_DNS']],load_balancing_policy=lbp)

# Change the bolded text to the keyspace which has the table you want to query. Same as above for < or > and quotations. Also delete this comment line
session = cluster.connect('twitchly')

redisConnection = redis.StrictRedis(host=config['REDIS_IP'], port=6379, db=0)


@app.route('/')
@app.route('/index')
def index():
  return "Hello, World!"

@app.route('/api/<channel>/<date>')
def get_channel_data(channel, date):
       stmt = "SELECT * FROM streamStatus WHERE channel = %s AND date = %s"
       response = session.execute(stmt, parameters=[channel, date])
       response_list = []
       for val in response:
            response_list.append(val)
       jsonresponse = [{"date":x.date, "count": x.count, "time": x.time, "viewers":x.viewers, "messageLength":x.messagelength} for x in response_list]
       return jsonify(channelName=channel,channelViews=jsonresponse)

@app.route('/api/mostViewed/')
def get_most_viewed():
       response = redisConnection.zrange('ViewerCount',0,9,True,True)
       jsonresponse = [{"name": x[0], "value":x[1]} for x in response]
       return jsonify(top=jsonresponse)

@app.route('/api/mostActive/')
def get_most_active():
       response = redisConnection.zrange('MessageCount',0,9,True,True)
       jsonresponse = [{"name": x[0], "value":x[1]} for x in response]
       return jsonify(top=jsonresponse)

@app.route('/api/mostEngaged/')
def get_most_engaged():
       response = redisConnection.zrange('Engagment',1,10,True,True)
       jsonresponse = [{"name": x[0], "value":x[1]} for x in response]
       return jsonify(top=jsonresponse)

@app.route('/api/mostSpammed/')
def get_most_spammed():
       response = redisConnection.zrange('Spam',1,10,True,True)
       jsonresponse = [{"name": x[0], "value":x[1]} for x in response]
       return jsonify(top=jsonresponse)

@app.route('/api/flushRedis/')
def flush_redis():
       redisConnection.flushall()
       return "Flushed Redis"
