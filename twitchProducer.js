// DEPENDENCY
var irc = require('irc');
var https = require('https');
var _ = require('lodash');
var channelService = require('./channelService');
var env = require('./env.json')
var kafka = require('kafka-node');

var Producer = kafka.HighLevelProducer;
var client = new kafka.Client(env.KAFKA_CONNECTION_STRING);
var producer = new Producer(client);

producer.on("ready", function(){
    channelService.get500(function(err,channels){
        if (err){
            console.log("There was a problem accessing Twitch REST API:", err)
            process.exit(1)
        }

        var client = new irc.Client('irc.chat.twitch.tv', env.TWITCH_USERNAME, {
            channels: channels,
            password: env.TWITCH_OAUTH
        });

        client.addListener('message', function (from, to, message) {
            var message = [{
                topic: 'topic',
                messages: '{"Time":"' + new Date().toISOString() + '","Channel":"' + to + '", "User":"' + from + '", "Message":"' + message + '"}'
            }];
            producer
                .send(message, function(err, data){
                    if(err) console.log("Error sending message to Kafka:", err);
                });
        });
    });
})
