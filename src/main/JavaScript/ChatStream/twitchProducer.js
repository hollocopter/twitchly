// DEPENDENCY
var irc = require('irc');
var https = require('https');
var _ = require('lodash');
var channelService = require('./channelService');
var env = require('../../resources/application.json')
var kafka = require('kafka-node');

// Kafka Setup
var Producer = kafka.HighLevelProducer;
var client = new kafka.Client(env.KAFKA_CONNECTION_STRING);
var producer = new Producer(client);

// Once our producer is available:
producer.on("ready", function(){

    // Get our top 500 channels
    channelService.get500(function(err,channels){
        if (err){
            console.log("There was a problem accessing Twitch REST API:", err)
            process.exit(1)
        }

        // Connect to the Twitch.tv IRC server
        var client = new irc.Client('irc.chat.twitch.tv', env.TWITCH_USERNAME, {
            channels: channels,
            password: env.TWITCH_OAUTH
        });

        //When a message comes in to a channel:
        client.addListener('message', function (from, to, message) {
            var twitchMessage = {
                "Time": new Date().toISOString(),
                "Channel": to.slice(1,to.length),
                "User": from,
                "Message": message
            }
            var message = [{
                topic: 'chat',
                messages: JSON.stringify(twitchMessage)
            }];

            producer.send(message, function(err, data){
                if(err) console.log("Error sending message to Kafka:", err);
            });
        });
    });
})
