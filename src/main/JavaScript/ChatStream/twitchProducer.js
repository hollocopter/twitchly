// DEPENDENCY
var irc = require('irc');
var https = require('https');
var _ = require('lodash');
var channelService = require('./channelService');
var env = require('../../resources/application.json')
var timer = require('timers')
var kafka = require('kafka-node');
var async = require('async')

// Kafka Setup
var Producer = kafka.HighLevelProducer;
var client = new kafka.Client(env.KAFKA_CONNECTION_STRING);
var producer = new Producer(client);
var connectedChannels = [];

// Once our producer is available:
producer.on("ready", function(){

    var client = new irc.Client('irc.chat.twitch.tv', env.TWITCH_USERNAME, {
        password: env.TWITCH_OAUTH
    });

    client.addListener('registered', function(message){
        var iteration = 0;

        timer.setInterval(function connect(){

            // Get our top 1000 channels
            channelService.get500(function(err,channels){
                if (err){
                    console.log("There was a problem accessing Twitch REST API:", err)
                }
                else if (channelValidator(channels)){
                    iteration++;

                    // Get channels we haven't joined yet
                    var added = _.uniqBy(_.differenceWith(channels,connectedChannels,channelComparator),(x) => x.channel.name)
                    // Get channels to disconnect from
                    var removed = _.uniqBy(_.differenceWith(connectedChannels,channels,channelComparator),(x) => x.channel.name)

                    var joinChannels = _.map(added, (x) => function(callback){
                        client.join('#' + x.channel.name, function(){
                            connectedChannels.push(x);
                            callback(null,null)
                        })
                    });

                    var partChannels = _.map(removed, (x) => function(callback){
                        client.part('#' + x.channel.name, function(){
                            connectedChannels.splice(_.findIndex(connectedChannels, (y) => x.channel.name === y.channel.name),1)
                            callback(null,null)
                        })
                    });

                    async.parallel(joinChannels, function(err, results){
                        if(err){
                            console.log('There was a problem joining a channel');
                        }
                        else {
                            // Update any existing channels with new data
                            _.forEach(channels,
                                (x) => connectedChannels.splice(_.findIndex(connectedChannels, (y) => x.channel.name === y.channel.name),1,x)
                            );

                            if (iteration > 18)
                            async.parallel(partChannels, function(err, results){
                                if(err){
                                    console.log('There was a problem parting a channel');
                                }
                                else {
                                    iteration = 0;
                                }
                            });
                        }
                    });

                }
            });
        } , 20000);
    });

    client.addListener('message', function (from, to, message) {
        var channelName = to.slice(1,to.length);
        var channelData = _.find(connectedChannels, (x) => x.channel.name === channelName)
        if(!channelData) return;
        var twitchMessage = {
            "Time": new Date().toISOString(),
            "Channel": channelName,
            "User": from,
            "Message": message,
            "Viewers": channelData.viewers,
            "Game": channelData.game,
            "Delay": channelData.delay
        }
        var message = [{
            topic: 'chat',
            messages: JSON.stringify(twitchMessage)
        }];
        // console.log(twitchMessage.User, twitchMessage.Channel)
        producer.send(message, function(err, data){
            if(err) console.log("Error sending message to Kafka:", err);
        });
    });
});

function channelComparator(a,b){
    return a.channel.name === b.channel.name;
}

function channelValidator(channels){
    return _.every(channels, (x) => !!x && !!x.channel && !!x.channel.name);
}
