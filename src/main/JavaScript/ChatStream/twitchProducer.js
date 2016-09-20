// DEPENDENCY
var irc = require('irc');
var https = require('https');
var _ = require('lodash');
var channelService = require('./channelService');
var env = require('../../resources/application.json')
var timer = require('timers')
var kafka = require('kafka-node');

// Kafka Setup
var Producer = kafka.HighLevelProducer;
var client = new kafka.Client(env.KAFKA_CONNECTION_STRING);
var producer = new Producer(client);
var curChannels = [];

// Once our producer is available:
producer.on("ready", function(){

    // Connect to the Twitch.tv IRC server
    var client = new irc.Client('irc.chat.twitch.tv', env.TWITCH_USERNAME, {
        password: env.TWITCH_OAUTH
    });

    client.addListener('registered', function(message){

        var iterCount = 0;

        timer.setInterval(function connect(){

            // Get our top 1000 channels
            channelService.get500(function(err,channels){
                if (err){
                    console.log("There was a problem accessing Twitch REST API:", err)
                }
                else {
                    iterCount++;
                    var channelNames = _.map(channels, (x) => x.channel.name)
                    var curChannelNames = _.map(curChannels, (x) => x.channel.name)
                    var added = _.uniqBy(_.difference(channelNames,curChannelNames),(x) => x)
                    var addedString = _.reduce(added,(acc,x)=>acc+'#'+x+',','')

                    // console.log('remove:',removed,'add:',added)
                    _.forEach(channels, function(updated){
                        var match = _.findIndex(curChannels, (x) => x.channel.name === updated.channel.name)

                        if(match >= 0){
                            curChannels.splice(match,1,updated);
                        }
                        else {
                            curChannels.push(updated);
                        }
                    });

                    if(!!addedString){
                        client.join(addedString);
                    }
                    // else if (iterCount > 4){
                    //     iterCount = 0;
                    //
                    //     console.log('cleanup')
                    //     var curChannelNames = _.map(curChannels, (x) => x.channel.name);
                    //     var removed = _.map(_.uniqBy(_.difference(curChannelNames,channelNames),(x) => x), (x) => '#'+x);
                    //
                    //     curChannels = channels;
                    //     if(!!removed){
                    //         var date = new Date().toISOString();
                    //         _.forEach(removed,function(channel){
                    //             client.part(channel,function(nick,reason,message){console.log(date,' left:',channel)});
                    //         })
                    //     }
                    // }

                }
            });
        } , 10000);


        // client.join('#lirik')
        // timer.setInterval(function(){client.part('#lirik')},7000)
    });

    client.addListener('message', function (from, to, message) {
        var channelName = to.slice(1,to.length);
        var channelData = _.find(curChannels, (x) => x.channel.name === channelName)
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
