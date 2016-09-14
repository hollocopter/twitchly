var async = require('async');
var _ = require('lodash');
var https = require('https');

var getStreams = function(offset, callback){
    https.get('https://api.twitch.tv/kraken/streams?limit=100&language=en&offset=' + offset, (res) => {
        var streams = ''
        res.on('data', (d) => {
            streams += d;
        });

        res.on('end', () => {
            obj = JSON.parse(streams)
            result = _.map(obj.streams, function(stream){
                return '#' + stream.channel.name;
            })
            callback(null,result)
        })
    })
}

module.exports.get500 = function(callback){
    async.parallel({
        one: function(cb){
            getStreams(0,cb);
        },
        two: function(cb){
            getStreams(100,cb);
        },
        three: function(cb){
            getStreams(200,cb);
        },
        four: function(cb){
            getStreams(300,cb);
        },
        five: function(cb){
            getStreams(400,cb);
        }
    }, function(err, results){
        var final = _.reduce(results, function(result,value){
            return result.concat(value)
        },[])
        callback(null, final);
    });
}
