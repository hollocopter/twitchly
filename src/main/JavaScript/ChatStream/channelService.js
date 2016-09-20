// DEPENDENCY
var async = require('async');
var _ = require('lodash');
var https = require('https');
var env = require('../../resources/application.json')

// Get the names of 100 streams (maximum via REST API)
var getStreams = function(offset, callback){
    var options = {
        hostname: 'api.twitch.tv',
        path: '/kraken/streams/?limit=100&offset=' + offset,
        method: 'GET',
        headers: {'Client-ID':env.TWITCH_CLIENT_ID}
    }
    https.get(options, (res) => {
        var streams = ''
        res.on('data', (d) => {
            streams += d;
        });

        res.on('end', () => {
            obj = JSON.parse(streams).streams;
            callback(null,obj)
        })
    })
}

// Get the names of the top 1000 streams
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
