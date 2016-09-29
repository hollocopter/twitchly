//See: https://github.com/pablojim/highcharts-ng
var myapp = angular.module('myapp', ["highcharts-ng"]);

myapp.controller('myctrl', function ($scope, $timeout, $http, $interval) {


    var mapToViewers = function(dataPoint){
        return [convertDate(dataPoint),dataPoint.viewers]
    }

    var mapToChatFreq = function(dataPoint){
        return [convertDate(dataPoint),dataPoint.count]
    }

    var mapToEngagement = function(dataPoint){
        return [convertDate(dataPoint), dataPoint.count * 2 / (dataPoint.viewers/1000)]
    }

    var convertDate = function(dataPoint){
        var year = dataPoint.date.substring(0,4);
        var month = dataPoint.date.substring(4,6);
        var day = dataPoint.date.substring(6,8);
        return new Date(year, parseInt(month) - 1, day).getTime() + dataPoint.time
    }

    $scope.currentChannel = 'reynad27'
    $scope.currentRanking = 'mostActive'
    $scope.currentMapping = mapToEngagement
    $scope.top10 = [];


    $scope.update = function(channel){
        $scope.currentChannel = channel;
        $http.get('http://52.23.210.67/api/' + $scope.currentChannel + '/20160929')
            .success(function(data){
                $scope.chartConfig.title.text = data.channelName;
                $scope.chartConfig.series = [{
                    id: data.channelName,
                    data: data.channelViews.map($scope.currentMapping)
                }]
                $scope.chartConfig.options.dummy = new Date();
            })
    }

    $scope.updateTop10 = function(ranking){
        $http.get('http://52.23.210.67/api/' + ranking + '/')
            .success(function(data){
                $scope.top10 = data.top;
            });
    }

    $scope.updateTab = function(tab){
        $scope.currentRanking = tab.ranking;
        $scope.updateTop10(tab.ranking);
        $scope.currentMapping = tab.map;
    }



    $scope.update($scope.currentChannel);
    $scope.updateTop10($scope.currentRanking);


    $scope.tabs = [
        {
            name: "Most Viewed",
            ranking: 'mostViewed',
            map: mapToViewers
        },
        {
            name: "Most Active",
            ranking: 'mostActive',
            map: mapToChatFreq
        }
    ]

    $scope.chartConfig = {
        options: {
            chart: {
                zoomType: 'x'
            },
            rangeSelector: {
                buttons: [{
                    count: 10,
                    type: 'minute',
                    text: '10M'
                }, {
                    count: 1,
                    type: 'hour',
                    text: '1H'
                }, {
                    type: 'all',
                    text: 'Day'
                }],
                inputEnabled: false,
                selected: 0
            },
            credits: {enabled:false},
            navigator: {
                enabled: true
            },
            scrollbar: {liveRedraw: false}
        },
        series: [],
        title: {
            text: "test"
        },
        useHighStocks: true,
        func: function(chart) {
            $timeout(function() {
                chart.reflow();
            }, 0);
        }
    }

    $interval(function(){
        $scope.update($scope.currentChannel)
    }, 5000)
    $interval(function(){
        $scope.updateTop10($scope.currentRanking)
    }, 6000)
});
