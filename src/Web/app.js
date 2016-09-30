var myapp = angular.module('myapp', ["highcharts-ng"]);

myapp.controller('myctrl', function ($scope, $timeout, $http, $interval) {

    // Get the current date to display by default
    var curDate = new Date();
    var year = curDate.getUTCFullYear();
    var month = '0' + (curDate.getUTCMonth() + 1);
    var date = '0' + curDate.getUTCDate();
    month = month.substring(month.length - 2);
    date = date.substring(date.length - 2);


    var dateString = '' + year + month + date

    /* Map historical data set various ways */
    var mapToViewers = function(dataPoint){
        return [convertDate(dataPoint),dataPoint.viewers]
    }

    var mapToChatFreq = function(dataPoint){
        return [convertDate(dataPoint),dataPoint.count]
    }

    var mapToEngagement = function(dataPoint){
        return [convertDate(dataPoint), dataPoint.count * 2 / (dataPoint.viewers/1000)]
    }

    var mapToSpam = function(dataPoint){
        return [convertDate(dataPoint), dataPoint.messageLength / dataPoint.count]
    }

    // Date conversion from YYYYMMDD
    var convertDate = function(dataPoint){
        var year = dataPoint.date.substring(0,4);
        var month = dataPoint.date.substring(4,6);
        var day = dataPoint.date.substring(6,8);
        return new Date(year, parseInt(month) - 1, day).getTime() + dataPoint.time
    }

    // Set up some defaults
    $scope.currentChannel = 'reynad27'
    $scope.currentRanking = 'mostActive'
    $scope.currentMapping = mapToChatFreq
    $scope.currentTab = 'Most Active'
    $scope.top10 = [];

    // Pull historical data to update the graph
    $scope.update = function(cb){
        $http.get('http://52.23.210.67/api/' + $scope.currentChannel + '/' + dateString)
            .success(function(data){
                $scope.chartConfig.title.text = data.channelName;
                $scope.chartConfig.series = [{
                    id: data.channelName,
                    data: data.channelViews.map($scope.currentMapping)
                }]
                cb();
            })
    }

    // Pull top 10
    $scope.updateTop10 = function(ranking, cb){
        $http.get('http://52.23.210.67/api/' + ranking + '/')
            .success(function(data){
                $scope.top10 = data.top;
                cb();
            });
    }

    // When a tab is selected
    $scope.updateTab = function(tab){
        $scope.currentRanking = tab.ranking;
        $scope.updateTop10(tab.ranking,function(){$scope.update(function(){return})});
        $scope.currentMapping = tab.map;
        $scope.currentTab = tab.name;
    }

    // When a channel is selected
    $scope.updateChannel = function(channel){
        $scope.currentChannel = channel;
        $scope.update(function(){
            // redraw the chart
            $scope.chartConfig.options.dummy = new Date();
        });
    }

    // On load, get the top 10 and draw the top channel graph
    $scope.updateTop10($scope.currentRanking, function(){
        $scope.currentChannel = $scope.top10[0].name;
        $scope.update(function(){});
    });

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
        },{
            name: "Most Engaged",
            ranking: 'mostEngaged',
            map: mapToEngagement
        },{
            name: "Spam Rating",
            ranking: 'mostSpammed',
            map: mapToSpam
        }
    ]

    // Chart Configuration
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
            text: ""
        },
        useHighStocks: true,
        func: function(chart) {
            $timeout(function() {
                chart.reflow();
            }, 0);
        }
    }

    // Update the chart and top 10 every 5 seconds
    $interval(function(){
        $scope.update(function(){$scope.updateTop10($scope.currentRanking,function(){return})})
    }, 5000)

});
