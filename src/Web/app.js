//See: https://github.com/pablojim/highcharts-ng
var myapp = angular.module('myapp', ["highcharts-ng"]);

myapp.controller('myctrl', function ($scope, $timeout, $http, $interval) {

    var update = function(){
        $http.get('http://52.23.210.67/api/overwatchopen/20160926')
            .success(function(data){
                $scope.chartConfig.title.text = data.channelViews[0].channel;
                $scope.chartConfig.series = [{
                    id: data.channelViews[0].channel,
                    data: data.channelViews.map(function(x){
                        var year = x.date.substring(0,4);
                        var month = x.date.substring(4,6);
                        var day = x.date.substring(6,8);
                        return [new Date(year, parseInt(month) - 1, day).getTime() + x.time,x.count]
                    })
                }]
            })
    }

    update();

    // $http.get('http://52.23.210.67/api/overwatchopen/20160926')
    //     .success(function(data){
    //         $scope.chartConfig.title.text = data.channelViews[0].channel;
    //         $scope.chartConfig.series = [{
    //             id: data.channelViews[0].channel,
    //             data: data.channelViews.map(function(x){
    //                 var year = x.date.substring(0,4);
    //                 var month = x.date.substring(4,6);
    //                 var day = x.date.substring(6,8);
    //                 return [new Date(year, parseInt(month) - 1, day).getTime() + x.time,x.viewers]
    //             })
    //         }]
    //     })
            // var date = data.channelViews[0].date;
            //
            // console.log(data.channelViews.map(function(x){
            //     var year = x.date.substring(0,4);
            //     var month = x.date.substring(4,6);
            //     var day = x.date.substring(6,8);
            //     return [new Date(new Date(year, month, day).getTime() + x.time),x.viewers]
            // }
            // ))


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
                    text: 'day'
                }],
                inputEnabled: false,
                selected: 0
            },
            navigator: {
                enabled: true
            }
        },
        series: [],
        title: {
            text: "test"
        },
        useHighStocks: true
    }

    // $scope.chartConfig.series.push({
    //     id: 1,
    //     data: [
    //         [1147651200000, 23.15],
    //         [1147737600000, 23.01],
    //         [1147824000000, 22.73],
    //         [1147910400000, 22.83],
    //         [1147996800000, 22.56],
    //         [1148256000000, 22.88],
    //         [1148342400000, 22.79],
    //         [1148428800000, 23.50],
    //         [1148515200000, 23.74],
    //         [1148601600000, 23.72],
    //         [1148947200000, 23.15],
    //         [1149033600000, 22.65]
    //     ]
    // }
    // );



        // $scope.options = {
        //     renderer: 'area'
        // };
        // $scope.series = [{
        //         name: 'Series 1',
        //         color: 'steelblue',
        //         data: [{x: 0, y: 23}, {x: 1, y: 15}, {x: 2, y: 79}, {x: 3, y: 31}, {x: 4, y: 60}]
        //     }];

    $interval(update, 10000)
    //
    // function updateDisplay(data){
    //     switch (data.State.Code){
    //         case 16: $scope.displayClass.panel = "panel panel-success"; break;
    //         case 32:
    //         case 48:
    //         case 64:
    //         case 80: $scope.displayClass.panel = "panel panel-danger"; break;
    //         case 0:
    //         default: $scope.displayClass.panel = "panel panel-default";
    //     }
    //     if(data.State.Code != 16){
    //         $scope.stopDisabled = true;
    //     }
    //     if(data.State.Code == 16){
    //         $scope.stopDisabled = false;
    //     }
    // }

});
