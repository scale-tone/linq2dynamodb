(function () {
    'use strict';

    var filters = angular.module('mobilenotesapp.filters', []);

    // converts JSON dates like '/Date(1224043200000)/' , provided by WCF Data Services
    filters.filter('msJsonDate', function () {
        return function (input) {
            return new Date(parseInt(input.substr(6)));
        };
    });

})();