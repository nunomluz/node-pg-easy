var _ = require('underscore');

var utils = {};

// a recursive deep _.extend
utils.deepExtend = function(out, arr) {
    if(arguments.length <= 0) {
        return {};
    }
    var out = arguments[0];
    if(arguments.length <= 1) {
        return out;
    }
    
    for(var i=1; i<arguments.length; i++) {
        var arr = arguments[i];
        if(arr && _.size(arr) > 0) {
            for(var key in arr) {
                if(!arr.hasOwnProperty(key)) {
                    continue;
                }
                if({}.toString.apply(arr[key]) === '[object Object]') {
                    out[key] = utils.deepExtend(out[key] || {}, arr[key]);
                } else {
                    out[key] = arr[key];
                }
            }
        }
    }
    
    return out;
};

module.exports = utils;