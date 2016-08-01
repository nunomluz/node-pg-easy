var eventEmitter = {};

eventEmitter.instance = function () {
    var handler = {
        listeners: {}
    };
    
    handler.trigger = function(ev, data) {
        if (handler.listeners[ev]) {
            for (var e = 0; e < handler.listeners[ev].length; e++) {
                var res = handler.listeners[ev][e].apply(this, data);
                if (res === false) {
                    return;
                }
            }
        }
    };
    
    handler.on = function(ev, cb) {
        var evs = ev.split(/\,/g);
        for (var e in evs) {
            if (!handler.listeners[evs[e]]) {
                handler.listeners[evs[e]] = [];
            }
            handler.listeners[evs[e]].push(cb);
        }
        return this;
    };
    
    handler.clear = function() {
        handler.listeners = {};
    };
                
    return handler;
};

module.exports = eventEmitter;