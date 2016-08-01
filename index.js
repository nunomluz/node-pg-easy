var pg = require('pg');
var parse = require('pg-connection-string').parse;

var Q = require('q');
var _ = require('underscore');

var utils = require('./utils.js');
var emitter = require('./eventemitter.js');

/*
    Abstraction on top of the pg module:
        - Promisifies the pg module in a way that makes it really easy to run transactions
*/
module.exports = function(opts) {
    
    if(!(opts && opts.connStr) && !process.env.PGCONNSTR) {
        throw 'Missing connection string! Supply it through the "connStr" option on in the "PGCONNSTR" environment variable.';
    }
    
    var opts = utils.deepExtend({
        connStr: process.env.PGCONNSTR,
        config: {
            port: 5432,
            max: 10,
            idleTimeoutMillis: 30000
        }
    }, {
        config: parse(opts.connStr || process.env.PGCONNSTR)
    }, opts);
    
    var pool = new pg.Pool(opts.config);
    var events = emitter.instance();

    // on error close the connection
    pg.on('error', function(err) {
        events.trigger('error', [err]); 
        console.error('PostgreSQL error!', err);
        pg.end();
    });
    
    function _query(query, args, client) {
        if(query.trim().indexOf(';', query.length - 1) < 0) {
            query += ';';
        }
                
        var deferred = Q.defer();
        
        client.query(query, args, function(err, result) {
            if(err) {
                events.trigger('error', [err, query, args, client, true]); 
                deferred.reject(err);
                return;
            }
            events.trigger('query', [query, args, result, client, true]); 
            deferred.resolve(result);
        });
        
        return deferred.promise;
    }

    var pgsql = {};
    
    pgsql.getOptions = function() {
        return opts;
    };
    
    pgsql.getDefaultOptions = function() {
        return DEFAULT_OPTIONS;
    };
    
    pgsql.begin = function() {
        var deferred = Q.defer();
        
        pool.connect(function(err, client, done) {
            if(err) {
                events.trigger('error', [err]); 
                deferred.reject(err);
                return;
            }

            function clientErrorHandler(err) {  
                events.trigger('error', [err]); 
                deferred.reject(err);
                return;
            }
            client.on('error', clientErrorHandler);
            
            var results = [];
            
            function commit() {
                return _query('COMMIT', [], client).then(function(res) {
                    done();
                    client.removeListener('error', clientErrorHandler);
                    events.trigger('transaction-finished', [{isCommit: true}, client]); 
                    return results;
                });
            }
            
            function rollback() {
                return _query('ROLLBACK', [], client).then(function(res) {
                    done();
                    client.removeListener('error', clientErrorHandler);
                    events.trigger('transaction-finished', [{isRollback: true}, client]); 
                    return res;
                });
            }
            
            function rollbackAndThrow(err) {
                _query('ROLLBACK', [], client).then(function(res) {
                    done();
                    client.removeListener('error', clientErrorHandler);
                    events.trigger('transaction-finished', [{isRollback: true}, client]); 
                    return res;
                }).done();
                throw err;
            }
            
            function execute(query, args) {
                return _query(query, args, client).then(function(res) {
                    results.push(res);
                    return res;
                });
            }

            client.query('BEGIN;', [], function(err, result) {   
                if(err) {
                    events.trigger('error', [err]); 
                    deferred.reject(err);
                    return;
                }

                events.trigger('transaction-started', []); 
                deferred.resolve({execute: execute, commit: commit, rollback: rollback, rollbackAndThrow: rollbackAndThrow});
            });

        });

        return deferred.promise;
    };
    
    pgsql.execute = function(query, args) {
        if(query.trim().indexOf(';', query.length - 1) < 0) {
            query += ';';
        }

        var deferred = Q.defer();

        pool.connect(function(err, client, done) {
            if(err) {
                events.trigger('error', [err]); 
                deferred.reject(err);
                return;
            }

            function clientErrorHandler(err) {  
                  deferred.reject(err);
                  return;
            }
            
            client.on('error', clientErrorHandler);

            client.query(query, args, function(err, result) {
                done();
                client.removeListener('error', clientErrorHandler);

                if(err) {
                    events.trigger('error', [err, query, args, client, false]); 
                    deferred.reject(err);
                    return;
                }

                events.trigger('query', [query, args, result, client, false]); 
                deferred.resolve(result);
            });

        });

        return deferred.promise;
    };

    pgsql.on = function(ev, cb) {
        return events.on(ev, cb);
    };
    
    return pgsql;
};
