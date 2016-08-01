var chai = require('chai');
var assert = chai.assert;
var expect = chai.expect;

var dbCreate = require('../');

describe('node-pg-easy', function() {
    
    describe('init database', function() {
        this.timeout(10000);
        
        var db;
        
        before(function(done) {
            db = dbCreate({ connStr: 'postgres://postgres@localhost:5432/postgres' });
            done();
        });
    
        it('should create the database', function(done) {
            db.execute('DROP DATABASE IF EXISTS nodepgeasy;').then(function() {
                return db.execute('CREATE DATABASE nodepgeasy ENCODING = \'UTF8\' TEMPLATE=template0;');
            }).then(function() {
                done();
            }, function(err) {
                console.error(err.message, err.stack);
                expect(1).not.to.be.ok;
                done();
            }).done();
        });
    });
    
    describe('init tables and data', function() {
        this.timeout(10000);
        
        var db;
        
        before(function(done) {
            db = dbCreate({ connStr: 'postgres://postgres@localhost:5432/nodepgeasy' });
            done();
        });
    
        it('should create the table', function(done) {
            db.execute('CREATE TABLE tbl_test (' +
                'test_id bigserial PRIMARY KEY,' +
                'external_test_id text,' +
                'name text NOT NULL,' +
                'ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
                'UNIQUE (test_id)' +
            ');').then(function() {
                done();
            }, function(err) {
                console.error(err.message, err.stack);
                expect(1).not.to.be.ok;
                done();
            }).done();
        });
        
        it('should insert data with transaction', function(done) {
            db.begin().then(function(trans) {
                // transaction block
                return trans.execute('INSERT INTO tbl_test(external_test_id, name, ts) VALUES ($1, $2, $3) RETURNING tbl_test.*, to_char(ts, \'YYYY-MM-DD HH24:MI:SS\') AS ts;', [null, 'test name 1', '2016-07-29 00:01:00']).then(function() {
                    return trans.execute('INSERT INTO tbl_test(external_test_id, name, ts) VALUES ($1, $2, $3) RETURNING tbl_test.*, to_char(ts, \'YYYY-MM-DD HH24:MI:SS\') AS ts;', ['2', 'test name 2', '2016-07-28 10:01:00']);
                }).then(trans.commit, trans.rollbackAndThrow);
                
            }).then(function(results) {
                assert.equal(results.length, 2);
                assert.equal(results[0].rows.length, 1);
                assert.equal(results[1].rows.length, 1);
                var t1 = results[0].rows[0];
                var t2 = results[1].rows[0];
                expect(t1.test_id).to.be.ok;
                expect(t2.test_id).to.be.ok;
                expect(t1.external_test_id).not.to.be.ok;
                assert.equal(t2.external_test_id, '2');
                assert.equal(t1.name, 'test name 1');
                assert.equal(t2.name, 'test name 2');
                assert.equal(t1.ts, '2016-07-29 00:01:00');
                assert.equal(t2.ts, '2016-07-28 10:01:00');
                done();
            }, function(err) {
                console.error(err.message, err.stack);
                expect(1).not.to.be.ok;
                done();
            }).done();
        });
        
        it('should update data with transaction', function(done) {
            db.begin().then(function(trans) {
                // transaction block
                return trans.execute('UPDATE tbl_test SET external_test_id = $2, name = $3 WHERE test_id = $1 RETURNING tbl_test.*, to_char(ts, \'YYYY-MM-DD HH24:MI:SS\') AS ts;', 
                    [1, 'XYZ', 'updated test name 1']).then(trans.commit, trans.rollbackAndThrow);
                
            }).then(function(results) {
                assert.equal(results.length, 1);
                assert.equal(results[0].rows.length, 1);
                var t1 = results[0].rows[0];
                expect(t1.test_id).to.be.ok;
                assert.equal(t1.external_test_id, 'XYZ');
                assert.equal(t1.name, 'updated test name 1');
                assert.equal(t1.ts, '2016-07-29 00:01:00');
                done();
            }, function(err) {
                console.error(err.message, err.stack);
                expect(1).not.to.be.ok;
                done();
            }).done();
        });
        
        it('should throw a conflict error on postgres', function(done) {
            db.execute('INSERT INTO tbl_test VALUES ($1, $2, $3, $4);', [1, '1', 'test name 1', '2016-07-25 01:01:01']).then(function() {
                expect(1).not.to.be.ok;
                done();
            }, function(err) {
                expect(err).to.be.ok;
                assert.equal(err.code, 23505);
                done();
            }).done();
        });
        
        it('should throw a syntax error on postgres', function(done) {
            db.execute('INSERT IN tbl_test VALUES ($1, $2, $3, $4);', [1, '1', 'test name 1', '2016-07-25 01:01:01']).then(function() {
                expect(1).not.to.be.ok;
                done();
            }, function(err) {
                expect(err).to.be.ok;
                assert.equal(err.code, 42601);
                done();
            }).done();
        });
        
        it('should delete data', function(done) {
            db.execute('DELETE FROM tbl_test;').then(function() {
                return db.execute('SELECT * FROM tbl_test;');
            }).then(function(testRes) {
                expect(testRes).to.be.ok;
                assert.equal(testRes.rows.length, 0);
                assert.equal(testRes.rowCount, 0);
                done();
            }, function(err) {
                console.error(err.message, err.stack);
                expect(1).not.to.be.ok;
                done();
            }).done();
        });
    });
    
});