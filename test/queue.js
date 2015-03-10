'use strict';

var assert = require('assert'),
    Queue = require('../redis-queue').Queue;

describe('Queue', function() {
  var queue, queue2;

  beforeEach(function() {
    queue = new Queue('tmp');
    queue2 = new Queue('tmp');
  });

  afterEach(function(done) {
    queue.clear(function () {
      queue2.clear(done);
    });
  });

  describe('#length', function() {
    it('returns 0 for an empty queue', function(done) {
      queue.length(function(err, value) {
        if (err) { done(err); }
        else {
          assert.equal(0, value);
          done();
        }
      });
    });
  });

  describe('#push', function() {
    it('returns 1 if one item has been pushed', function(done) {
      queue2.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        else {
          queue.length(function(err, value) {
            assert.equal(1, value);
            done();
          });
        }
      });
    });
  });

  describe('#pop', function() {
    it('returns 1 item from the queue (push then pop)', function(done) {
      queue2.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        else {
          queue.pop(function(err, message) {
            if (err) { done(err); }
            assert.deepEqual({ 'hello': 'world' }, message);
            done();
          });
        }
      });
    });

    it('returns 1 item from the queue (pop then push)', function(done) {
      queue.pop(function(err, message) {
        if (err) { done(err); }
        else {
          assert.deepEqual({ 'hello': 'world' }, message);
          done();
        }
      });

      queue2.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
      });
    });

    it('returns all items in order (pop then push)', function(done) {
      queue.pop(function(err, message) {
        if (err) { done(err); }
        assert.deepEqual({ 'id': 1 }, message);
        queue.pop(function(err, message) {
          if (err) { done(err); }
          assert.deepEqual({ 'id': 2 }, message);
          queue.pop(function(err, message) {
            if (err) { done(err); }
            assert.deepEqual({ 'id': 3 }, message);
            queue.pop(function(err, message) {
              if (err) { done(err); }
              else {
                assert.deepEqual({ 'id': 4 }, message);
                done();
              }
            });
          });
        });
      });

      queue2.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue2.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
          queue2.push({ 'id': 3 }, function(err) {
            if (err) { done(err); }
            queue2.push({ 'id': 4 }, function(err) {
              if (err) { done(err); }
            });
          });
        });
      });
    });

    it('returns all items in order (pop then push)', function(done) {
      queue2.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue2.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
          queue2.push({ 'id': 3 }, function(err) {
            if (err) { done(err); }
            queue2.push({ 'id': 4 }, function(err) {
              if (err) { done(err); }
              queue.pop(function(err, message) {
                if (err) { done(err); }
                assert.deepEqual({ 'id': 1 }, message);
                queue.pop(function(err, message) {
                  if (err) { done(err); }
                  assert.deepEqual({ 'id': 2 }, message);
                  queue.pop(function(err, message) {
                    if (err) { done(err); }
                    assert.deepEqual({ 'id': 3 }, message);
                    queue.pop(function(err, message) {
                      if (err) { done(err); }
                      else {
                        assert.deepEqual({ 'id': 4 }, message);
                        done();
                      }
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });

  describe('#tpop', function() {
    it('rollbacks a message so that it is available again', function(done) {
      queue2.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        queue.tpop(function(err, message, commit, rollback) {
          if (err) { done(err); }
          assert.deepEqual({ 'hello': 'world' }, message);
          queue.length(function(err, value) {
            if (err) { done(err); }
            assert.equal(0, value);
            rollback(function(err) {
              if (err) { done(err); }
              queue.length(function(err, value) {
                if (err) { done(err); }
                else {
                  assert.equal(1, value);
                  done();
                }
              });
            });
          });
        });
      });
    });

    it('commits a transaction successfully', function(done) {
      queue2.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
        queue.tpop(function(err, message, commit, rollback) {
          if (err) { done(err); }
          assert.deepEqual({ 'hello': 'world' }, message);
          queue.length(function(err, value) {
            if (err) { done(err); }
            assert.equal(0, value);
            commit(function(err) {
              if (err) { done(err); }
              queue.length(function(err, value) {
                if (err) { done(err); }
                else {
                  assert.equal(0, value);
                  done();
                }
              });
            });
          });
        });
      });
    });

    it('returns 1 item from the queue (tpop then push)', function(done) {
      queue.tpop(function(err, message, commit, rollback) {
        if (err) { done(err); }
        else {
          assert.deepEqual({ 'hello': 'world' }, message);
          done();
        }
      });

      queue2.push({ 'hello': 'world' }, function(err) {
        if (err) { done(err); }
      });
    });

    it('returns all items in order (tpop then push)', function(done) {
      queue.tpop(function(err, message, commit, rollback) {
        assert.deepEqual({ 'id': 1 }, message);
        commit(function(err) {
          if (err) { done(err); }
          queue.tpop(function(err, message, commit, rollback) {
            assert.deepEqual({ 'id': 2 }, message);
            commit(function(err) {
              if (err) { done(err); }
              done();
            });
          });
        });
      });

      queue2.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue2.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
        });
      });
    });

    it('returns all items in order (pop then push)', function(done) {
      queue2.push({ 'id': 1}, function(err) {
        if (err) { done(err); }
        queue2.push({ 'id': 2 }, function(err) {
          if (err) { done(err); }
          queue2.push({ 'id': 3 }, function(err) {
            if (err) { done(err); }
            queue2.push({ 'id': 4 }, function(err) {
              if (err) { done(err); }
              queue.pop(function(err, message, commit, rollback) {
                if (err) { done(err); }
                assert.deepEqual({ 'id': 1 }, message);
                queue.tpop(function(err, message, commit, rollback) {
                  if (err) { done(err); }
                  assert.deepEqual({ 'id': 2 }, message);
                  queue.tpop(function(err, message, commit, rollback) {
                    if (err) { done(err); }
                    assert.deepEqual({ 'id': 3 }, message);
                    queue.tpop(function(err, message, commit, rollback) {
                      if (err) { done(err); }
                      else {
                        assert.deepEqual({ 'id': 4 }, message);
                        done();
                      }
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});
