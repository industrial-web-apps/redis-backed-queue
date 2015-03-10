var redis = require("redis");

function Queue(options) {
    this._client = redis.createClient();
    this.arrayName = (typeof options === 'string') ? options : options.arrayName;
    this.processArray = this.arrayName + '_proc';
}

Queue.prototype.quit = function() {
    this._client.quit();
};

Queue.prototype.clear = function(cb) {
    this._client.del(this.arrayName, cb);
};

Queue.prototype.length = function(cb) {
    this._client.llen(this.arrayName, cb);
};

Queue.prototype.push = function(item, cb) {
    var that = this;
    this._client.rpush(that.arrayName, JSON.stringify(item), cb);
};

Queue.prototype.pop = function(cb) {
    this._client.blpop(this.arrayName, 0, function(err, item) {
        if (err)
            return cb(err);

        cb(null, JSON.parse(item[1]));
    });
};

Queue.prototype.tpop = function(cb) {
    var that = this;
    that._client.blpop(this.arrayName, 0, function(err, item) {
        if (err)
            return cb(err);
        var commit = function (cb) {
                that._client.lrem(that.processArray, 1, item, cb);
            },
            rollback = function (cb) {
                that._client.lpush(that.arrayName, item, function () {
                    that._client.lrem(that.processArray, 1, item, cb);
                });
            };
        that._client.rpush(that.processArray, item);
        cb(null, JSON.parse(item[1]), commit, rollback);
    });
};

module.exports = {
    Queue: Queue
};
