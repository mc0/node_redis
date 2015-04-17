var util = require('util'),
    EventEmitter = require('events').EventEmitter,
    constants = require('constants'),
    redis = require('../index'),
    redisClusterSlot = require('./redisClusterSlot'),
    commands = require('./commands'),
    requestTtl = 16,
    requestDelayNearTtl = 100,
    defaultMaxConnections = 128,
    consts = {},
    RedisCluster;

consts.ERROR_NOT_CLUSTER_COMMAND = 1;
consts.ERROR_HIT_MAX_TTL = 2;
consts.ERROR_NO_STARTUP_NODES = 3;

function bindCommands() {
    var c = commands.length;

    // wraps each command and makes the call to the necessary connection
    var commandFunc = function() {
        var args = Array.prototype.slice.call(arguments),
            command = args.shift(),
            commandLower = command.toLowerCase(),
            key = args[0],
            ttl = requestTtl,
            randomize = false,
            askNode, callback, connection, slot, err,
            errorRecovery, callbackWrapper, attemptSend;

        if (typeof args[args.length - 1] === 'function') {
            callback = args.pop();
        }

        if (args[0] instanceof Array) {
            args = args[0];
        }

        switch (commandLower) {
            case 'info':
            case 'multi':
            case 'exec':
            case 'slaveof':
            case 'config':
            case 'shutdown':
                err = new Error('not a command available via cluster');
                err.code = consts.ERROR_NOT_CLUSTER_COMMAND;
                if (callback) {
                    callback(err);
                }
                this.emit('error', err);
                return;
            default:
                break;
        }

        errorRecovery = function(err) {
            var errParts = err.message && err.message.split(' '),
                isAsk = errParts[0] === 'ASK',
                hostPort, host, port, nodeName, node;

            if (isAsk || errParts[0] === 'MOVED') {
                slot = Number(errParts[1]);
                hostPort = errParts[2].split(':');
                host = hostPort[0];
                port = Number(hostPort[1] || 0);

                nodeName = host + ':' + port;
                node = this.nodes[nodeName];
                if (!node) {
                    node = {
                        host: host,
                        port: port,
                        name: nodeName
                    };
                }

                if (isAsk) {
                    askNode = node;
                } else {
                    if (RedisCluster.debug_mode) {
                        console.log('redisCluster got MOVED', err.message, 'for key', key, 'with arguments', arguments);
                    }
                    this.dirtySlotsHash = true;
                    this.slots[slot] = node;
                }

                attemptSend();
                return;
            }

            if (err.errno === constants.ECONNREFUSED || err.errno === constants.EACCES) {
                if (RedisCluster.debug_mode) {
                    console.log('Redis cluster conn error', err);
                }
                randomize = true;

                if (ttl < requestTtl / 2) {
                    // delay while we approach the TTL
                    setTimeout(attemptSend, requestDelayNearTtl);
                } else {
                    attemptSend();
                }
                return;
            }

            if (callback) {
                callback(err);
            }
        }.bind(this);

        callbackWrapper = function(err, result) {
            if (err) {
                errorRecovery(err);
                return;
            }

            if (callback) {
                callback(err, result);
            }
        }.bind(this);

        attemptSend = function() {
            if (slot === null || slot === undefined) {
                if (key && key instanceof Array) {
                    slot = this.getSlotForKey(key[0]);
                } else if (key) {
                    slot = this.getSlotForKey(key);
                } else {
                    slot = 0;
                }
            }

            if (!ttl) {
                err = new Error('reached max TTL for request');
                err.code = consts.ERROR_HIT_MAX_TTL;
                if (callback) {
                    callback(err);
                }
                return;
            }

            if (askNode) {
                connection = this.getConnectionByNode(askNode);
                askNode = null;
            } else if (randomize) {
                connection = this.getRandomConnection();
                randomize = false;
            } else {
                connection = this.getConnectionBySlot(slot);
            }

            if (!connection && RedisCluster.debug_mode) {
                console.log('Redis cluster: no connection for slot', slot, askNode, randomize);
            }

            ttl--;
            connection.send_command(command, args, callbackWrapper);
        }.bind(this);

        if (this.dirtySlotsHash) {
            this.updateSlots(attemptSend);
        } else {
            // start attempting
            attemptSend();
        }
    };

    while (c--) {
        this[commands[c]] = commandFunc.bind(this, commands[c]);
    }
}

function parseHostPortInput(host, port) {
    var split;

    if (!port && typeof host === 'string' && host.indexOf(':') >= 0) {
        split = host.split(':');
        host = split[0];
        port = split[1];
    }

    if (typeof host === 'object' && host.host) {
        port = host.port;
        host = host.host;
    }

    return {
        host: host,
        port: port
    };
}

function closeNodeConnection(node) {
    if (!node.name) {
        if (RedisCluster.debug_mode) {
            console.log('redisCluster closeNode on node with no name', node);
        }
        return;
    }
    var connection = this._connections[node.name];
    if (!connection) {
        return;
    }

    // attempt to quit gracefully or just end the connection
    if (connection.quit) {
        connection.quit();
    } else {
        connection.end();
    }
    delete this._connections[node.name];
    this._connectionCount--;
}

function setNameOnNode(node) {
    if (node && !node.name) {
        node.name = node.host + ':' + node.port;
    }
}

function handleRedisError(err) {
    if (RedisCluster.debug_mode) {
        console.log('Redis cluster got redis error', err);
    }
    this.emit('error', err);
}

function handleRedisEnd(connection, err) {
    if (RedisCluster.debug_mode) {
        console.log('Redis cluster got redis end', err);
    }

    closeNodeConnection.call(this, connection);
}

function closeRandomConnection() {
    var keys = Object.keys(this._connections),
        nodeName = keys[Math.floor(Math.random() * keys.length)],
        node = this.nodes[nodeName];

    closeNodeConnection.call(this, node);
}

function onClusterSlots(startupNode, attemptNode, startupHostPort, err, result) {
    var startupNodeHasSlot = false,
        i, j, k, l, host, port, node, nodeName, addedSlot;

    this._updatingSlots = false;

    if (err) {
        if (this._startupNodes[attemptNode + 1]) {
            this.updateSlots();
        } else {
            for (i = 0, l = this._startupCallbacks.length; i < l; i++) {
                this._startupCallbacks[i](err, null);
            }
        }
        return;
    }

    this.slots = {};
    this.nodes = {};

    // loop over the set of slots
    for (i = 0, l = result.length; i < l; i++) {
        host = result[i][2][0];
        port = result[i][2][1];
        nodeName = host + ':' + port;
        node = this.nodes[nodeName];
        if (!node) {
            node = {
                host: host,
                port: port,
                name: nodeName
            };
        }
        if (startupHostPort.host === host && parseInt(startupHostPort.port, 10) === parseInt(port, 10)) {
            startupNodeHasSlot = true;
        }
        addedSlot = false;

        // loop over each slot, inclusive to the end slot
        for (j = result[i][0], k = result[i][1]; j <= k; j++) {
            this.slots[j] = node;
            addedSlot = true;
        }

        if (addedSlot) {
            this.nodes[nodeName] = node;
        }

        // ensure we have a good list of startup nodes in case the initial list of nodes went down
        if (this._startupNodes.indexOf(nodeName) === -1) {
            this._startupNodes.push(node);
        }
    }

    this._lastStartupNodeAttempt = -1;
    this.dirtySlotsHash = false;

    // if the startup node doesn't have a slot, lets not keep the connection open
    if (!startupNodeHasSlot) {
        closeNodeConnection.call(this, startupNode);

        // remove the startupNode from the startupNodes list if we have at least 2
        // so that we can avoid issues if it gets removed completely
        if (this._startupNodes.length >= 2) {
            this._startupNodes.splice(attemptNode, 1);
        }
    }

    for (i = 0, l = this._startupCallbacks.length; i < l; i++) {
        this._startupCallbacks[i](null, this.slots);
    }
}

RedisCluster = function(startupNodes, options, auth) {
    if (!(startupNodes instanceof Array)) {
        startupNodes = [startupNodes];
    }
    var err;

    this._startupNodes = startupNodes.slice(0);
    this._lastStartupNodeAttempt = -1;
    this._startupCallbacks = [];
    this._options = options || {};
    this._auth = auth;
    this._connections = {};
    this._connectionCount = 0;
    this._maxConnections = this._options.maxConnections || defaultMaxConnections;

    this.slots = {};
    this.nodes = {};
    this.dirtySlotsHash = true;

    if (!this._startupNodes) {
        err = new Error('no startup nodes provided');
        err.code = consts.ERROR_NO_STARTUP_NODES;
        throw err;
    }

    bindCommands.call(this);

    this.updateSlots(function(err) {
        if (err) {
            if (RedisCluster.debug_mode) {
                console.log(err);
            }
            this.emit('error', err);
            return;
        }

        this.connected = true;
        this.emit('connected');

        // match RedisClient
        this.emit('ready');
        this.emit('connect');
    }.bind(this));
};
RedisCluster.debug_mode = false;
Object.keys(consts).forEach(function(key) {
    RedisCluster[key] = consts[key];
});
util.inherits(RedisCluster, EventEmitter);

RedisCluster.prototype.updateSlots = function(callback) {
    var attemptNode = this._lastStartupNodeAttempt + 1,
        startupNode, startupHostPort, i, l;

    if (callback) {
        this._startupCallbacks.push(callback);
    }

    if (!this._startupNodes[attemptNode]) {
        this._lastStartupNodeAttempt = -1;
        for (i = 0, l = this._startupCallbacks.length; i < l; i++) {
            this._startupCallbacks[i]({noStartupNodes: true}, null);
        }
        return;
    }
    if (this._updatingSlots) {
        return;
    }

    this._updatingSlots = true;

    startupHostPort = parseHostPortInput(this._startupNodes[attemptNode]);

    this._lastStartupNodeAttempt = attemptNode;
    startupNode = this.getConnectionByNode(startupHostPort);

    if (RedisCluster.debug_mode) {
        console.log('redisCluster updating slots', this.dirtySlotsHash);
    }

    startupNode.cluster('slots', onClusterSlots.bind(this, startupNode, attemptNode, startupHostPort));
};

RedisCluster.prototype.getSlotForKey = function(key) {
    return redisClusterSlot(key);
};

RedisCluster.prototype.getConnectionBySlot = function(slot) {
    var node = this.slots[slot],
        connection;

    // if we don't know the node, get a random node
    if (!node) {
        connection = this.getRandomConnection();
    } else {
        connection = this.getConnectionByNode(node);
    }

    return connection;
};

RedisCluster.prototype.getConnectionByNode = function(node) {
    setNameOnNode(node);

    // if we don't have a connection, make one
    if (!this._connections[node.name]) {
        this._connections[node.name] = this.createNodeConnection(node);
    }

    return this._connections[node.name];
};

RedisCluster.prototype.getRandomConnection = function() {
    var keys = Object.keys(this.slots),
        key = keys[Math.floor(keys.length * Math.random())],
        node = this.slots[key];

    return this.getConnectionByNode(node);
};

RedisCluster.prototype.createNodeConnection = function(inputHost, inputPort) {
    var parts = parseHostPortInput(inputHost, inputPort),
        host = parts.host,
        port = parts.port,
        connection;

    if (RedisCluster.debug_mode) {
        console.log('redisCluster connecting to node', host, port);
    }

    if (this._connectionCount >= this._maxConnections) {
        if (RedisCluster.debug_mode) {
            console.log('redisCluster hit maxConnections', this._connectionCount);
        }
        closeRandomConnection.call(this);
    }

    if (this._auth) {
        connection = redis.createClient(port, host, this._options).auth(this._auth);
    } else {
        connection = redis.createClient(port, host, this._options);
    }

    connection.on('error', handleRedisError.bind(this));
    connection.on('end', handleRedisEnd.bind(this, connection));
    this._connectionCount++;

    return connection;
};

module.exports = RedisCluster;
