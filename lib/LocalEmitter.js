var EventEmitter = require('events').EventEmitter;

var brokers = {};
var mutils = require('./utils');

var getEndpoint = function(endpoint) {
	brokers[endpoint] = brokers[endpoint] || new EventEmitter();
	return brokers[endpoint];
};

var getNode = getEndpoint;

module.exports = {

	Broker: function() {

		var started = false;
		var binded = {};

		return {
			on: function(type, fct) {

				binded[type] = function(identity, msg) {
					fct.apply(null, mutils.args(arguments));
				};
				if (started) {
					getEndpoint(endpoint).on(type, binded[type]);
				}
			},
			bind: function(endpoint, cb) {
				started = true;
				this.endpoint = endpoint;

				Object.keys(binded).forEach(function(type) {
					getEndpoint(endpoint).on(type, binded[type]);
				})

				cb();
			},
			send: function(msg) {
				var dest = msg.shift();
				getNode(dest).emit.apply(getNode(dest), ['message'].concat(msg));
			},
			unbind: function(endpoint, cb) {
				Object.keys(binded).forEach(function(type) {
					getEndpoint(endpoint).removeAllListeners(type);
				});
				setImmediate(cb);
			},
			close: function() {

			}

		};
	},
	Client: function() {
		var binded = {};
		return {
			on: function(type, fct) {
				binded[type] = function() {
					fct.apply(fct, arguments);
				};
				getNode(this.identity).on(type, binded[type]);
			},
			connect: function(endpoint, cb) {
				this.endpoint = endpoint;
				(cb || function() {})();

			},
			send: function(msg) {
				getEndpoint(this.endpoint).emit.apply(getEndpoint(this.endpoint), ['message', this.identity.toString()].concat(mutils.args(msg)));
			},
			_zmq: 0,
			close: function() {
				getNode(this.identity).removeAllListeners('message');
			}
		};
	},
	Worker: function() {
		var binded = {};

		return {
			on: function(type, fct) {
				binded[type] = function() {
					fct.apply(fct, arguments);
				};
				getNode(this.identity).on(type, binded[type]);
			},
			connect: function(endpoint, cb) {
				this.endpoint = endpoint;
				(cb || function() {})();

			},
			send: function(msg) {
				getEndpoint(this.endpoint).emit.apply(getEndpoint(this.endpoint), ['message', this.identity.toString()].concat(mutils.args(msg)));
			},
			close: function() {
				getNode(this.identity).removeAllListeners('message');
			},
			_zmq: 0
		};
	}

}

/*
function(inport) {

	if (emitters[inport]) {
		return emitters[inport];
	};

	var emitter = new EventEmitter();
	emitter.bind(function(endpoint, cb) {
		setImmediate(cb);
	});

	emitters[inport] = emitter;
	return emitter;

}*/