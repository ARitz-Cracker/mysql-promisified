const {EventEmitter} = require("events");
const mysql = require("mysql");
const {toPromise, toPromiseArray} = require("arc-topromise");

class MySQLConnection extends EventEmitter{
	constructor(connectionUri){
		if(connectionUri.threadId == null){
			this._connection = mysql.createConnection(connectionUri);
		}else{
			this._connection = connectionUri;
		}
		
		this._connection.on("error", this.emit.bind(this, "error"));
		this._connection.on("error", (err) => {
			if(err.code === "PROTOCOL_CONNECTION_LOST"){
				this.emit("disconnected");
			}
		});
		this._connection.on("drain", this.emit.bind(this, "drain"));
		this._connection.on("enqueue", this.emit.bind(this, "enqueue"));
		this._connection.on("fields", this.emit.bind(this, "fields"));
		this._connection.on("end", this.emit.bind(this, "end"));
	}

	beginTransaction(){
		return toPromise(this._connection, this._connection.beginTransaction);
	}
	changeUser(options){
		return toPromise(this._connection, this._connection.changeUser, options);
	}
	get config(){
		return this._connection.config;
	}
	set config(val){
		this._connection.config = val;
	}
	connect(){
		return toPromise(this._connection, this._connection.connect);
	}
	commit(){
		return toPromise(this._connection, this._connection.commit);
	}
	createQuery(...args){
		return toPromiseArray(this._connection, this._connection.createQuery, ...args);
	}
	createQueryStream(...args){
		return this._connection.createQuery(...args);
	}
	destroy(){
		return this._connection.destroy();
	}
	end(){
		return new Promise((resolve, reject) => {
			this._connection.end((err) => {
				if(err){
					reject(err);
				}else{
					resolve();
				}
				this.emit("disconnected");
			});
		});
	}
	escape(...args){
		return this._connection.escape(...args); 
	}
	escapeId(...args){
		return this._connection.escapeId(...args); 
	}
	format(...args){
		return this._connection.format(...args); 
	}
	pause(){
		this._connection.pause(); 
	}
	ping(){
		return toPromise(this._connection, this._connection.ping);
	}
	resume(){
		this._connection.resume(); 
	}

	query(...args){
		return toPromiseArray(this._connection, this._connection.query, ...args);
	}
	queryStream(...args){
		return this._connection.query(...args);
	}
	release(){
		return this._connection.release();
	}
	rollback(){
		return toPromise(this._connection, this._connection.rollback);
	}
	get state(){
		return this._connection.state;
	}
	statistics(){
		return toPromise(this._connection, this._connection.statistics);
	}
	get threadId(){
		return this._connection.threadId;
	}
}

class MySQLPool extends EventEmitter{
	constructor(connectionUri){
		this._connections = {};
		this._pool = mysql.createPool(connectionUri);
		this._pool.on("error", this.emit.bind(this, "error"));
		this._pool.on("enqueue", this.emit.bind(this, "enqueue"));
		this._pool.on("acquire", (connection) => {
			this.emit("acquire", this._connections[connection.threadId]);
		});
		this._pool.on("release", (connection) => {
			this.emit("release", this._connections[connection.threadId]);
		});
		this._pool.on("end", this.emit.bind(this, "end"));
		this._pool.on("connection", (connection) => {
			const threadId = connection.threadId;
			this._connections[threadId] = new MySQLConnection(connection);
			this._connections[threadId].on("disconnected", () => {
				delete this._connections[threadId];
			});
			this.emit("connection", this._connections[threadId]);
		});
	}
	acquireConnection(connection){
		return new Promise((resolve, reject) => {
			this._pool.acquireConnection(connection._connection, (err, connection) => {
				if(err){
					reject(err);
				}else{
					resolve(this._connections[connection.threadId]);
				}
			});
		});
	}
	releaseConnection(connection){
		this._pool.releaseConnection(connection._connection);
	}
	getConnection(){
		return new Promise((resolve, reject) => {
			this._pool.getConnection((err, connection) => {
				if(err){
					reject(err);
				}else{
					resolve(this._connections[connection.threadId]);
				}
			});
		});
	}
	query(...args){
		return toPromiseArray(this._pool, this._pool.query, ...args);
	}
	queryStream(...args){
		return this._pool.query(...args);
	}
	destroy(){
		return this._connection.destroy();
	}
	end(){
		return toPromise(this._pool, this._pool.end);
	}
	escape(...args){
		return this._pool.escape(...args); 
	}
	escapeId(...args){
		return this._pool.escapeId(...args); 
	}
	format(...args){
		return this._pool.format(...args); 
	}
}

class MySQLPoolCluster extends EventEmitter{
	constructor(options){
		this._connections = {};
		this._poolCluster = mysql.createPoolCluster(options);
		this._poolCluster.on("remove", this.emit.bind(this, "remove"));
		this._poolCluster.on("offline", this.emit.bind(this, "offline"));
	}
	add(...args){
		this._poolCluster.add(...args);
	}
	remove(...args){
		this._poolCluster.remove(...args);
	}
	query(...args){
		return toPromiseArray(this._poolCluster, this._poolCluster.query, ...args);
	}
	queryStream(...args){
		return this._poolCluster.query(...args);
	}
	async getConnection(...args){
		const connection = await toPromise(this._poolCluster, this._poolCluster.getConnection, ...args);
		const threadId = connection.threadId;
		this._connections[threadId] = new MySQLConnection(connection);
		this._connections[threadId].on("disconnected", () => {
			delete this._connections[threadId];
		});
		return this._connections[threadId];
	}
	end(){
		return toPromise(this._poolCluster, this._poolCluster.end);
	}
}

const mysqlPromisified = {
	createConnection: function(connectionUri){
		return new MySQLConnection(connectionUri);
	},
	createPool: function(options){
		return new MySQLPool(options);
	},
	createPoolCluster: function(options){
		return new MySQLPoolCluster(options);
	},
	escape: mysql.escape.bind(mysql),
	format: mysql.format.bind(mysql),
	raw: mysql.raw.bind(mysql),
	escapeId: mysql.escapeId.bind(mysql)
}

module.exports = mysqlPromisified;
