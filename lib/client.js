'use strict'

const co = require('co')
const Readable = require('stream').Readable

let request
let config = {
    bucketUri: '',
    pollingInterval: 1000,
    queryTimeout: 0,
    format: 'array',
    baseRetryWait: 200,
    retryCountMax: 5,
    concurrentExecMax: 5,
    execRightCheckInterval: 100,
    streamCheckInterval: 100,
}

class Client {
    constructor(config_) {
        checkConfig(config_)
        this.config = Object.assign({}, config, config_)
        this.initExecRights()
    }

    initExecRights() {
        let rights = []
        for (let i = 0; i < this.config.concurrentExecMax; i++) {
            rights.push({})
        }
        this.execRights = rights
    }

    getExecRight() {
        return this.execRights.pop()
    }

    setExecRight(newRight) {
        if (this.execRights.length < this.config.concurrentExecMax) {
            this.execRights.push(newRight)
        }
    }

    execute(query, options, callback) {
        return new Promise((resolve, reject) => {
            options = options || {}
            if (typeof (options) == 'function') {
                callback = options
                options = {}
            }
            let readable = this.createStream(query, options)
            let format = options.format || this.config.format
            let data, isError
            readable.on('data', chunk => {
                switch (format) {
                    case 'raw':
                        if (!data) {
                            data = chunk
                            delete (data.NextToken)
                        } else {
                            data.ResultSet.Rows = data.ResultSet.Rows.concat(chunk.ResultSet.Rows)
                        }
                        break
                    case 'array':
                        if (!data) {
                            data = []
                        }
                        data = data.concat(chunk)
                        break
                }
            })

            readable.on('end', () => {
                if (isError) return
                if (callback) {
                    callback(null, data)
                }
                return resolve(data)
            })

            readable.on('error', err => {
                isError = true
                return handleError(err, resolve, reject, callback)
            })
        })
    }

    createStream(query, options) {
        options = options || {}
        let nowConfig = Object.assign({}, this.config)
        return new AthenaReadable(this, query, nowConfig, options)
    }
}



class AthenaReadable extends Readable {
    constructor(client, query, config, options) {
        super({
            objectMode: true,
        })
        this.client = client
        this.query = query
        this.config = config
        this.options = options
        this.cache = []
        this.isEnd = false

        let self = this
        let right = self.client.getExecRight()
        co(function* () {
            // limit the number of concurrent executions
            while (right === undefined) {
                yield sleep(self.config.execRightCheckInterval)
                right = self.client.getExecRight()
            }
            let queryId = yield request.startQuery(this.query, self.config)
            let timeout = self.options.timeout || self.config.queryTimeout
            let isTimeout = false
            let timer
            if (timeout) {
                timer = setTimeout(function timeoutFunc() {
                    isTimeout = true
                    request.stopQuery(queryId, self.config).then(() => {
                        let err = new Error('query timeout')
                        self.emit('error', err)
                    }).catch(err => {
                        self.emit('error', err)
                    })
                }, timeout)
            }
            LOOP:
            while (!isTimeout) {
                yield sleep(self.config.pollingInterval)
                let isEnd = yield request.checkQuery(queryId, self.config)
                if (!isEnd) {
                    continue
                }
                clearTimeout(timer)
                let format = self.options.format || self.config.format
                let data = yield request.getQueryResults(queryId, self.config)
                let previousToken
                while (!isTimeout && data.NextToken && data.NextToken !== previousToken) {
                    let dataTmp = yield request.getQueryResults(queryId, self.config, data.NextToken)
                    data.NextToken = dataTmp.NextToken || null
                    previousToken = data.NextToken
                    self.cache.push(extractData(data, format))
                }
                break
            }
            self.client.setExecRight(right)
            self.isEnd = true
        }).catch(err => {
            self.emit('error', err)
            self.isEnd = true
        })

    }

    _read() {
        let self = this
        co(function* () {
            while (self.cache.length != 0 || !self.isEnd) {
                let chunk = self.cache.shift()
                if (chunk) return self.push(chunk)
                yield sleep(self.config.streamCheckInterval)
            }
            self.push(null)
        }).catch(err => {
            self.emit('error', err)
        })

    }
}

function checkConfig(config_) {
    if (!config_.bucketUri) {
        throw new Error('buket uri required')
    }
    config_.pollingInterval = Math.max(config_.pollingInterval, 0)
    config_.queryTimeout = Math.max(config_.queryTimeout, 0)
}

function extractData(data, format) {
    let result
    switch (format) {
        case 'raw':
            result = data
            break
        case 'array':
            result = []
            let rows = data.ResultSet.Rows
            if (rows && rows.length !== 0) {
                let cols = rows.shift().Data
                let len = rows.length
                for (let i = 0; i < len; i++) {
                    let row = rows[i].Data
                    let record = {}
                    for (let j = 0; j < row.length; j++) {
                        let colName = cols[j].VarCharValue
                        let colVal = row[j].VarCharValue
                        record[colName] = colVal
                    }
                    result.push(record)
                }
            }
            break
        default:
            throw new Error(`invalid format ${format}`)
    }
    return result
}

function handleError(err, resolve, reject, callback) {
    if (callback) {
        callback(err, null)
        return resolve()
    }
    return reject(err)
}

function sleep(time) {
    return new Promise(resolve => {
        setTimeout(() => {
            return resolve()
        }, time)
    })
}

exports.create = (request_, config_) => {
    request = request_
    return new Client(config_)
}