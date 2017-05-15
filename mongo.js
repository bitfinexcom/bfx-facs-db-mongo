'use strict'

const _ = require('lodash')
const async = require('async')
const Mongo = require('mongodb').MongoClient
const Facility = require('./base')
const fmt = require('util').format

function client (conf, label, cb) {
  const url = fmt(
    'mongodb://%s:%s@%s:%s/%s?authMechanism=DEFAULT&maxPoolSize=50',
    conf.user, conf.password, conf.host, conf.port, conf.database
  )
  Mongo.connect(url, cb)
}

class MongoFacility extends Facility {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'mongo'
    this._hasConf = true

    this.init()
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        client(_.pick(
          this.conf,
          ['user', 'password', 'database', 'host', 'port']
        ), null, (err, db) => {
          if (err) return next(err)

          this.cli = db
          next()
        })
      }
    ], cb)
  }

  _stop (cb) {
    async.series([
      next => { super._stop(next) },
      next => {
        this.cli.close()
        delete this.cli
        next()
      }
    ], cb)
  }
}

module.exports = MongoFacility
