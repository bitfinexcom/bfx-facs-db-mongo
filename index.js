'use strict'

const _ = require('lodash')
const async = require('async')
const Mongo = require('mongodb').MongoClient
const Base = require('bfx-facs-base')
const fmt = require('util').format

function client (conf, label, cb) {
  let url = fmt(
    'mongodb://%s:%s@%s:%s/%s?authMechanism=DEFAULT&maxPoolSize=' + (conf.maxPoolSize || 150),
    conf.user, conf.password, conf.host, conf.port, conf.database
  )

  if (conf.rs) {
    url += `&replicaSet=${conf.rs}&readPreference=secondaryPreferred`
  }

  Mongo.connect(url, cb)
}

class MongoFacility extends Base {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'db-mongo'
    this._hasConf = true

    this.init()
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        client(_.pick(
          this.conf,
          ['user', 'password', 'database', 'host', 'port', 'rs']
        ), null, (err, cli) => {
          if (err) return next(err)

          this.cli = cli
          this.db = cli.db(this.conf.database)
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
        delete this.db
        next()
      }
    ], cb)
  }
}

module.exports = MongoFacility
