'use strict'

const _ = require('lodash')
const async = require('async')

const Driver = require('mongodb')
const { MongoClient, ObjectID } = Driver

const Base = require('bfx-facs-base')
const fmt = require('util').format

function getFormattedURI (conf) {
  const suffix = `authMechanism=DEFAULT&maxPoolSize=${(conf.maxPoolSize || 150)}`

  const { srv } = conf
  return fmt(
    `mongodb${srv ? '+srv' : ''}://%s:%s@%s%s/%s?${suffix}`,
    conf.user, conf.password, conf.host, srv ? '' : `:${conf.port}`, conf.database
  )
}

function client (conf, opts, cb) {
  let url = (opts.mongoUri)
    ? opts.mongoUri
    : getFormattedURI(conf)

  if (conf.socketTimeoutMS && !opts.mongoUri) {
    url += `&socketTimeoutMS=${conf.socketTimeoutMS}`
  }

  if (conf.rs && !opts.mongoUri) {
    url += `&replicaSet=${conf.rs}`
  }

  if (conf.authSource && !opts.mongoUri) {
    url += `&authSource=${conf.authSource}`
  }

  MongoClient.connect(url, cb)
}

class MongoFacility extends Base {
  constructor (caller, opts, ctx) {
    super(caller, opts, ctx)

    this.name = 'db-mongo'
    this._hasConf = true

    this.init()
  }

  getDriver () {
    return Driver
  }

  getObjectID (id) {
    return new ObjectID(id)
  }

  _start (cb) {
    async.series([
      next => { super._start(next) },
      next => {
        const connConf = _.pick(
          this.conf,
          ['user', 'password', 'database', 'host', 'port', 'rs', 'maxPoolSize', 'authSource', 'socketTimeoutMS', 'srv']
        )
        client(connConf, this.opts, (err, cli) => {
          if (err) return next(err)

          this.cli = cli
          this.db = cli.db(this.conf.database)
          next()
        })
      },
      next => {
        if (!Array.isArray(this.opts.createIndexes)) {
          return next()
        }
        for (const ix of this.opts.createIndexes) {
          if (!ix.collection || !_.isString(ix.collection) || !_.isPlainObject(ix.spec)) {
            throw new Error('Mongodb index must have "collection" and "spec"')
          }
        }
        async.mapSeries(
          this.opts.indexes,
          (ix, next) => {
            this.db.collection(ix.collection).createIndex(ix.spec, ix.opts || {}, next)
          },
          cb
        )
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
