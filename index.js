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

function client (conf) {
  let url = (conf.uri)
    ? conf.uri
    : getFormattedURI(conf)

  if (conf.socketTimeoutMS && !conf.uri) {
    url += `&socketTimeoutMS=${conf.socketTimeoutMS}`
  }

  if (conf.rs && !conf.uri) {
    url += `&replicaSet=${conf.rs}`
  }

  if (conf.authSource && !conf.uri) {
    url += `&authSource=${conf.authSource}`
  }

  return MongoClient.connect(url)
}

/**
 * @typedef {Object} MongoIndex
 * @property {Object} spec
 * @property {Object} opts
 * @property {Object} [flags]
 * @property {Boolean} [flags.forceForeground] Move index creation to foreground
 */
class MongoFacility extends Base {
  /**
   * @param {Object} caller
   * @param {Object} opts
   * @param {Array<MongoIndex>} opts.createIndexes
   * @param {Object} ctx
   */
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
      async () => {
        const connConf = _.pick(
          this.conf,
          ['user', 'password', 'database', 'host', 'port', 'rs', 'maxPoolSize', 'authSource', 'socketTimeoutMS', 'srv', 'uri']
        )
        // backward compatibility
        if (this.opts.mongoUri) {
          connConf.uri = this.opts.mongoUri
        }
        this.cli = await client(connConf)
        this.db = this.cli.db(this.conf.database)

        if (!Array.isArray(this.opts.createIndexes)) {
          return
        }
        for (const ix of this.opts.createIndexes) {
          if (!ix.collection || !_.isString(ix.collection) || !_.isPlainObject(ix.spec)) {
            throw new Error('Mongodb index must have "collection" and "spec"')
          }
        }
        for (const ix of this.opts.createIndexes) {
          const opts = { ...(ix.opts || {}), background: !ix?.flags?.forceForeground }
          await this.db.collection(ix.collection).createIndex(ix.spec, opts)
        }
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
