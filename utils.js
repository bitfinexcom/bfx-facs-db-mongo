'use strict'

const fmt = require('util').format

const DEFAULT_MAX_POOL_SIZE = 150

// Every value goes through fmt arguments: a value inside the format string
// would have its % sequences (e.g. a password with "%d") interpreted.
// Credentials are percent-encoded so characters like "@", ":" or "/" don't break the URI.
function getFormattedURI (conf) {
  const { srv, user } = conf
  return fmt(
    '%s://%s%s%s/%s?%smaxPoolSize=%s',
    srv ? 'mongodb+srv' : 'mongodb',
    user ? fmt('%s:%s@', encodeURIComponent(user), encodeURIComponent(conf.password)) : '',
    conf.host,
    srv ? '' : fmt(':%s', conf.port),
    conf.database,
    user ? 'authMechanism=DEFAULT&' : '',
    conf.maxPoolSize || DEFAULT_MAX_POOL_SIZE
  )
}

module.exports = { getFormattedURI }
