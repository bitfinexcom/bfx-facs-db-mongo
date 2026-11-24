'use strict'

const { test } = require('node:test')
const assert = require('node:assert/strict')

const { getFormattedURI } = require('../index')

const base = { host: 'db.local', port: 27017, database: 'app' }

test('getFormattedURI', async (t) => {
  const cases = [
    {
      name: 'with credentials',
      conf: { ...base, user: 'bob', password: 'secret' },
      expected: 'mongodb://bob:secret@db.local:27017/app?authMechanism=DEFAULT&maxPoolSize=150'
    },
    {
      name: 'without user skips credentials and authMechanism',
      conf: { ...base, password: 'ignored' },
      expected: 'mongodb://db.local:27017/app?maxPoolSize=150'
    },
    {
      name: 'empty user is treated as no user',
      conf: { ...base, user: '', password: '' },
      expected: 'mongodb://db.local:27017/app?maxPoolSize=150'
    },
    {
      name: 'format specifiers are not interpreted',
      conf: { ...base, user: 'u%s', password: 'p%d%c%%%j', database: 'd%o' },
      expected: 'mongodb://u%25s:p%25d%25c%25%25%25j@db.local:27017/d%o?authMechanism=DEFAULT&maxPoolSize=150'
    },
    {
      name: 'reserved characters in credentials are percent-encoded',
      conf: { ...base, user: 'a@b:c', password: 'p@ss:w/rd?#' },
      expected: 'mongodb://a%40b%3Ac:p%40ss%3Aw%2Frd%3F%23@db.local:27017/app?authMechanism=DEFAULT&maxPoolSize=150'
    },
    {
      name: 'srv omits the port',
      conf: { ...base, srv: true, user: 'bob', password: 'secret' },
      expected: 'mongodb+srv://bob:secret@db.local/app?authMechanism=DEFAULT&maxPoolSize=150'
    },
    {
      name: 'srv without user',
      conf: { ...base, srv: true },
      expected: 'mongodb+srv://db.local/app?maxPoolSize=150'
    },
    {
      name: 'custom maxPoolSize',
      conf: { ...base, maxPoolSize: 10 },
      expected: 'mongodb://db.local:27017/app?maxPoolSize=10'
    }
  ]

  for (const { name, conf, expected } of cases) {
    await t.test(name, () => {
      assert.equal(getFormattedURI(conf), expected)
    })
  }
})
