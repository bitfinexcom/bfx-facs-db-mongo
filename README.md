# bfx-facs-db-mongo

MongoDB interaction facility for Grenache.

Uses native mongodb driver to interact with mongodb server.

### Example configuration

```
{
  "m0": {
    "host": "127.0.0.1",
    "port": 27017,
    "user": "",
    "password": "",
    "database": "",
    "authSource": "admin"
  }
}
```
or
```
{
    "m0": {
       mongoUri: "mongodb://mongodb0.example.com:27017"
    }
}
```
or to send config in SRV connection format
```
{
  "m0": {
    "host": "127.0.0.1",
    "srv": true,
    "user": "",
    "password": "",
    "database": "",
    "authSource": "admin"
  }
}
```

