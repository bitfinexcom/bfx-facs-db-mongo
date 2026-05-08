# bfx-facs-db-mongo

MongoDB interaction facility for Grenache.

Uses native mongodb driver to interact with mongodb server.

### Example configuration
This facility supports .json and .js configuration files. The .json configuration files have higher priority than .js files. If both types of configuration files are present, the facility will use the .json configuration file.

#### JSON configuration example
You can use either of the following formats to configure the facility:

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

#### JavaScript configuration example
You can also use JavaScript configuration files. The following example demonstrates how to export the configuration using `module.exports`:
```JavaScript
module.exports = {
  m0: {
    mongoUri: process.env.MONGO_URI,
  }
};
```
