Graphite-InfluxDB
=================

An influxdb backend for Graphite-web (source or 0.10.x) or graphite-api.

Installation
------------

Run maintain_cache.py, which keeps the cache up to date in a loop

::

    pip install graphite_influxdb

Using with graphite-api
-----------------------

In your graphite-api config file::

    finders:
      - graphite_influxdb.InfluxdbFinder
    influxdb:
       host: localhost
       port: 8086
       user: graphite
       pass: graphite
       db:   graphite

Also enable the cache. memcache doesn't seem to work well because the list of series is too big.
filesystem seems to work well::

    cache:
        CACHE_TYPE: 'filesystem'
        CACHE_DIR: '/tmp/graphite-api-cache'


Using with graphite-web
-----------------------

In graphite's ``local_settings.py``::

    STORAGE_FINDERS = (
        'graphite_influxdb.InfluxdbFinder',
    )
    INFLUXDB_HOST = "localhost"
    INFLUXDB_PORT = 8086
    INFLUXDB_USER = "graphite"
    INFLUXDB_PASS = "graphite"
    INFLUXDB_DB =  "graphite"
