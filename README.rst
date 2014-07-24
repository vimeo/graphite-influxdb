Graphite-InfluxDB
=================

An influxdb (0.8-rc3 or higher) backend for Graphite-web (source or 0.10.x) or graphite-api.


Install and configure using docker
----------------------------------

Using docker is an easy way to get graphite-api + graphite-influx up and running.
See https://github.com/Dieterbe/graphite-api-influxdb-docker

Otherwise, follow instructions below.
Graphite-api is the simplest to setup, though graphite-web might perform better.
You can use the experimental statsd support in graphite-api to have this backend
submit performance metrics (not supported with graphite-web)


Manual installation
-------------------

::

    pip install graphite_influxdb

don't forget to run maintain_cache.py, which keeps the cache up to date in a loop

Using with graphite-api
-----------------------

You need the patched version from https://github.com/brutasse/graphite-api/pull/36
This adds support for caching, statsd instrumentation, and graphite-style templates

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


Optional variable
-----------------

In graphite-api, using parameter `cheat_times: true`
means assume we have data since the beginning of time until now.
Since we automatically yield None's for unknown values, this
doesn't really make any difference, graphs look the same,
but now we don't query influx for the begin/end of each timeseries,
which boosts performance.
The default value of this is False.
In graphite-web, the parameter is `INFLUXDB_CHEAT_TIMES`.
