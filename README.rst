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


About the retention schemas
---------------------------

In the configs below, you'll see that you need to configure the schemas (datapoint resolutions) explicitly.
It basically contains the same information as /etc/carbon/storage-schemas.conf would for whisper.
But Influxdb currently has no way to supply us this information (yet), so we must configure it explicitly here.
Also, it seems like internally, the graphite-web/graphite-api is to configure the step (resolution in seconds)
per metric (i.e. per Node/Reader), without looking at the timeframe.   I don't know how to deal with this yet (TODO), so for now it's one step per
pattern, so we don't need to specify retention timeframes.
(In fact, in the code we can assume the data exists from now to -infinity, missing data you query for
will just show up as nulls anyway)


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
       schema:
         - ['', 60]
         - ['high-res-metrics', 10]



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
    INFLUXDB_SCHEMA = [
        ('', 60),
        ('high-res-metrics', 10)
    ]

