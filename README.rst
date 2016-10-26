Graphite-InfluxDB
=================

An influxdb (0.8-rc5 or higher) backend for Graphite-web (source or 0.10.x) or graphite-api.

STATUS
======

THIS PROJECT IS NO LONGER MAINTAINED.
CHECK OUT [InfluxGraph](https://github.com/InfluxGraph/influxgraph) instead.


Install and configure using docker
----------------------------------

Using docker is an easy way to get graphite-api + graphite-influx up and running.
See https://github.com/Dieterbe/graphite-api-influxdb-docker which provides
a container that has all packages installed to make maximum use of these tools.

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
The schema declares at which interval you should have points in InfluxDB.
Schema rules use regex and are processed in order, first match wins.  If no rule matches, 60 seconds is used.


Using with graphite-api
-----------------------

You need the patched version from https://github.com/Dieterbe/graphite-api/tarball/support-templates2
This adds support for caching, statsd instrumentation, and graphite-style templates

Note that the elasticsearch stuff is optional, see below

In your graphite-api config file::

    finders:
      - graphite_influxdb.InfluxdbFinder
    influxdb:
       host: localhost
       port: 8086
       user: graphite
       pass: graphite
       db:   graphite
       ssl: false
       schema:
         - ['high-res-metrics', 1]
         - ['^collectd', 10]
    es:
       enabled: false
       hosts:
         - elastichost1:9200
       index: graphite_metrics2
       field: _id



Also enable the cache. memcache doesn't seem to work well because the list of series is too big.
filesystem seems to work well::

    cache:
        type: 'filesystem'
        dir: '/tmp/graphite-api-cache'


Using with graphite-web
-----------------------

Note that the elasticsearch stuff is optional, see below
In graphite's ``local_settings.py``::

    STORAGE_FINDERS = (
        'graphite_influxdb.InfluxdbFinder',
    )
    INFLUXDB_HOST = "localhost"
    INFLUXDB_PORT = 8086
    INFLUXDB_USER = "graphite"
    INFLUXDB_PASS = "graphite"
    INFLUXDB_DB =  "graphite"
    INFLUXDB_SSL = "false"
    INFLUXDB_SCHEMA = [
        ('', 60),
        ('high-res-metrics', 10)
    ]
    ES_ENABLED = "false"
    ES_HOSTS = ['elastichost1:9200']
    ES_INDEX = "graphite_metrics2"
    ES_FIELD = "_id"


Using Elasticsearch as an index
-------------------------------
If you have an index in elasticsearch that contains all your metric id's,
then you can use that as a metadata source.  Your mileage may vary, but for me ES is noticeably faster.
(see also https://github.com/influxdb/influxdb/issues/884)
You just need to install the elasticsearch pip module (comes in the docker image mentioned above) and enable it
in the config.
If you're wondering how to populate an ES index, you can use graph-explorer structured metrics plugins or carbon-tagger
(beware the latter currently only does metrics 2.0 metrics)
