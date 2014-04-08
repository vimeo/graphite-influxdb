Graphite-InfluxDB
=================

An influxdb backend for Graphite-web (source or 0.10.x) or graphite-api.

Installation
------------

::

    pip install graphite_influxdb

Using with graphite-api
-----------------------

In your graphite-api config file::

    finders:
      - graphite_influxdb.InfluxdbFinder

Using with graphite-web
-----------------------

In graphite's ``local_settings.py``::

    STORAGE_FINDERS = (
        'graphite_influxdb.InfluxdbFinder',
    )
