#!/usr/bin/env python2

import os
import yaml
from influxdb import InfluxDBClient
from werkzeug.contrib.cache import MemcachedCache, FileSystemCache  # dep of flask-cache
from structlog import get_logger
import time


def duration(name, start):
    now = time.time()
    print name, "took", (now - start), "seconds"

logger = get_logger()


# with about 200k series:
# list series:
# 2-3s localhost, 14 to -I think- 200s over vpn
# select * from /.*/ limit 1;
# 880s localhost (14.5 min)
# select * from // limit 1;
# 780s localhost (13min)
# select * from // order asc limit 1
# 750s localhost
# select * from <single> [order asc] limit 1
# around 5 to 30ms, most <10ms, so for 300k series = 3000 seconds -> don't do it per single series.

"""
list series:

[{u'columns': [u'time', u'sequence_number'],
      u'name': u'cdn.fastly.viewmaster-dev.ATL.requests',
        u'points': []},

select * from // order asc limit 1

[{u'columns': [u'time', u'sequence_number', u'value'],
  u'name': u'collectd.dfvimeorpc13.cpu.3.cpu.system',
  u'points': [[1402607609, 1, 0.216667]]},
"""

config_file = os.environ.get('GRAPHITE_API_CONFIG', '/etc/graphite-api.yaml')
with open(config_file) as f:
    config = yaml.safe_load(f)

# echo stats | nc localhost 11211 | egrep 'cmd_.et|curr_items'

cfg = config.get('influxdb', {})
host = cfg.get('host', 'localhost')
port = cfg.get('port', 8086)
user = cfg.get('user', 'graphite')
passw = cfg.get('pass', 'graphite')
db = cfg.get('db', 'graphite')

client = InfluxDBClient(host, port, user, passw, db)

if config['cache']['CACHE_TYPE'] == 'memcached':
    cache = MemcachedCache(key_prefix=config['cache']['CACHE_KEY_PREFIX'])
elif config['cache']['CACHE_TYPE'] == 'filesystem':
    cache = FileSystemCache(config['cache']['CACHE_DIR'])
else:
    raise Exception("unsupported cache backend")


while True:

    print "BEGIN LOOP"
    start_loop = time.time()

    # first off, load series as quick as we can
    section = "influxdb:: list series"
    print section
    start = time.time()
    series = client.query("list series")
    duration(section, start)


    section = "building datastructures"
    print section
    start = time.time()

    series_list = [p[1] for p in series[0]['points']]
    from sys import getsizeof
    print "size of series data", getsizeof(series)
    print "size of series list", getsizeof(series_list)

    duration(section, start)


    section = "cache:: store series_list"
    print section
    start = time.time()

    cache.set("influxdb_list_series", series_list, 60 * 20)
    duration(section, start)


    if cfg.get('cheat_times'):
        print "cheating on start/end time. no further influx queries needed..."
        continue

    section = "influxdb:: select * from // order asc limit 1"
    print section
    start = time.time()
    series = client.query("select * from // order asc limit 1")
    duration(section, start)


    section = "cache:: store first-point for all series"
    print section
    start = time.time()

    for series_info in series:
        name = series_info['name']
        key_first = "%s_first" % name
        cache.set(key_first, series_info['points'][0][0], timeout=60 * 20)

    duration(section, start)
    duration("ENTIRE LOOP", start_loop)
