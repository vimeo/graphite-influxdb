import re
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from influxdb.influxdb08 import InfluxDBClient

logger = logging.getLogger('graphite_influxdb')

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    try:
        from graphite.intervals import Interval, IntervalSet
        from graphite.node import LeafNode, BranchNode
    except ImportError:
        raise SystemExit(1, "You have neither graphite_api nor \
    the graphite webapp in your pythonpath")


class NullStatsd():
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def timer(self, key, val=None):
        return self

    def timing(self, key, val):
        pass

    def start(self):
        pass

    def stop(self):
        pass


class FakeCache(object):
    """Fake cache object with dummy add/get methods for use in the case
    that cache is not configured"""

    def get(self, *args):
        return

    def add(self, *args, **kwargs):
        pass

_CACHE = None
try:
    from graphite_api.app import app
except ImportError:
    try:
        from django.core.cache import cache
    except ImportError:
        raise SystemExit(1, "You have neither graphite_api nor \
    django in your pythonpath")
    else:
        _CACHE = cache
else:
    # Using getattr to not break horribly if app.cache is not set
    _CACHE = getattr(app, 'cache', None)
finally:
    if not _CACHE:
        _CACHE = FakeCache()

# in case graphite-api doesn't have statsd configured,
# just use dummy one that doesn't do anything
# (or if you use graphite-web which just doesn't support statsd)
try:
    from graphite_api.app import app
    statsd = app.statsd
    assert statsd is not None
except (ImportError, AssertionError):
    statsd = NullStatsd()

# if you want to manually set a statsd client, do this:
# from statsd import StatsClient
# statsd = StatsClient("host", 8125)


def print_time(t=None):
    """
    t unix timestamp or None
    """
    if t is None:
        t = time.time()
    return "%d (%s)" % (t, time.ctime(t))


def normalize_config(config=None):
    ret = {}
    if config is not None:
        cfg = config.get('influxdb', {})
        ret['host'] = cfg.get('host', 'localhost')
        ret['port'] = cfg.get('port', 8086)
        ret['user'] = cfg.get('user', 'graphite')
        ret['passw'] = cfg.get('pass', 'graphite')
        ret['db'] = cfg.get('db', 'graphite')
        ssl = cfg.get('ssl', False)
        ret['ssl'] = (ssl == 'true')
        ret['schema'] = cfg.get('schema', [])
        ret['log_file'] = cfg.get('log_file', None)
        ret['log_level'] = cfg.get('log_level', 'info')
        cfg = config.get('es', {})
        ret['es_enabled'] = cfg.get('enabled', False)
        ret['es_index'] = cfg.get('index', 'graphite_metrics2')
        ret['es_hosts'] = cfg.get('hosts', ['localhost:9200'])
        ret['es_field'] = cfg.get('field', '_id')
    else:
        from django.conf import settings
        ret['host'] = getattr(settings, 'INFLUXDB_HOST', 'localhost')
        ret['port'] = getattr(settings, 'INFLUXDB_PORT', 8086)
        ret['user'] = getattr(settings, 'INFLUXDB_USER', 'graphite')
        ret['passw'] = getattr(settings, 'INFLUXDB_PASS', 'graphite')
        ret['db'] = getattr(settings, 'INFLUXDB_DB', 'graphite')
        ssl = getattr(settings, 'INFLUXDB_SSL', False)
        ret['ssl'] = (ssl == 'true')
        ret['schema'] = getattr(settings, 'INFLUXDB_SCHEMA', [])
        ret['log_file'] = getattr(
            settings, 'INFLUXDB_LOG_FILE', None)
        # Default log level is 'info'
        ret['log_level'] = getattr(
            settings, 'INFLUXDB_LOG_LEVEL', 'info')
        ret['es_enabled'] = getattr(settings, 'ES_ENABLED', False)
        ret['es_index'] = getattr(settings, 'ES_INDEX', 'graphite_metrics2')
        ret['es_hosts'] = getattr(settings, 'ES_HOSTS', ['localhost:9200'])
        ret['es_field'] = getattr(settings, 'ES_FIELD', '_id')
    return ret


class InfluxdbReader(object):
    __slots__ = ('client', 'path', 'step', 'cache')

    def __init__(self, client, path, step, cache):
        self.client = client
        self.path = path
        self.step = step
        self.cache = cache

    def fetch(self, start_time, end_time):
        # in graphite,
        # from is exclusive (from=foo returns data at ts=foo+1 and higher)
        # until is inclusive (until=bar returns data at ts=bar and lower)
        # influx doesn't support <= and >= yet, hence the add.
        logger.debug("fetch() path=%s start_time=%s, end_time=%s, step=%d", self.path, start_time, end_time, self.step)
        with statsd.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.what_is_query_individual_duration'):
            _query = 'select time, value from "%s" where time > %ds and time < %ds order asc' % (
                self.path, start_time, end_time + 1)
            logger.debug("fetch() path=%s querying influxdb query: '%s'", self.path, _query)
            data = self.client.query(_query)
        logger.debug("fetch() path=%s returned data: %s", self.path, data)
        try:
            known_points = data[0]['points']
        except Exception:
            logger.debug("fetch() path=%s COULDN'T READ POINTS. SETTING TO EMPTY LIST", self.path)
            known_points = []
        logger.debug("fetch() path=%s invoking fix_datapoints()", self.path)
        datapoints = InfluxdbReader.fix_datapoints(known_points, start_time, end_time, self.step, self.path)

        time_info = start_time, end_time, self.step
        return time_info, datapoints

    @staticmethod
    def fix_datapoints_multi(data, start_time, end_time, step):
        out = {}
        """
        data looks like:
        [{u'columns': [u'time', u'sequence_number', u'value'],
          u'name': u'stats.timers.dfvimeoplayproxy3.varnish.miss.410.count_ps',
            u'points': [[1402928319, 1, 0.133333],
            ....
        """
        for seriesdata in data:
            logger.debug("fix_datapoints_multi() on series with name %s invoking fix_datapoints()", seriesdata['name'])
            datapoints = InfluxdbReader.fix_datapoints(seriesdata['points'], start_time, end_time, step, seriesdata['name'])
            out[seriesdata['name']] = datapoints
        return out

    @staticmethod
    def fix_datapoints(known_points, start_time, end_time, step, debug_key):
        """
        points is a list of known points (potentially empty)
        """
        logger.debug("fix_datapoints() key=%s len_known_points=%d", debug_key, len(known_points))
        if len(known_points) == 1:
            logger.debug("fix_datapoints() key=%s only_known_point=%s", debug_key, known_points[0])
        elif len(known_points) > 1:
            logger.debug("fix_datapoints() key=%s first_known_point=%s", debug_key, known_points[0])
            logger.debug("fix_datapoints() key=%s last_known_point=%s", debug_key, known_points[-1])

        datapoints = []
        steps = int(round((end_time - start_time) * 1.0 / step))
        # if we have 3 datapoints: at 0, at 60 and 120, then step is 60, steps = 2 and should have 3 points
        # note that graphite assumes data at quantized intervals, whereas in influx they can be stored at like 07, 67, etc.
        ratio = len(known_points) * 1.0 / (steps + 1)
        statsd.timer('service_is_graphite-api.target_type_is_gauge.unit_is_none.what_is_known_points/needed_points', ratio)

        if len(known_points) == steps + 1:
            logger.debug("fix_datapoints() key=%s -> no steps missing!", debug_key)
            datapoints = [p[2] for p in known_points]
        else:
            amount = steps + 1 - len(known_points)
            logger.debug("fix_datapoints() key=%s -> fill %d missing steps with None values", debug_key, amount)
            next_point = 0
            for s in range(0, steps + 1):
                # if we have no more known points, fill with None's
                # even ininitially when next_point = 0, len(known_points) might be == 0
                if next_point >= len(known_points):
                    datapoints.append(None)
                    continue

                # if points are not evenly spaced. i.e. they should be a minute apart but sometimes they are 55 or 65 seconds,
                # and if they are all about step/2 away from the target timestamps, then sometimes a target point has 2 candidates, and
                # sometimes 0. So a point might be more than step/2 older.  in that case, since points are sorted, we can just forward the pointer
                # influxdb's fill(null) will make this cleaner and stop us from having to worry about this.

                should_be_near = start_time + step * s
                diff = known_points[next_point][0] - should_be_near
                while next_point + 1 < len(known_points) and diff < (step / 2) * -1:
                    next_point += 1
                    diff = known_points[next_point][0] - should_be_near

                # use this point if it's within step/2 from our target
                if abs(diff) <= step / 2:
                    datapoints.append(known_points[next_point][2])
                    next_point += 1  # note: might go out of bounds, which we use as signal

                else:
                    datapoints.append(None)

        logger.debug("fix_datapoints() key=%s len_known_points=%d, len_datapoints=%d", debug_key, len(known_points), len(datapoints))
        logger.debug("fix_datapoints() key=%s first_returned_point=%s, last_returned_point=%s", debug_key, datapoints[0], datapoints[-1])
        return datapoints

    def get_intervals(self):
            now = int(time.time())
            return IntervalSet([Interval(1, now)])


class InfluxLeafNode(LeafNode):
    __fetch_multi__ = 'influxdb'


class InfluxdbFinder(object):
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'es', 'schemas', 'cache', 'config')

    def __init__(self, config=None):
        # Shouldn't be trying imports in __init__.
        # It turns what should be a load error into a runtime error
        # If cache is not configured, use fake cache object
        # so that calls to cache.add/get will not break
        self.cache = _CACHE
        config = normalize_config(config)
        self.config = config
        self.client = InfluxDBClient(config['host'], config['port'], config['user'], config['passw'], config['db'], config['ssl'])
        self.schemas = [(re.compile(patt), step) for (patt, step) in config['schema']]
        self._setup_logger(config['log_level'], config['log_file'])
        self.es = None
        if config['es_enabled']:
            from elasticsearch import Elasticsearch
            self.es = Elasticsearch(config['es_hosts'])

    def _setup_logger(self, level, log_file):
        """Setup log level and log file if set"""
        level = getattr(logging, level.upper())
        logger.setLevel(level)
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(module)s.%(funcName)s() - %(message)s')
        handler = logging.StreamHandler()
        logger.addHandler(handler)
        handler.setFormatter(formatter)
        if not log_file:
            return
        try:
            handler = TimedRotatingFileHandler(log_file)
        except IOError:
            logger.error("Could not write to %s, falling back to stdout",
                         log_file)
        else:
            logger.addHandler(handler)
            handler.setFormatter(formatter)

    def assure_series(self, query):
        key_series = "%s_series" % query.pattern
        with statsd.timer('service_is_graphite-api.action_is_cache_get_series.target_type_is_gauge.unit_is_ms'):
            series = self.cache.get(key_series)
        if series is not None:
            return series
        # if not in cache, generate from scratch
        # if ES configured, try it first, it's usually fastest.
        done = False
        if self.es:
            # note: ES always treats a regex as anchored at start and end
            regex = self.compile_regex('{0}.*', query)
            with statsd.timer('service_is_graphite-api.ext_service_is_elasticsearch.target_type_is_gauge.unit_is_ms.action_is_get_series'):
                logger.debug("assure_series() Calling ES with regexp - %s", regex.pattern)
                try:
                    res = self.es.search(index=self.config['es_index'],
                                         size=10000,
                                         body={
                                             "query": {
                                                 "regexp": {
                                                     self.config['es_field']: regex.pattern,
                                                 },
                                             },
                                             "fields": [self.config['es_field']]
                                         }
                                         )
                    if res['_shards']['successful'] > 0:
                        # pprint(res['hits']['total'])
                        series = [hit['fields'][self.config['es_field']] for hit in res['hits']['hits']]
                        done = True
                    else:
                        logger.error("assure_series() Calling ES failed for %s: no successful shards", regex.pattern)
                except Exception, e:
                    logger.error("assure_series() Calling ES failed for %s: %s", regex.pattern, e)
        # if no ES configured, or ES failed, try influxdb.
        if not done:
            # regexes in influxdb are not assumed to be anchored, so anchor them explicitly
            regex = self.compile_regex('^{0}', query)
            with statsd.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.action_is_get_series'):
                _query = "list series /%s/" % regex.pattern
                logger.debug("assure_series() Calling influxdb with query - %s", _query)
                ret = self.client.query(_query)
                # as long as influxdb doesn't have good safeguards against
                # series with bad data in the metric names, we must filter out
                # like so:
                series = [serie[1] for serie in ret[0]['points']
                          if serie[1].encode('ascii', 'ignore') == serie[1]]

        # store in cache
        with statsd.timer('service_is_graphite-api.action_is_cache_set_series.target_type_is_gauge.unit_is_ms'):
            self.cache.add(key_series, series, timeout=300)
        return series

    def compile_regex(self, fmt, query):
        """Turn glob (graphite) queries into compiled regex
        * becomes .*
        . becomes \.
        fmt argument is so that caller can control anchoring (must contain exactly 1 {0} !"""
        return re.compile(fmt.format(
            query.pattern.replace('.', '\.').replace('*', '[^\.]*').replace(
                '{', '(').replace(',', '|').replace('}', ')')
        ))

    def get_leaves(self, query):
        key_leaves = "%s_leaves" % query.pattern
        with statsd.timer('service_is_graphite-api.action_is_cache_get_leaves.target_type_is_gauge.unit_is_ms'):
            data = self.cache.get(key_leaves)
        if data is not None:
            return data
        series = self.assure_series(query)
        regex = self.compile_regex('^{0}$', query)
        logger.debug("get_leaves() key %s", key_leaves)
        timer = statsd.timer('service_is_graphite-api.action_is_find_leaves.target_type_is_gauge.unit_is_ms')
        now = datetime.datetime.now()
        timer.start()
        # return every matching series and its
        # resolution (based on first pattern match in schema, fallback to 60s)
        leaves = [(name, next((res for (patt, res) in self.schemas if patt.match(name)), 60))
                  for name in series if regex.match(name)
                  ]
        timer.stop()
        end = datetime.datetime.now()
        dt = end - now
        logger.debug("get_leaves() key %s Finished find_leaves in %s.%ss",
                     key_leaves,
                     dt.seconds,
                     dt.microseconds)
        with statsd.timer('service_is_graphite-api.action_is_cache_set_leaves.target_type_is_gauge.unit_is_ms'):
            self.cache.add(key_leaves, leaves, timeout=300)
        return leaves

    def get_branches(self, query):
        seen_branches = set()
        key_branches = "%s_branches" % query.pattern
        with statsd.timer('service_is_graphite-api.action_is_cache_get_branches.target_type_is_gauge.unit_is_ms'):
            data = self.cache.get(key_branches)
        if data is not None:
            return data
        # Very inefficient call to list
        series = self.assure_series(query)
        regex = self.compile_regex('^{0}$', query)
        logger.debug("get_branches() %s", key_branches)
        timer = statsd.timer('service_is_graphite-api.action_is_find_branches.target_type_is_gauge.unit_is_ms')
        start_time = datetime.datetime.now()
        timer.start()
        branches = []
        for name in series:
            while '.' in name:
                name = name.rsplit('.', 1)[0]
                if name not in seen_branches:
                    seen_branches.add(name)
                    if regex.match(name) is not None:
                        logger.debug("get_branches() %s found branch name: %s", key_branches, name)
                        branches.append(name)
        timer.stop()
        end_time = datetime.datetime.now()
        dt = end_time - start_time
        logger.debug("get_branches() %s Finished find_branches in %s.%ss",
                     key_branches,
                     dt.seconds, dt.microseconds)
        with statsd.timer('service_is_graphite-api.action_is_cache_set_branches.target_type_is_gauge.unit_is_ms'):
            self.cache.add(key_branches, branches, timeout=300)
        return branches

    def find_nodes(self, query):
        logger.debug("find_nodes() query %s", query.pattern)
        # TODO: once we can query influx better for retention periods, honor the start/end time in the FindQuery object
        with statsd.timer('service_is_graphite-api.action_is_yield_nodes.target_type_is_gauge.unit_is_ms.what_is_query_duration'):
            for (name, res) in self.get_leaves(query):
                yield InfluxLeafNode(name, InfluxdbReader(
                    self.client, name, res, self.cache))
            for name in self.get_branches(query):
                yield BranchNode(name)

    def fetch_multi(self, nodes, start_time, end_time):
        series = ', '.join(['"%s"' % node.path for node in nodes])
        # use the step of the node that is the most coarse
        # not sure if there's a better way? can we combine series
        # with different steps (and use the optimal step for each?)
        # probably not
        step = max([node.reader.step for node in nodes])
        query = 'select time, value from %s where time > %ds and time < %ds order asc' % (
                series, start_time, end_time + 1)
        logger.debug('fetch_multi() query: %s', query)
        logger.debug('fetch_multi() - start_time: %s - end_time: %s, step %s',
                     print_time(start_time), print_time(end_time), step)

        with statsd.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.action_is_select_datapoints'):
            logger.debug("Calling influxdb multi fetch with query - %s", query)
            data = self.client.query(query)
        logger.debug('fetch_multi() - Retrieved %d datapoints', len(data))

        # some series we requested might not be in the resultset.
        # this is because influx doesn't include series that had no values
        # this is a behavior that some people actually appreciate when graphing, but graphite doesn't do this (yet),
        # and we want to look the same, so we must add those back in.
        # a better reason though, is because for advanced alerting cases like bosun, you want all entries even if they have no data, so you can properly
        # compare, join, or do logic with the targets returned for requests for the same data but from different time ranges, you want them to all
        # include the same keys.
        query_keys = set([node.path for node in nodes])
        seen_keys = set([m['name'] for m in data])
        missing_keys = query_keys.difference(seen_keys)
        if missing_keys:
            logger.debug('fetch_multi() - adding missing keys %s', missing_keys)
            for key in missing_keys:
                data.append({
                    'columns': ['time', 'sequence_number', 'value'],
                    'name': key,
                    'points': []
                })

        with statsd.timer('service_is_graphite-api.action_is_fix_datapoints_multi.target_type_is_gauge.unit_is_ms'):
            logger.debug('fetch_multi() - invoking fix_datapoints_multi()')
            datapoints = InfluxdbReader.fix_datapoints_multi(data, start_time, end_time, step)

        time_info = start_time, end_time, step
        return time_info, datapoints
