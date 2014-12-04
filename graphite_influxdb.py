import re
import time
import logging
from logging.handlers import TimedRotatingFileHandler

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

from influxdb import InfluxDBClient

class NullStatsd():
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def timer(self, key, val=None):
        return self

    def timing(self, key, val):
        pass


# in case graphite-api doesn't have statsd configured,
# just use dummy one that doesn't do anything
# (or if you use graphite-web which just doesn't support statsd)
try:
    from graphite_api.app import app
    statsd = app.statsd
    assert statsd is not None
except:
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
        ret['schema'] = cfg.get('schema', [])
        ret['log_file'] = cfg.get('log_file', None)
        ret['log_level'] = cfg.get('log_level', 'info')
    else:
        from django.conf import settings
        ret['host'] = getattr(settings, 'INFLUXDB_HOST', 'localhost')
        ret['port'] = getattr(settings, 'INFLUXDB_PORT', 8086)
        ret['user'] = getattr(settings, 'INFLUXDB_USER', 'graphite')
        ret['passw'] = getattr(settings, 'INFLUXDB_PASS', 'graphite')
        ret['db'] = getattr(settings, 'INFLUXDB_DB', 'graphite')
        ret['schema'] = getattr(settings, 'INFLUXDB_SCHEMA', [])
        ret['log_file'] = getattr(
            settings, 'INFLUXDB_LOG_FILE', None)
        ret['log_level'] = getattr(
            settings, 'INFLUXDB_LOG_LEVEL', 'info')
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
        # logger.debug(caller="fetch()", start_time=start_time, end_time=end_time, step=self.step, debug_key=self.path)
        with statsd.timer('service=graphite-api.ext_service=influxdb.target_type=gauge.unit=ms.what=query_individual_duration'):
            _query = 'select time, value from "%s" where time > %ds and time < %ds order asc' % (
                self.path, start_time, end_time + 1)
            logger.debug("Calling influxdb with query - %s" % _query)
            data = self.client.query(_query)
        # logger.debug(caller="fetch()", returned_data=data, debug_key=self.path)
        try:
            known_points = data[0]['points']
        except Exception:
            # logger.debug(caller="fetch()", msg="COULDN'T READ POINTS. SETTING TO EMPTY LIST", debug_key=self.path)
            known_points = []
        # logger.debug(caller="fetch()", msg="invoking fix_datapoints()", debug_key=self.path)
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
            # logger.debug(caller="fix_datapoints_multi", msg="invoking fix_datapoints()", debug_key=seriesdata['name'])
            datapoints = InfluxdbReader.fix_datapoints(seriesdata['points'], start_time, end_time, step, seriesdata['name'])
            out[seriesdata['name']] = datapoints
        return out

    @staticmethod
    def fix_datapoints(known_points, start_time, end_time, step, debug_key):
        """
        points is a list of known points (potentially empty)
        """
        # logger.debug(caller='fix_datapoints', len_known_points=len(known_points), debug_key=debug_key)
        # if len(known_points) == 1:
        #     logger.debug(caller='fix_datapoints', only_known_point=known_points[0], debug_key=debug_key)
        # elif len(known_points) > 1:
        #     logger.debug(caller='fix_datapoints', first_known_point=known_points[0], debug_key=debug_key)
        #     logger.debug(caller='fix_datapoints', last_known_point=known_points[-1], debug_key=debug_key)

        datapoints = []
        steps = int(round((end_time - start_time) * 1.0 / step))
        # if we have 3 datapoints: at 0, at 60 and 120, then step is 60, steps = 2 and should have 3 points
        # note that graphite assumes data at quantized intervals, whereas in influx they can be stored at like 07, 67, etc.
        ratio = len(known_points) * 1.0 / (steps + 1)
        statsd.timer('service=graphite-api.target_type=gauge.unit=none.what=known_points/needed_points', ratio)

        if len(known_points) == steps + 1:
            # logger.debug(action="No steps missing", debug_key=debug_key)
            datapoints = [p[2] for p in known_points]
        else:
            amount = steps + 1 - len(known_points)
            # logger.debug(action="Fill missing steps with None values", amount=amount, debug_key=debug_key)
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

        # logger.debug(caller='fix_datapoints', len_known_points=len(known_points), len_datapoints=len(datapoints), debug_key=debug_key)
        # logger.debug(caller='fix_datapoints', first_returned_point=datapoints[0], debug_key=debug_key)
        # logger.debug(caller='fix_datapoints', last_returned_point=datapoints[-1], debug_key=debug_key)
        return datapoints

    def get_intervals(self):
            now = int(time.time())
            return IntervalSet([Interval(1, now)])


class InfluxLeafNode(LeafNode):
    __fetch_multi__ = 'influxdb'


class InfluxdbFinder(object):
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'schemas', 'cache')

    def __init__(self, config=None):
        try:
            from graphite_api.app import app
            self.cache = app.cache
        except:
            from django.core.cache import cache
            self.cache = cache
        # import ipdb; ipdb.set_trace()
        config = normalize_config(config)
        self.client = InfluxDBClient(config['host'], config['port'], config['user'], config['passw'], config['db'])
        self.schemas = [(re.compile(patt), step) for (patt, step) in config['schema']]
        self._setup_logger(config['log_level'], config['log_file'])

    def _setup_logger(self, level, log_file):
        """Setup log level and log file if set"""
        level = getattr(logging, level.upper())
        logger.setLevel(level)
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(module)s.%(funcName)s() - %(message)s')
        if not log_file:
            return
        try:
            handler = TimedRotatingFileHandler(log_file)
        except IOError:
            handler = logging.StreamHandler()
            logger.addHandler(handler)
            logger.error("Could not write to %s, falling back to stdout",
                         log_file)
        else:
            logger.addHandler(handler)
        handler.setFormatter(formatter)

    def assure_series(self, query):
        regex = self.compile_regex(query, True)

        key_series = "%s_series" % query.pattern
        with statsd.timer('service=graphite-api.action=cache_get_series.target_type=gauge.unit=ms'):
            series = self.cache.get(key_series)
        if series is not None:
            return series
        # if not in cache, generate from scratch
        # first we must load the list with all nodes
        with statsd.timer('service=graphite-api.ext_service=influxdb.target_type=gauge.unit=ms.action=get_series'):
            _query = "list series /%s/" % regex.pattern
            ret = self.client.query(_query)
            logger.debug("Calling influxdb with query - %s" % _query)
            # as long as influxdb doesn't have good safeguards against series with bad data in the metric names, we must filter out like so:
            series = [serie[1] for serie in ret[0]['points'] if serie[1].encode('ascii', 'ignore') == serie[1]]

        # store in cache
        with statsd.timer('service=graphite-api.action=cache_set_series.target_type=gauge.unit=ms'):
            self.cache.add(key_series, series, timeout=300)
        return series

    def compile_regex(self, query, series=False):
        # we turn graphite's custom glob-like thing into a regex, like so:
        # * becomes [^\.]*
        # . becomes \.

        if series:
            regex = '^{0}'
        else:
            regex = '^{0}$'

        regex = regex.format(
            query.pattern.replace('.', '\.').replace('*', '[^\.]*').replace('{', '(').replace(',', '|').replace('}', ')')
        )
        # logger.debug("searching for nodes", pattern=query.pattern, regex=regex)
        return re.compile(regex)

    def get_leaves(self, query):
        key_leaves = "%s_leaves" % query.pattern
        # with statsd.timer('service=graphite-api.action=cache_get_leaves.target_type=gauge.unit=ms'):
        #     data = self.cache.get(key_leaves)
        # if data is not None:
        #     return data
        data = None
        series = self.assure_series(query)
        regex = self.compile_regex(query)
        # logger.debug(caller="get_leaves", key=key_leaves)
        leaves = []
        with statsd.timer('service=graphite-api.action=find_leaves.target_type=gauge.unit=ms'):
            for name in series:
                if regex.match(name) is not None:
                    # logger.debug("found leaf", name=name)
                    res = 60  # fallback default
                    for (rule_patt, rule_res) in self.schemas:
                        if rule_patt.match(name):
                            res = rule_res
                            break
                    leaves.append([name, res])
        # with statsd.timer('service=graphite-api.action=cache_set_leaves.target_type=gauge.unit=ms'):
        #     self.cache.add(key_leaves, leaves, timeout=300)
        return leaves

    def get_branches(self, query):
        seen_branches = set()
        key_branches = "%s_branches" % query.pattern
        # with statsd.timer('service=graphite-api.action=cache_get_branches.target_type=gauge.unit=ms'):
        #     data = self.cache.get(key_branches)
        # if data is not None:
        #     return data
        data = None
        series = self.assure_series(query)
        regex = self.compile_regex(query)
        # logger.debug(caller="get_branches", key=key_branches)
        branches = []
        with statsd.timer('service=graphite-api.action=find_branches.target_type=gauge.unit=ms'):
            for name in series:
                while '.' in name:
                    name = name.rsplit('.', 1)[0]
                    if name not in seen_branches:
                        seen_branches.add(name)
                        if regex.match(name) is not None:
                            # logger.debug("found branch", name=name)
                            branches.append(name)
        # with statsd.timer('service=graphite-api.action=cache_set_branches.target_type=gauge.unit=ms'):
        #     self.cache.add(key_branches, branches, timeout=300)
        return branches

    def find_nodes(self, query):
        # import ipdb; ipdb.set_trace()
        # logger.debug("Got find nodes query %s" % query.pattern)
        # TODO: once we can query influx better for retention periods, honor the start/end time in the FindQuery object
        with statsd.timer('service=graphite-api.action=yield_nodes.target_type=gauge.unit=ms.what=query_duration'):
            for (name, res) in self.get_leaves(query):
                yield InfluxLeafNode(name, InfluxdbReader(self.client, name, res, self.cache))
            for name in self.get_branches(query):
                yield BranchNode(name)

    def fetch_multi(self, nodes, start_time, end_time):
        series = ', '.join(['"%s"' % node.path for node in nodes])
        # use the step of the node that is the most coarse
        # not sure if there's a batter way? can we combine series with different steps (and use the optimal step for each?)
        # probably not
        step = max([node.reader.step for node in nodes])
        query = 'select time, value from %s where time > %ds and time < %ds order asc' % (
                series, start_time, end_time + 1)
        # logger.debug(caller='fetch_multi', query=query)
        # logger.debug(caller='fetch_multi', start_time=print_time(start_time), end_time=print_time(end_time), step=step)
        with statsd.timer('service=graphite-api.ext_service=influxdb.target_type=gauge.unit=ms.action=select_datapoints'):
            # import ipdb; ipdb.set_trace()
            logger.debug("Calling influxdb multi fetch with query - %s" % query)
            data = self.client.query(query)
        # logger.debug(caller='fetch_multi', returned_data=data)
        if not len(data):
            data = [{'name': node.path, 'points': []} for node in nodes]
            # logger.debug(caller='fetch_multi', FIXING_DATA_TO=data)
        # logger.debug(caller='fetch_multi', len_datapoints_before_fixing=len(data))

        with statsd.timer('service=graphite-api.action=fix_datapoints_multi.target_type=gauge.unit=ms'):
            # logger.debug(caller='fetch_multi', action='invoking fix_datapoints_multi()')
            datapoints = InfluxdbReader.fix_datapoints_multi(data, start_time, end_time, step)

        time_info = start_time, end_time, step
        return time_info, datapoints
