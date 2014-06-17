import re
import structlog
import time

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

from influxdb import InfluxDBClient

logger = structlog.get_logger()


def config_to_client(config=None):
    if config is not None:
        cfg = config.get('influxdb', {})
        host = cfg.get('host', 'localhost')
        port = cfg.get('port', 8086)
        user = cfg.get('user', 'graphite')
        passw = cfg.get('pass', 'graphite')
        db = cfg.get('db', 'graphite')
        cheat_times = cfg.get('cheat_times', False)
    else:
        from django.conf import settings
        host = getattr(settings, 'INFLUXDB_HOST', 'localhost')
        port = getattr(settings, 'INFLUXDB_PORT', 8086)
        user = getattr(settings, 'INFLUXDB_USER', 'graphite')
        passw = getattr(settings, 'INFLUXDB_PASS', 'graphite')
        db = getattr(settings, 'INFLUXDB_DB', 'graphite')
        cheat_times = getattr(settings, 'INFLUXDB_CHEAT_TIMES', False)

    return InfluxDBClient(host, port, user, passw, db), cheat_times


class InfluxdbReader(object):
    __slots__ = ('client', 'path', 'step', 'cache', 'cheat_times')

    def __init__(self, client, path, step, cache, cheat_times):
        self.client = client
        self.path = path
        self.step = step
        self.cache = cache
        self.cheat_times = cheat_times

    def fetch(self, start_time, end_time):
        # in graphite,
        # from is exclusive (from=foo returns data at ts=foo+1 and higher)
        # until is inclusive (until=bar returns data at ts=bar and lower)
        # influx doesn't support <= and >= yet, hence the add.
        data = self.client.query('select time, value from "%s" where time > %ds '
                                 'and time < %ds order asc' % (
                                     self.path, start_time, end_time + 1))

        try:
            known_points = data[0]['points']
        except Exception:
            known_points = []
        datapoints = InfluxdbReader.fix_datapoints(known_points, start_time, end_time, self.step)

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
            datapoints = InfluxdbReader.fix_datapoints(seriesdata['points'], start_time, end_time, step)
            out[seriesdata['name']] = datapoints
        return out

    @staticmethod
    def fix_datapoints(known_points, start_time, end_time, step):
        """
        points is a list of known points (potentially empty)
        """
        datapoints = []
        steps = int(round((end_time - start_time) * 1.0 / step))
        # if we have 3 datapoints: at 0, at 60 and 120, then step is 60, steps = 2 and should have 3 points
        # note that graphite assumes data at quantized intervals, whereas in influx they can be stored at like 07, 67, etc.
        if len(known_points) == steps + 1:
            logger.debug("No steps missing")
            datapoints = [p[2] for p in known_points]
        else:
            logger.debug("Fill missing steps with None values")
            next_point = 0
            for s in range(0, steps + 1):
                should_be_near = start_time + step * s
                # even ininitially when next_point = 0, len(known_points) might be == 0
                if len(known_points) > next_point and abs(known_points[next_point][0] - should_be_near) <= step / 2:
                    datapoints.append(known_points[next_point][2])
                    if next_point + 1 < len(known_points):
                        next_point += 1
                else:
                    datapoints.append(None)
        return datapoints

    def get_intervals(self):
        if self.cheat_times:
            print "cheat_times enabled. this is gonna be quick.."
            now = int(time.time())
            return IntervalSet([Interval(1, now)])

        key_first = "%s_first" % self.path
        first = self.cache.get(key_first)
        if first is None:
            print "CACHE MISS", key_first
            q = 'select * from "%s" limit 1 order asc' % self.path
            first_data = self.client.query(q)
            first = 0
            valid_res = False
            try:
                first = first_data[0]['points'][0][0]
                valid_res = True
            except Exception:
                pass
            if valid_res:
                print "updating cache"
                self.cache.add(key_first, first, timeout=self.step * 1000)
        else:
            print "CACHE HIT", key_first

        key_last = "%s_last" % self.path
        last = self.cache.get(key_last)
        if last is None:
            print "CACHE MISS", key_last
            q = 'select * from "%s" limit 1' % self.path
            last_data = self.client.query(q)
            last = 0
            valid_res = False
            try:
                last = last_data[0]['points'][0][0]
                valid_res = True
            except Exception:
                pass
            if valid_res:
                print "updating cache"
                self.cache.add(key_last, last, timeout=self.step)
        else:
            print "CACHE HIT", key_last

        return IntervalSet([Interval(first, last)])


class InfluxLeafNode(LeafNode):
    __fetch_multi__ = 'influxdb'


class InfluxdbFinder(object):
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'schemas', 'cache', 'cheat_times')

    def __init__(self, config=None):
        from graphite_api.app import app
        self.cache = app.cache

        self.client, self.cheat_times = config_to_client(config)
        # we basically need to replicate /etc/carbon/storage-schemas.conf here
        # so that for a given series name, and given from/to, we know the corresponding steps in influx
        # for now we assume we don't do continuous queries yet, and have only one resolution per match-string
        # for now, edit your settings here, TODO make this properly configurable or have it stored in influx and query influx
        self.schemas = [(re.compile(''), 60)]

    def find_nodes(self, query):
        # query.pattern is basically regex, though * should become [^\.]+
        # and . \.
        # but list series doesn't support pattern matching/regex yet
        regex = '^{0}$'.format(
            query.pattern.replace('.', '\.').replace('*', '[^\.]+')
        )
        logger.info("searching for nodes", pattern=query.pattern, regex=regex)
        regex = re.compile(regex)
        series = self.cache.get("influxdb_list_series")
        if series is None:
            raise Exception("series not in cache. please run maintain_cache.py")

        seen_branches = set()

        for name in series:
            if regex.match(name) is not None:
                logger.debug("found leaf", name=name)
                res = 10
                for (rule_patt, rule_res) in self.schemas:
                    if rule_patt.match(name):
                        res = rule_res
                        break
                yield InfluxLeafNode(name, InfluxdbReader(self.client, name, res, self.cache, self.cheat_times))

            while '.' in name:
                name = name.rsplit('.', 1)[0]
                if name not in seen_branches:
                    seen_branches.add(name)
                    if regex.match(name) is not None:
                        logger.debug("found branch", name=name)
                        yield BranchNode(name)

    def fetch_multi(self, nodes, start_time, end_time):
        from pprint import pprint
        series = ', '.join(['"%s"' % node.path for node in nodes])
        step = 60  # TODO: this is not ideal in all cases. for one thing, don't hardcode, for another.. how to deal with multiple steps?
        query = 'select time, value from %s where time > %ds and time < %ds order asc' % (
                series, start_time, end_time + 1)
        data = self.client.query(query)

        datapoints = InfluxdbReader.fix_datapoints_multi(data, start_time, end_time, step)

        time_info = start_time, end_time, step
        return time_info, datapoints
