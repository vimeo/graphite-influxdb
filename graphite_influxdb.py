import re
import structlog

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
    else:
        from django.conf import settings
        host = getattr(settings, 'INFLUXDB_HOST', 'localhost')
        port = getattr(settings, 'INFLUXDB_PORT', 8086)
        user = getattr(settings, 'INFLUXDB_USER', 'graphite')
        passw = getattr(settings, 'INFLUXDB_PASS', 'graphite')
        db = getattr(settings, 'INFLUXDB_DB', 'graphite')

    return InfluxDBClient(host, port, user, passw, db)


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
        data = self.client.query('select time, value from "%s" where time > %ds '
                                 'and time < %ds order asc' % (
                                     self.path, start_time, end_time + 1))
        datapoints = []
        try:
            points = data[0]['points']
        except Exception:
            points = []
        steps = int(round((end_time - start_time) * 1.0 / self.step))
        # if we have 3 datapoints: at 0, at 60 and 120, then step is 60, steps = 2 and should have 3 points
        # note that graphite assumes data at quantized intervals, whereas in influx they can be stored at like 07, 67, etc.
        if len(points) == steps + 1:
            logger.debug("No steps missing")
            datapoints = [p[2] for p in points]
        else:
            logger.debug("Fill missing steps with None values")
            next_point = 0
            for s in range(0, steps + 1):
                should_be_near = start_time + self.step * s
                # even ininitially when next_point = 0, len(points) might be == 0
                if len(points) > next_point and abs(points[next_point][0] - should_be_near) <= self.step / 2:
                    datapoints.append(points[next_point][2])
                    if next_point + 1 < len(points):
                        next_point += 1
                else:
                    datapoints.append(None)

        time_info = start_time, end_time, self.step
        logger.debug("influx REQUESTED RANGE for %s: %d to %d (both inclusive)" % (
            self.path, start_time, end_time + 1))
        logger.debug("influx RETURNED  RANGE for %s: %d to %d" % (
            self.path, start_time, end_time))
        return time_info, datapoints

    def get_intervals(self):
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


class InfluxdbFinder(object):
    __slots__ = ('client', 'schemas', 'cache')

    def __init__(self, config=None):
        from graphite_api.app import app
        self.cache = app.cache

        self.client = config_to_client(config)
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
                yield LeafNode(name, InfluxdbReader(self.client, name, res, self.cache))

            while '.' in name:
                name = name.rsplit('.', 1)[0]
                if name not in seen_branches:
                    seen_branches.add(name)
                    if regex.match(name) is not None:
                        logger.debug("found branch", name=name)
                        yield BranchNode(name)
