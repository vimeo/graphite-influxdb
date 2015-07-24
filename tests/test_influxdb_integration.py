import unittest
from influxdb import InfluxDBClient
import influxdb.exceptions
import graphite_influxdb
import datetime

class Query(object):

    def __init__(self, pattern):
        self.pattern = pattern

class GraphiteInfluxdbIntegrationTestCase(unittest.TestCase):

    def setup_db(self):
        try:
            self.client.drop_database(self.db_name)
        except influxdb.exceptions.InfluxDBClientError:
            pass
        self.client.create_database(self.db_name)
        data = [{
            "measurement": series,
            "tags": {},
            "time": _time,
            "fields": {
                "value": 1,
                }
            }
            for series in self.series
            for _time in [
                (self.end_time - datetime.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                (self.end_time - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                ]]
        self.assertTrue(self.client.write_points(data))

    def setUp(self):
        self.step, self.num_datapoints, self.db_name = 60, 2, 'integration_test'
        self.start_time, self.end_time = (datetime.datetime.utcnow() - datetime.timedelta(hours=1)), \
          datetime.datetime.utcnow()
        self.steps = int(round((int(self.end_time.strftime("%s")) - \
                                int(self.start_time.strftime("%s"))) * 1.0 / self.step)) + 1
        self.client = InfluxDBClient(database=self.db_name)
        self.config = { 'influxdb' : { 'host' : 'localhost',
                                       'port' : 8086,
                                       'user' : 'root',
                                       'pass' : 'root',
                                       'db' : self.db_name,
                                       'schema' : [('', 60)],
                                       'log_level' : 'debug',
                                       },}
        self.finder = graphite_influxdb.InfluxdbFinder(self.config)
        self.metric_prefix = "integration_test"
        self.nodes = ["leaf_node1", "leaf_node2"]
        self.series1, self.series2 = ".".join([self.metric_prefix, self.nodes[0]]), \
          ".".join([self.metric_prefix, self.nodes[1]])
        self.series = [self.series1, self.series2]
        self.setup_db()

    def test_compile_regex(self):
        metric_query_pat = 'metric_prefix.*'
        expected = "^metric_prefix\\.[^\\.]*$"
        rec = self.finder.compile_regex('^{0}$', Query(metric_query_pat))
        self.assertEqual(rec.pattern, expected,
                         msg="Got unexpected compiled regex pattern %s from graphite metric query %s. "
                         "Expected compiled pattern %s" % (
                             rec.pattern, metric_query_pat, expected,))

    def test_find_series(self):
        """Test finding a series by name"""
        nodes = [node.name for node in self.finder.find_nodes(Query(self.series1))
                 if node.is_leaf]
        expected = [self.nodes[0]]
        self.assertEqual(nodes, expected,
                        msg="Got node list %s - wanted %s" % (nodes, expected,))

    def test_find_series_wildcard(self):
        """Test finding metric prefix by wildcard"""
        nodes = [node.name for node in self.finder.find_nodes(Query('*'))]
        self.assertTrue(self.metric_prefix in nodes,
                        msg="Node list does not contain prefix '%s' - %s" % (
                            self.metric_prefix, nodes))

    def test_find_leaf_nodes(self):
        """Test finding leaf nodes by wildcard"""
        nodes = [node.name
                 for node in self.finder.find_nodes(Query(self.metric_prefix + ".*"))]
        expected = self.nodes
        self.assertEqual(nodes, expected,
                         msg="Expected leaf node list '%s' - got %s" % (expected, nodes))

    def test_multi_fetch_data(self):
        """Test fetching data for a single series by name"""
        nodes = list(self.finder.find_nodes(Query(self.series1)))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        self.assertEqual(time_info,
                         (int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        datapoints = [v for v in data[self.series1] if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))

    def test_single_fetch_data(self):
        """Test single fetch data for a series by name"""
        node = list(self.finder.find_nodes(Query(self.series1)))[0]
        time_info, data = node.reader.fetch(int(self.start_time.strftime("%s")),
                                            int(self.end_time.strftime("%s")))
        self.assertTrue(self.steps == len(data),
                        msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(data),))
        datapoints = [v for v in data if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))
        
    def test_multi_fetch_data_multi_series(self):
        """Test fetching data for multiple series by name"""
        nodes = list(self.finder.find_nodes(Query(self.metric_prefix + ".*")))
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(self.start_time.strftime("%s")),
                                                  int(self.end_time.strftime("%s")))
        self.assertTrue(self.series1 in data and self.series2 in data,
                        msg="Did not get data for requested series %s and %s - got data for %s" % (
                            self.series1, self.series2, data.keys(),))
        self.assertEqual(time_info,
                         (int(self.start_time.strftime("%s")),
                          int(self.end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        for series in [self.series1, self.series2]:
            self.assertTrue(self.steps == len(data[series]),
                            msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(data[series]),))
            datapoints = [v for v in data[series] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for series %s - got %s" % (
                                self.num_datapoints, series, len(datapoints),))
