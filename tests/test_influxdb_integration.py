import unittest
import graphite_influxdb
import datetime

class Query(object):

    def __init__(self, pattern):
        self.pattern = pattern

class GraphiteInfluxdbIntegrationTestCase(unittest.TestCase):

    def setUp(self):
        self.step, self.num_datapoints = 60, 2
        self.config = { 'influxdb' : { 'host' : 'localhost',
                                       'port' : 8086,
                                       'user' : 'root',
                                       'pass' : 'root',
                                       'db' : 'graphite',
                                       'schema' : [('', 60)],
                                       'log_level' : 'debug',
                                       }}
        self.finder = graphite_influxdb.InfluxdbFinder(self.config)
        self.metric_prefix = "integration_test"
        self.series1, self.series2 = ".".join([self.metric_prefix, "leaf_node1"]), \
          ".".join([self.metric_prefix, "leaf_node2"])

    def test_find_series(self):
        """Test finding a series by name"""
        nodes = [node.name for node in self.finder.find_nodes(Query('integration_test'))
                 if node.is_leaf]
        expected = ['integration_test']
        self.assertEqual(nodes, expected,
                        msg="Got node list %s - wanted %s" % (nodes, expected))

    def test_find_series_wildcard(self):
        """Test finding all series by wildcard"""
        nodes = [node.name for node in self.finder.find_nodes(Query('*'))]
        expected = 'integration_test'
        self.assertTrue(expected in nodes,
                        msg="Node list does not contain '%s' - %s" % (expected, nodes))

    def test_find_leaf_nodes(self):
        """Test finding leaf nodes by wildcard"""
        nodes = [node.name for node in self.finder.find_nodes(Query('integration_test.*'))]
        expected = ['leaf_node1', 'leaf_node2']
        self.assertEqual(nodes, expected,
                         msg="Expected leaf node list '%s' - got %s" % (expected, nodes))

    def test_multi_fetch_data(self):
        """Test fetching data for a single series by name"""
        nodes = list(self.finder.find_nodes(Query(self.series1)))
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(hours=1)), \
          datetime.datetime.now()
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(start_time.strftime("%s")),
                                                  int(end_time.strftime("%s")))
        self.assertTrue(self.series1 in data,
                        msg="Did not get data for requested series %s - got data for %s" % (
                            self.series1, data.keys(),))
        self.assertEqual(time_info,
                         (int(start_time.strftime("%s")),
                          int(end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        datapoints = [v for v in data[self.series1] if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))

    def test_single_fetch_data(self):
        """Test single fetch data for a series by name"""
        node = list(self.finder.find_nodes(Query(self.series1)))[0]
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(hours=1)), \
          datetime.datetime.now()
        time_info, data = node.reader.fetch(int(start_time.strftime("%s")),
                                            int(end_time.strftime("%s")))
        datapoints = [v for v in data[self.series1] if v]
        self.assertTrue(len(datapoints) == self.num_datapoints,
                        msg="Expected %s datapoints - got %s" % (
                            self.num_datapoints, len(datapoints),))
        
    def test_multi_fetch_data_multi_series(self):
        """Test fetching data for multiple series by name"""
        nodes = list(self.finder.find_nodes(Query(self.metric_prefix + ".*")))
        start_time, end_time = (datetime.datetime.now() - datetime.timedelta(hours=1)), \
          datetime.datetime.now()
        time_info, data = self.finder.fetch_multi(nodes,
                                                  int(start_time.strftime("%s")),
                                                  int(end_time.strftime("%s")))
        self.assertTrue(self.series1 in data and self.series2 in data,
                        msg="Did not get data for requested series %s and %s - got data for %s" % (
                            self.series1, self.series2, data.keys(),))
        self.assertEqual(time_info,
                         (int(start_time.strftime("%s")),
                          int(end_time.strftime("%s")),
                         self.step),
                         msg="Time info and step do not match our requested values")
        for series in [self.series1, self.series2]:
            datapoints = [v for v in data[series] if v]
            self.assertTrue(len(datapoints) == self.num_datapoints,
                            msg="Expected %s datapoints for series %s - got %s" % (
                                self.num_datapoints, series, len(datapoints),))
