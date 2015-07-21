import unittest
import graphite_influxdb
import datetime
import random

class Query(object):

    def __init__(self, pattern):
        self.pattern = pattern

class GraphiteInfluxdbIntegrationTestCase(unittest.TestCase):

    def setUp(self):
        self.config = { 'influxdb' : { 'host' : 'localhost',
                                       'port' : 8086,
                                       'user' : 'root',
                                       'pass' : 'root',
                                       'db' : 'graphite',
                                       'schema' : [('', 60)],
                                       'log_level' : 'debug',
                                       }}
        self.finder = graphite_influxdb.InfluxdbFinder(self.config)

    def test_find_series(self):
        """Test finding a series by name"""
        nodes = [node.name for node in self.finder.find_nodes(Query('integration_test'))]
        expected = ['integration_test']
        self.assertEqual(nodes, expected,
                         msg="Got node list %s - wanted %s" % (nodes, expected))
