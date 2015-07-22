import unittest
import graphite_influxdb
import datetime
import random
from pprint import pprint

class GraphiteInfluxdbTestCase(unittest.TestCase):

    def setUp(self):
        self.step = 60
        self.reader = graphite_influxdb.InfluxdbReader(None, None, self.step, None)
        st = datetime.datetime(2012, 01, 01, 10, 0, 5)
        self.start_time, self.end_time, self.series_name = st, \
          st + datetime.timedelta(minutes=10), 'my_series'
        self.steps = int(round((int(self.end_time.strftime("%s")) - int(self.start_time.strftime("%s"))) * 1.0 / self.step)) + 1
        self.datapoints = [(self.start_time + datetime.timedelta(minutes=1), 1),
                           # Multiple points in same step
                           (self.start_time + datetime.timedelta(minutes=3, seconds=10), 1),
                           (self.start_time + datetime.timedelta(minutes=3, seconds=30), 2),
                           (self.start_time + datetime.timedelta(minutes=3, seconds=40), 3),
                           (self.start_time + datetime.timedelta(minutes=9), 1),
                           ]

    def test_fix_datapoints(self):
        """Test that filling datapoints gives expected results"""
        self.reader.fix_datapoints(self.datapoints,
                                   int(self.start_time.strftime("%s")),
                                   int(self.end_time.strftime("%s")), self.step, self.series_name)
        self.assertTrue(self.steps == len(self.datapoints),
                        msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(self.datapoints),))
        valid_datapoints = [v[1] for v in self.datapoints if v[1]]
        self.assertTrue(len(valid_datapoints) == 3,
                        msg="Got unexpected number of valid datapoints, %s. Expected %s" % (
                            len(valid_datapoints), 3,))
        # Average of values within minute 3 in self.setUp
        self.assertEqual(valid_datapoints[1], 2.0,
                         msg="Got incorrect average value for multiple datapoints - got %s, expected %s" % (
                             valid_datapoints[1], 2.0,))

    def test_fix_datapoints_multi(self):
        """Test that filling datapoints gives expected results"""
        data = { self.series_name : self.datapoints,
                 'another_series' : self.datapoints[:],
                 }
        self.reader.fix_datapoints_multi(data,
                                         int(self.start_time.strftime("%s")),
                                         int(self.end_time.strftime("%s")), self.step)
        for series_name in data:
            self.assertTrue(self.steps == len(list(data[series_name])),
                            msg="Expected %s datapoints, got %s instead" % (
                                self.steps, len(list(data[series_name])),))
