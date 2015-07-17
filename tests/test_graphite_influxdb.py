import unittest
import graphite_influxdb
import datetime
import random

class GraphiteInfluxdbTestCase(unittest.TestCase):

    def setUp(self):
        self.step = 60
        self.reader = graphite_influxdb.InfluxdbReader(None, None, self.step, None)
        self.start_time, self.end_time, self.series_name = datetime.datetime.now(), \
          datetime.datetime.now() + datetime.timedelta(hours=1), 'my_series'
        self.steps = int(round((int(self.end_time.strftime("%s")) - int(self.start_time.strftime("%s"))) * 1.0 / self.step))
        self.datapoints = [(self.start_time + datetime.timedelta(minutes=20), random.randint(1,5)),
                           # Two points in same step
                           (self.start_time + datetime.timedelta(minutes=40), random.randint(1,5)),
                           (self.start_time + datetime.timedelta(minutes=40, seconds=30), random.randint(1,5)),
                           (self.start_time + datetime.timedelta(minutes=60), random.randint(1,5)),
                           ]

    def test_fix_datapoints(self):
        """Test that filling datapoints gives expected results"""
        self.reader.fix_datapoints(self.datapoints,
                                   int(self.start_time.strftime("%s")),
                                   int(self.end_time.strftime("%s")), self.step, self.series_name)
        self.assertTrue(self.steps == len(self.datapoints),
                        msg="Expected %s datapoints, got %s instead" % (
                            self.steps, len(self.datapoints),))

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
