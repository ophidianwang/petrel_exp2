# -*- coding: utf-8 -*-
"""
Created on Tue Jan 05 11:07:26 2016

@author: Jonathan Wang
"""

import time
import logging
from petrel import storm
from petrel.emitter import BasicBolt

log = logging.getLogger('SplitBolt')  # set logger


class SplitBolt(BasicBolt):
    """
    此bolt的目的為: 將接收到的csv line, 切分成row, 並只傳有興趣的欄位給下個bolt
    """

    def __init__(self):
        super(SplitBolt, self).__init__(script=__file__)
        self.counter = 0
        log.debug("SplitBolt.__init__")

    @classmethod
    def declareOutputFields(self):
        """
        定義emit欄位(設定tuple group條件用)
        """
        return ['msisdn', 'rec_opening_time', 'uplink', 'downlink']

    def process(self, tup):
        """
        將接收到的csv line, 切分成row, 並只傳有興趣的欄位給下個bolt
        """
        if tup.is_tick_tuple():
            log.debug("tuple is tick")
        else:
            line = tup.values[0]
            line = line.strip()[8:]  # remove "message:" added by fluentd
            # log.warning("SplitBolt process: %s", line.strip())
            raw_row = line.split(",")
            if len(raw_row) == 47:
                storm.emit([raw_row[6], raw_row[4], raw_row[17], raw_row[18]])
                if self.counter == 0:
                    log.warning("start process 1000000 records at {0} (timestamp)".format(time.time()))
                self.counter += 1
                if self.counter == 1000000:  # this won't work since more than on instance
                    log.warning("finish process 1000000 records at {0} (timestamp)".format(time.time()))


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    SplitBolt().run()
