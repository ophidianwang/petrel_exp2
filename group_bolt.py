# -*- coding: utf-8 -*-
"""
Created on Tue Jan 05 11:07:42 2016

@author: Jonathan Wang
"""

import time
import logging
from collections import defaultdict

from petrel import storm
from petrel.emitter import BasicBolt

log = logging.getLogger('GroupBolt')    # set logger


class GroupBolt(BasicBolt):
    """
    此bolt目的為: 累計一定時間(量)的輸入資料中, 同個msisdn的uplink & downlink
    並輸出給下個bolt
    """

    emit_freq_secs = 20

    def __init__(self):
        super(GroupBolt, self).__init__(script=__file__)
        self.total_uplink = defaultdict(int)
        self.total_downlink = defaultdict(int)
        self.total_records = defaultdict(list)
        self.counter = 0

    @classmethod
    def declareOutputFields(self):
        """
        定義emit欄位(設定tuple group條件用)
        """
        return ['msisdn', 'total_uplink', 'total_downlink', "records"]

    def process(self, tup):
        """
        累計同個msisdn的uplink及downlink
        若為tick tuple, 則輸出累計紀錄給下個bolt
        """
        if tup.is_tick_tuple():
            log.debug("tuple is tick")
            # self.toNextBolt()
        else:
            # log.debug("handling record: %s", tup.values)
            self.counter += 1
            msisdn = tup.values[0]
            record_time = tup.values[1]
            try:
                uplink = int(tup.values[2])
                downlink = int(tup.values[3])
            except :
                log.error("transforming tup values failed: %s", tup)
                return
            self.total_uplink[msisdn] += uplink
            self.total_downlink[msisdn] += downlink
            self.total_records[msisdn].append(record_time)
            # log.warning("handling record #%s: %s at %s", self.counter, tup.values, time.time())
            if self.counter == 10000:
                # log.warning("to next bolt at {0} (timestamp)".format(time.time()))
                self.toNextBolt()

    def toNextBolt(self):
        """
        將累計的同個msisdn的uplink & downlink emit給下個bolt
        並重新累計
        """
        for msisdn in self.total_uplink:
            # see if we could pass list in storm tuple: False, emit members needs to be hashable
            # so ... merge list to str
            merged = ",".join(self.total_records[msisdn])
            # log.debug("%s", [msisdn, merged])
            storm.emit([msisdn, self.total_uplink[msisdn], self.total_downlink[msisdn], merged])

        # clear accumulator
        self.total_uplink.clear()
        self.total_downlink.clear()
        self.total_records.clear()
        self.counter = 0
    
    def getComponentConfiguration(self):
        """
        設定此bolt參數, 可控制tick tuple頻率
        """
        return {"topology.tick.tuple.freq.secs": self.emit_freq_secs}


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    GroupBolt().run()