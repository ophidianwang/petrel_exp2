# -*- coding: utf-8 -*-
"""
Created on Wed Jan 06 10:48:48 2016

@author: Jonathan Wang
"""
import logging
from pymongo import MongoClient

from petrel import storm
from petrel.emitter import BasicBolt

log = logging.getLogger('output_bolt')  # set logger


class OutputBolt(BasicBolt):
    """
    此bolt的目的為: 將接收到的msisdn與uplink & downlink累計結果寫入mongodb
    """

    split_freq_secs = 60

    def __init__(self):
        """
        開啟mongodb連線
        """
        super(OutputBolt, self).__init__(script=__file__)
        self.conf = None
        self.client = None
        self.db = None
        self.collection = None
        """
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.exp_db_name
        self.collection = self.db.exp_col_name
        """

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts up.
        :param conf: topology.yaml內的設定
        :param context:
        """
        log.debug("OutputBolt initialize")
        self.conf = conf
        self.client = MongoClient(conf["OutputBolt.initialize.host"], conf["OutputBolt.initialize.port"])
        self.db = self.client[conf["OutputBolt.initialize.db"]]
        self.collection = self.db[conf["OutputBolt.initialize.collection"]]

    @classmethod
    def declareOutputFields(self):
        """
        定義emit欄位(設定tuple group條件用)
        """
        return ['msisdn']

    def process(self, tup):
        """
        將接收到的tuple寫入mongodb
        若為tick tuple, 則log紀錄
        """
        if tup.is_tick_tuple():
            log.debug("tuple is tick")
        else:
            """
            doc = {"msisdn": tup.values[0],
                   "total_uplink": tup.values[1],
                   "total_downlink": tup.values[2],
                   "recors": tup.values[3]
                   }
            """
            fields = ["msisdn", "total_uplink", "total_downlink", "records"]
            doc = dict(zip(fields, tup.values))
            object_id = self.collection.insert_one(doc).inserted_id
            log.debug("insert doc: %s, object_id: %s.", doc, object_id)

    def getComponentConfiguration(self):
        """
        設定此bolt參數, 可控制tick tuple頻率
        """
        return {"topology.tick.tuple.freq.secs": self.split_freq_secs}


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    OutputBolt().run()
