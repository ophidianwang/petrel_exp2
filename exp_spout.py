# -*- coding: utf-8 -*-
"""
Created on Tue Jan 05 11:07:12 2016

@author: Jonathan Wang
"""

import time
import logging
from pykafka import KafkaClient
from pykafka.exceptions import NoPartitionsForConsumerException
from petrel import storm
from petrel.emitter import Spout

log = logging.getLogger('exp_spout')  # set logger


class ExpSpout(Spout):
    """
    此spout目的為: 讀取檔案, 並且將每行內容傳給下個bolt
    """

    def __init__(self):
        """
        開啟kafka連線
        """
        super(ExpSpout, self).__init__(script=__file__)
        self.conf = None
        self.client = None
        self.topic = None
        self.consumer = None

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts up.
        :param conf: topology.yaml內的設定
        :param context:
        """
        log.debug("ExpSpout initialize start")
        self.conf = conf
        self.client = KafkaClient(hosts=conf["ExpSpout.initialize.hosts"])
        self.topic = self.client.topics[str(conf["ExpSpout.initialize.topics"])]
        self.consumer = self.topic.get_balanced_consumer(consumer_group=str(conf["ExpSpout.initialize.consumer_group"]),
                                                         zookeeper_connect=str(conf["ExpSpout.initialize.zookeeper"]),
                                                         consumer_timeout_ms=int(conf[
                                                             "ExpSpout.initialize.consumer_timeout_ms"]),
                                                         auto_commit_enable=True
                                                         )
        log.debug("ExpSpout initialize done")

    @classmethod
    def declareOutputFields(cls):
        """
        定義emit欄位(設定tuple group條件用)
        """
        return ['line']

    def nextTuple(self):
        """
        從kafka batch 讀取資料處理
        messages (m) are namedtuples with attributes:
        m.offset: message offset on topic-partition log (int)
        m.value: message (output of deserializer_class - default is raw bytes)
        """
        if self.consumer is None:
            log.debug("self.consumer is not ready yet.")
            return

        log.debug("ExpSpout.nextTuple()")
        time.sleep(3)  # prototype減速觀察
        cursor = 0
        try:
            for message in self.consumer:
                cursor += 1
                if message is not None:
                    log.debug("offset: %s \t value: %s", message.offset, message.value)
                    storm.emit([message.value])
                if cursor > 10000:  # prototype減量觀察
                    break
        except NoPartitionsForConsumerException:
            log.debug("NoPartitionsForConsumerException")


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    ExpSpout().run()
