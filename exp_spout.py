# -*- coding: utf-8 -*-
"""
Created on Tue Jan 05 11:07:12 2016

@author: Jonathan Wang
"""

import time
import logging
from threading import Thread
from pykafka import KafkaClient
from petrel import storm
from petrel.emitter import Spout

log = logging.getLogger('ExpSpout')  # set logger


class EmitThread(Thread):
    """
    async emit
    """

    def __init__(self, threadName):
        super(EmitThread, self).__init__(name=threadName)
        self.messages = []
        self.counter = 0

    def append(self, message):
        self.messages.append(message)

    def run(self):
        while True:
            if len(self.messages) != 0:
                # log.warning(self.messages.pop(0))
                storm.emit([self.messages.pop(0)])
                self.counter += 1
                if self.counter % 10000 == 0:
                    log.warning("#{0}".format(self.counter))
                if self.counter == 1000000:  # mark time
                    log.warning("emit process 1000000 records at {0} (timestamp)".format(time.time()))
            else:
                time.sleep(0.01)


class ExpSpout(Spout):
    """
    此spout目的為: 讀取檔案, 並且將每行內容傳給下個bolt
    """

    def __init__(self):
        """
        assign None to members
        """
        super(ExpSpout, self).__init__(script=__file__)
        self.conf = None
        self.client = None
        self.topic = None
        self.consumer = None
        self.counter = 0
        self.emit_thread = None

    def initialize(self, conf, context):
        """
        Storm calls this function when a task for this component starts up.
        :param conf: topology.yaml內的設定
        :param context:
        開啟kafka連線
        """
        log.debug("ExpSpout initialize start")
        self.conf = conf
        self.client = KafkaClient(hosts=conf["ExpSpout.initialize.hosts"])
        self.topic = self.client.topics[str(conf["ExpSpout.initialize.topics"])]
        self.consumer = self.topic.get_balanced_consumer(consumer_group=str(conf["ExpSpout.initialize.consumer_group"]),
                                                         zookeeper_connect=str(conf["ExpSpout.initialize.zookeeper"]),
                                                         consumer_timeout_ms=int(conf[
                                                                                     "ExpSpout.initialize.consumer_timeout_ms"]),
                                                         auto_commit_enable=False
                                                         )

        self.emit_thread = EmitThread("emit_thread")
        self.emit_thread.start()

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

        if self.counter >= 1000000:
            return

        # log.debug("ExpSpout.nextTuple()")
        # time.sleep(3)  # prototype減速觀察
        try:
            for message in self.consumer:
                if message is not None:
                    # log.warning("offset: %s \t value: %s \t at %s", message.offset, message.value, time.time())
                    if self.counter == 0:
                        log.warning("start process 1000000 records at {0} (timestamp)".format(time.time()))
                    self.counter += 1
                    self.emit_thread.append(message.value)
                    # storm.emit([message.value])
                if self.counter == 1000000:  # mark time
                    log.warning("finish process 1000000 records at {0} (timestamp)".format(time.time()))
                    # self.counter = 0
        except Exception as inst:
            log.debug("Exception Type: %s ; Args: %s", type(inst), inst.args)


def run():
    """
    給petrel呼叫用, 為module function, 非class function
    """
    ExpSpout().run()
