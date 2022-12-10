# -*- coding: utf-8 -*-
# @Time         : 11:56 2022/1/28
# @Author       : Chris
# @Description  :
import ssl
from logging import Handler, LogRecord

import pika


class RabbitHandler(Handler):
    """
    RoutingKey adopts logger's name.
    """
    def __init__(self, amqps_url: str, exchange: str = "logs"):
        super(RabbitHandler, self).__init__()
        self.exchange = exchange
        self.amqps_url = amqps_url
        self._connection = None
        self._channel = None
        try:
            self._connect()
        except Exception as e:
            print(f"Failed connecting to '{self.amqps_url}'! Info: {e}")

    def emit(self, record: LogRecord) -> None:
        if self._channel is None:
            return
        msg = self.format(record)
        self._channel.basic_publish("logs", record.name, bytes(msg, "utf-8"))

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self._channel = None

    def _connect(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.verify_mode = ssl.CERT_NONE
        parameters = pika.URLParameters(self.amqps_url)
        parameters.ssl_options = pika.SSLOptions(context)
        # Create a new instance of the Connection object
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(self.exchange, "topic")
