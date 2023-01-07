# -*- coding: utf-8 -*-
# @Time         : 14:30 2022/1/27
# @Author       : Chris
# @Description  : Entity to entity deck based on RabbitMQ.
import _thread
import datetime
import json
import logging
import socket
import ssl
import threading
import time
import uuid
import weakref
from enum import IntFlag, IntEnum
from queue import Queue
from typing import Callable, Dict, Any, Tuple

import pandas as pd
import pika
from pika import spec, exceptions
from pika.adapters.blocking_connection import BlockingChannel

from .util import json_default_serialize, json_default_deserialize

logger = logging.getLogger("e2e")


class Address:
    def __init__(self, center: str, user: str):
        self.center = center
        self.user = user
        self._str_address = f"{self.user}@{self.center}"

    @staticmethod
    def parse(str_addr: str):
        """
        Parse a literal address.
        :param str_addr: user@center.
        :rtype: Address
        """
        chips = str_addr.split("@")
        if len(chips) != 2:
            raise RuntimeError(f"Invalid address '{str_addr}'.")
        return Address(chips[1], chips[0])

    def __str__(self):
        return self._str_address


class Method(IntEnum):
    POST = 1
    GET = 2


class MessageType(IntFlag):
    NORMAL = 0x00
    ERROR = 0x01
    REPLY = 0x02  # Reply of a request.
    SYSTEM = 0x04  # System message.
    PROBE = 0x08  # Probe messages will suppress most types of errors in deck procedure.


class Message:
    def __init__(self, sender: str, recipient: str, corr_id, method: Method, body,
                 headers: Dict[str, str] = None,
                 msg_type=MessageType.NORMAL):
        self.sender = sender
        self.recipient = recipient
        self.headers: Dict[str, str] = {} if headers is None else headers
        self.body = body
        self.method = method
        self.type = msg_type
        self.time_stamp: datetime.datetime = None  # The time that message is sent.
        self._corr_id = corr_id

    @property
    def corr_id(self):
        return self._corr_id

    def create_reply(self, body, headers: dict, msg_type=MessageType.NORMAL):
        """
        Create a reply message for 'msg'.
        Attention: If this message is of type MessageType.PROBE, the reply will inherit probe flag.
        :rtype: Message
        """
        msg_type |= MessageType.REPLY
        if self.type & MessageType.PROBE:
            msg_type |= MessageType.PROBE
        return Message(self.recipient, self.sender, self.corr_id, Method.POST, body, headers, msg_type)

    def __str__(self):
        return f"{self.sender} => {self.recipient}"


class Result:
    """Result returned by incoming-message default_handler."""
    LATER_ON = None
    """Used by default_handler to notify message box that the desired result will be returned somewhere else later on."""

    def __init__(self, body, headers: Dict[str, str] = None, type_=MessageType.NORMAL, probe_suppress=False):
        self.body = body
        self.headers = headers if headers else {}
        self.type = type_
        self.probe_suppress = probe_suppress

    @staticmethod
    def parse(obj):
        if isinstance(obj, Result):  # Result object.
            return obj
        elif isinstance(obj, Tuple) and len(obj) == 2 and isinstance(obj[1], Dict):  # Body + Headers
            return Result(obj[0], obj[1])
        elif isinstance(obj, Dict):  # Headers.
            return Result(None, obj[1])
        else:  # Body
            return Result(obj[0], {})


# Notify the on_receive_msg method that program will return result later on at other place.
Result.LATER_ON = Result(None, {})


class HandlerState(IntEnum):
    OFFLINE = 0
    IDLE = 1  # Online and idle.
    BUSY = 2  # Online and busy.


class _MessageHandler:
    """1 Thread per default_handler."""
    def __init__(self, name: str, predicate: Callable[[Message], bool], handler: Callable[[Message], Result],
                 priority: int, box):
        self.name = name
        self.predicate = predicate
        self.handler = handler
        self.priority = priority
        self.box: MessageBox = weakref.proxy(box)
        self._queue: Queue[Tuple[Message, int]] = Queue()  # Queue[(msg, delivery_tag)]
        self.__state = HandlerState.IDLE
        self.__closed = False
        _thread.start_new_thread(self.__handle_message, ())

    @property
    def state(self) -> HandlerState:
        return self.__state

    def accept(self, msg: Message, delivery_tag: int):
        """Put message to queue"""
        self._queue.put((msg, delivery_tag))

    def close(self):
        if self.__closed:
            return
        self.__closed = True
        while self.__closed:  # Wait for executor thread to revert 'closed' flag.
            time.sleep(0.001)
        self.__closed = True

    def __handle_message(self):
        """Use 'self.default_handler' to handle message."""
        while not self.__closed:
            msg, delivery_tag = self._queue.get(True)
            try:
                self.__state = HandlerState.BUSY
                res: Result = self.handler(msg)
            except BaseException as e:
                logger.error(f"{self.name}: Failed handling message '{msg}':", exc_info=e, stack_info=True)
                res = Result(str(e), None, MessageType.ERROR)
            finally:
                self.__state = HandlerState.IDLE
            if msg.method == Method.GET:
                if res is None:
                    e = RuntimeError("The default_handler must return a result for 'GET' request message.")
                    logger.error("Handler Invalid", exc_info=e, stack_info=True)
                    raise e
                if not (res.probe_suppress and (msg.type & MessageType.PROBE)):
                    self.box.reply(msg, res)
            # If program is cancelled or crashed half way, there won't be an ack.
            # Then the msg will be requeued by rabbit.
            self.box.ack(delivery_tag)
        self.__closed = False  # Notify 'self.close()' method.


class MessageBox:
    """
    Thread safe message box. MessageBox will use a separete thread to execute default_handler.
    """
    DEFAULT_TIMEOUT = 60

    def __init__(self, address: str, center):
        """
        :param address: Address of new created message box.
        :type center: MessageCenter
        The default_handler will be invoked in separate thread.
        """
        self.address: str = address
        self._corr2msg_reply = {}
        self._center: MessageCenter = weakref.proxy(center)
        self._channel: BlockingChannel = None  # Don't use channel outside method 'process_message'.
        # Message transmission _queue. (routing_key, properties, body)
        self._msg_tq: Queue[Tuple[str, pika.BasicProperties, Any]] = Queue()
        # Message receiving _queue. (method, properties, body)
        self._msg_rq: Queue[Tuple[spec.Basic.GetOk, pika.BasicProperties, Any]] = Queue()
        # Delegates to be executed by message center.
        self._delegates: Queue[Callable[[BlockingChannel], None]] = Queue()
        # Messages to be dealt by 'self.default_handler'.
        self._name2handler: Dict[str, _MessageHandler] = {}
        # Register built-in handlers.
        self.register("__reply_of_get", lambda msg: msg.type & MessageType.REPLY, self._handle_reply_of_get, 12)
        self.register("__sys", lambda msg: msg.type & MessageType.SYSTEM, self._handle_sys_request, 11)
        self.register("__unknown", lambda msg: True, self._handle_unknown_msg, 0)
        # Start receiving monitor.
        self.__closed = False
        _thread.start_new_thread(self.__receive_message, ())

    @property
    def closed(self):
        return self.__closed

    @property
    def center_name(self) -> str:
        return self._center.name

    def register(self, name: str, predicate: Callable[[Message], bool],
                 handler: Callable[[Message], Result], priority: int):
        """
        Register message default_handler.
        :param name: Name(id) of the default_handler.
        :param predicate: Use to determine whether a message can be handled by the default_handler.
        :param handler: The actual message handling method.
        :param priority: Priority[1~10] of the default_handler. The greater priority number is, the more priorly default_handler is invoked.
        :return:
        """
        if name in self._name2handler:
            raise Exception(f"There is already a default_handler of name '{name}' registered!")
        self._name2handler[name] = _MessageHandler(name, predicate, handler, priority, self)

    def post(self, recipient: str, body, headers: Dict[str, str] = None):
        msg = Message(self.address, recipient, str(uuid.uuid4()), Method.POST, body, headers)
        self.__send_message(msg)

    def get(self, recipient: str, body, headers: Dict[str, str] = None, timeout=DEFAULT_TIMEOUT) -> Message:
        """
        Send a message to remote recipient using 'GET' method.
        :param recipient:
        :param body:
        :param headers:
        :param timeout: Wait timeout in 'seconds'. Setting to '0' means never timeout.
        :return:
        """
        msg = Message(self.address, recipient, str(uuid.uuid4()), Method.GET, body, headers)
        return self._get(msg, timeout)

    def reply(self, received_msg: Message, result: Result):  # Reply 'OK' as default.
        if result is Result.LATER_ON:
            pass
        else:
            reply = received_msg.create_reply(result.body, result.headers, result.type)
            if received_msg.method == Method.POST:
                reply.type &= ~MessageType.REPLY  # Remove 'REPLY' flag for 'POST' type request.
            self.__send_message(reply)  # Auto Reply.

    def query(self, remote_box: str, timeout: int = 2, handler_name="default") -> HandlerState:
        """
        Query state of a remote box
        :param handler_name:
        :param timeout: Unit in second.
        :param remote_box:
        :return:
        """
        try:
            msg = Message(self.address, remote_box, str(uuid.uuid4()), Method.GET,
                          handler_name, {"method": "query_state"}, MessageType.SYSTEM | MessageType.PROBE)
            res = self._get(msg, timeout)
            if res.type & MessageType.ERROR:
                raise RuntimeError(res.body)
            return HandlerState(res.body)
        except TimeoutError:
            return HandlerState.OFFLINE

    def ack(self, delivery_tag: int):
        """Ack a rabbit message of given delivery tag."""
        self._delegates.put(lambda channel: channel.basic_ack(delivery_tag))

    def close(self):
        for handler in self._name2handler.values():
            handler.close()
        if self.__closed:
            return
        self.__closed = True
        while self.__closed:  # Wait for monitor thread to quit.
            time.sleep(0.001)
        self.__closed = True

    def _get(self, msg: Message, timeout: int) -> Message:  # timeout in 'seconds'.
        assert msg.method == Method.GET
        self._corr2msg_reply[msg.corr_id] = None  # Wait for reply.
        self.__send_message(msg)
        reply = None
        t_cnt = 0.0
        while reply is None:
            reply = self._corr2msg_reply.get(msg.corr_id)
            time.sleep(0.01)
            if timeout != 0:
                t_cnt += 0.01
                if t_cnt >= timeout:
                    raise TimeoutError(f"GET '{msg}' timeout!")
        del self._corr2msg_reply[msg.corr_id]
        return reply

    def _handle_reply_of_get(self, msg: Message) -> Result:
        """Handle reply of 'GET' message."""
        if msg.corr_id in self._corr2msg_reply:
            self._corr2msg_reply[msg.corr_id] = msg
        else:  # There isn't any waiting handle for the reply.
            return Result(f"There isn't any waiting handle of correlation ID '{msg.corr_id}'",
                          type_=MessageType.ERROR, probe_suppress=True)

    def _handle_sys_request(self, msg: Message) -> Result:
        """Handle message of type 'SYSTEM'"""
        method = msg.headers.get("method")
        if method == "query_state":  # Query status of default_handler.
            handler = self._name2handler.get(msg.body)
            if handler is None:
                return Result(f"There's no such default_handler named '{msg.body}'", None, MessageType.ERROR)
            return Result(handler.state.value, None, MessageType.SYSTEM)
        else:
            return Result(f"System operation '{method}' of python lib is not implemented!", None, MessageType.ERROR)

    def _handle_unknown_msg(self, msg: Message) -> Result:
        """Handle messages don't have any meaningful related default_handler."""
        return Result(f"There isn't any default_handler defined for message '{msg}'.", None, MessageType.ERROR)

    def __send_message(self, msg: Message):
        msg.time_stamp = datetime.datetime.now()
        properties = pika.BasicProperties(
            # Persist non-probe 'GET' message only.
            delivery_mode=2 if (msg.method == Method.GET and not (msg.type & MessageType.PROBE)) else 1,
            headers={
                "sender": msg.sender,
                "recipient": msg.recipient,
                "method": msg.method.name,
                "type": msg.type.value,
                "corr_id": msg.corr_id,
                "time_stamp": msg.time_stamp.strftime("%Y-%m-%dT%H:%M:%S")
            },
        )
        headers: bytes = json.dumps(msg.headers, ensure_ascii=False).encode('utf-8')
        body: bytes = msg.body
        if isinstance(msg.body, bytes):
            properties.content_type = "bytes"
        elif isinstance(msg.body, str):
            properties.content_type = "plain"
            body = msg.body.encode('utf-8')
        elif msg.body is None:
            properties.content_type = "none"
            body = bytearray()
        elif isinstance(msg.body, pd.DataFrame):  # DataFrame.
            properties.content_type = "dataframe"
            body = msg.body.to_json(orient="split", date_format='iso', force_ascii=False).encode('utf-8')
        else:  # Any object.
            properties.content_type = "json"
            body = json.dumps(msg.body, ensure_ascii=False, default=json_default_serialize).encode('utf-8')
        hl, bl = len(headers), len(body)
        if hl > 2147483647:
            raise OverflowError(f"Header exceeds max length 2147483647 bytes.")
        payload = bytearray(hl+bl+4)
        payload[:4] = hl.to_bytes(4, 'little', signed=True)
        payload[4:hl+4] = headers
        payload[hl+4:] = body
        self._msg_tq.put((msg.recipient, properties, payload))

    def __receive_message(self):
        """Receive and route."""
        while not self.__closed:
            method = None
            try:
                time.sleep(0.01)
                if self._msg_rq.empty():
                    continue
                method, props, payload = self._msg_rq.get_nowait()
                hl = int.from_bytes(payload[0:4], 'little', signed=True)
                h_bytes: bytes = payload[4:hl + 4]
                b_bytes: bytes = payload[hl + 4:]
                # 1. Parse result pack.
                if props.content_type == "plain":  # Plain text.
                    result_pack = b_bytes.decode('utf-8')
                elif props.content_type == "json":  # Json object.
                    result_pack = json.loads(b_bytes, object_pairs_hook=json_default_deserialize)
                elif props.content_type == "none":  # None.
                    result_pack = None
                elif props.content_type == "dataframe":  # Dataframe.
                    result_pack = pd.read_json(b_bytes, orient="split", convert_dates=False, keep_default_dates=False,
                                               convert_axes=False)
                else:  # Bytes.
                    result_pack = b_bytes
                headers = json.loads(h_bytes.decode('utf-8'))
                meta = props.headers
                msg = Message(meta["sender"], meta["recipient"], meta["corr_id"], Method[meta["method"]],
                              result_pack, headers, MessageType(meta["type"]))
                msg.time_stamp = datetime.datetime.strptime(meta["time_stamp"], "%Y-%m-%dT%H:%M:%S")
                # 2. Route messages into related handlers.
                for handler in sorted(self._name2handler.values(), key=lambda h: h.priority, reverse=True):
                    if handler.predicate(msg):
                        handler.accept(msg, method.delivery_tag)
                        break  # Process message only once.
            except Exception as e:
                logger.error("Error receiving message.", exc_info=e, stack_info=True)  # Log and pass.
                if method is not None:
                    self.ack(method.delivery_tag)  # Close(Discard) the incomprehensible message.
        self.__closed = False  # Notify 'self.close()'.

    def process_message(self):
        """
        Invoked by MessageCenter periodically.
        Attention:
        1. Do not invoke this method in other places.
        2. Keep this method a lightweight method.
        :return:
        """
        # Receive. Pull incoming message and push it to receiving _queue.
        if self._msg_rq.empty():
            method, properties, body = self._channel.basic_get(self.address, False)
            if method is not None:
                self._msg_rq.put((method, properties, body))
        # Transmit. Publish message fetched from transmission _queue.
        if not self._msg_tq.empty():
            recipient, properties, payload = self._msg_tq.get_nowait()
            try:
                self._channel.basic_publish(
                    exchange='e2e',
                    routing_key=recipient,
                    properties=properties,
                    body=payload,
                    mandatory=True)
            except:
                # Put message back to tq when publishing failed.
                self._msg_tq.put_nowait((recipient, properties, payload))
                raise
        # Execute delegates.
        while not self._delegates.empty():
            delegate = self._delegates.get_nowait()
            delegate(self._channel)

    def reset_network(self):
        """Invoked by MessageCenter."""
        self._channel = None

    def ensure_network(self, connection: pika.BaseConnection):
        """Invoked by MessageCenter."""
        if self._channel is None:
            channel = connection.channel()
            channel.exchange_declare("e2e", "direct", durable=True)
            channel.queue_declare(queue=self.address, exclusive=False, durable=True)
            channel.queue_bind(self.address, "e2e")
            channel.basic_qos(prefetch_count=0)  # Turn off QoS.
            self._channel = channel


class PumperThread(threading.Thread):
    def __init__(self, center, dogfood_event: threading.Event):
        super().__init__()
        self.center: MessageCenter = center
        self.stop_event = threading.Event()
        self.dogfood_event = dogfood_event
        self._connection: pika.BlockingConnection = None
        self.name = f"BumperThread({center.name})"

    def stop(self, timeout: float):
        self.stop_event.set()
        self.join(timeout)

    def run(self) -> None:
        while True:
            # 1 Check connection.
            if self._connection is None or not self._connection.is_open:
                self._connect()  # Reconnect if connection is lost.
            # 2 Let registered boxes process messages.
            for box in self.center.get_boxes():
                try:
                    box.ensure_network(self._connection)
                    box.process_message()
                except pika.exceptions.AMQPConnectionError:
                    self._connect()  # Reconnect without logging.
                except pika.exceptions.AMQPError as e:
                    logger.info(f"AMQP Error: {e}. performing auto recovery...", exc_info=True)
                    self._connect()
                except BaseException as e:
                    logger.error(f"MessageCenter crashed unexpectedly!", exc_info=e, stack_info=True)
                    self._connect()
                finally:
                    if self.stop_event.is_set():
                        break
            if self.stop_event.wait(0.01):
                break
            self.dogfood_event.set()  # Feed dog.

    def _connect(self):
        center = self.center
        while True:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_NONE
            context.check_hostname = False
            parameters = pika.URLParameters(center.amqps_server)
            parameters.ssl_options = pika.SSLOptions(context)
            parameters.heartbeat = 60
            # Try connecting and recovering boxes.
            try:
                if self._connection is not None and self._connection.is_open:
                    self._connection.close()  # Close last open connection.
                self._connection = pika.BlockingConnection(parameters)
                if self._connection is not None and self._connection.is_open:
                    # Reset boxes.
                    for box in center.get_boxes():
                        box.reset_network()
                    logger.info(
                        f"Center '{center.name}"
                        f"{'' if center.description is None else f'({center.description})'}' "
                        f"has connected to server!")
                    break
            except pika.exceptions.AMQPConnectionError:
                pass
            except socket.gaierror:  # socket.gaierror: [Errno 11001] getaddrinfo failed
                pass
            except ssl.SSLEOFError:  # EOF occurred in violation of protocol (_ssl.c:1125)
                pass
            except BaseException as e:
                logger.error("E2E message center reconnecting fails unexpectedly!", exc_info=e, stack_info=True)
                raise
            logger.info(f"Connecting failed, retry in {center.network_recovery_interval} sec(s).")
            if self.stop_event.wait(center.network_recovery_interval):
                break
            self.dogfood_event.set()  # Feed dog.


class MessageCenter:
    """
    Thread safe message center.
    """
    def __init__(self, amqps_server: str, name: str, description: str = None):
        self.description = description
        self.name = name
        self.amqps_server = amqps_server
        self.network_recovery_interval = 5  # Unit: second
        self._owner2box: Dict[str, MessageBox] = {}
        self.__closed = False
        # Start watchdog.
        self.__event_close_watchdog = threading.Event()
        self.__thread_watchdog = threading.Thread(None, self.__watchdog, "watchdog", (self.__event_close_watchdog,))
        self.__thread_watchdog.start()

    def get_box(self, user: str, default_handler: Callable[[Message], Result] = None) -> MessageBox:
        """
        :param user: Unique user identifier.
        :param default_handler: A function which handles incoming message and return processing result.
        :return: A message box bound with given user.
        """
        box = self._owner2box.get(user)
        if box is None:
            # 1. Create box
            address = Address(self.name, user)
            box = MessageBox(str(address), self)
            if default_handler is not None:
                box.register("default", lambda msg: True, default_handler, 1)
            # 2. Register box.
            self._owner2box[user] = box
        return box

    def get_boxes(self):
        return list(self._owner2box.values())

    def close(self):
        if self.__closed:
            return
        # 1. Close threads.
        self.__event_close_watchdog.set()
        self.__thread_watchdog.join(5)
        # 2. Close boxes.
        for box in self._owner2box.values():
            box.close()
        # 3. Update state flag.
        self.__closed = True

    def __watchdog(self, stop_event: threading.Event):
        thread_pumper: PumperThread = None
        dogfood_event = threading.Event()
        while True:
            if not dogfood_event.is_set():  # Pumper is dead.
                logger.info(f"e2e.{self.name}.watchdog: Pumper thread is dead, starting new pumper...")
                if thread_pumper is not None:  # Stop the previous pumper thread first.
                    thread_pumper.stop(5)  # Wait until the pumper is called terminates.
                # Start a new pumper.
                dogfood_event = threading.Event()
                thread_pumper = PumperThread(self, dogfood_event)
                thread_pumper.start()
                logger.info(f"e2e.{self.name}: New pumper started.")
            else:
                dogfood_event.clear()  # Eat all food.
            if stop_event.wait(60):  # Sleep 60 seconds.
                break
