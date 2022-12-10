# -*- coding: utf-8 -*-
# ********* RADIATION RISK ********* #
# @Time         : 10:33 2021/12/31
# @Author       : Chris
# @Description  : ft. RabbitMQ.
import json
import uuid
from abc import ABC, abstractmethod
from typing import List

from .e2e import MessageBox, MessageType


class RpcResultItem:
    def __init__(self, body, headers: dict):
        self.body = body
        self.headers = headers


class RpcResult(ABC):
    def __init__(self):
        self._closed = False

    @abstractmethod
    def get_result(self) -> List[RpcResultItem]:
        pass

    def _close_core(self):
        pass

    def close(self):
        if self._closed:
            return
        self._close_core()
        self._closed = True

    def closed(self):
        return self._closed

    def get_single_result(self) -> RpcResultItem:
        """
        Get result produced by return.
        :return:
        """
        result = None
        for part in self.get_result():
            if result is None:
                result = part
            else:
                raise Exception("Result contains multiple parts.")
        return result


class RpcYieldResult(RpcResult):
    """
    Synchronized handle.
    """

    def __init__(self, rpc_id: str, server_addr: str, box: MessageBox, timeout: int):
        super(RpcYieldResult, self).__init__()
        self._timeout = timeout
        self._box: MessageBox = box
        self._rpc_id = rpc_id
        self._server_addr = server_addr

    def get_result(self) -> List[RpcResultItem]:
        """
        Get result produced by yield.
        :return:
        """
        while True:
            res = self._box.get(self._server_addr, None, {"rpc_id": self._rpc_id, "pending": "", "next": ""})
            if res.type & MessageType.ERROR:
                raise RuntimeError(f"Rpc server error: {res.body}")
            if "close" in res.headers:  # Server notify close.
                self._closed = True
                break
            yield RpcResultItem(res.body, res.headers)

    def _close_core(self):
        self._box.get(self._server_addr, None, {"rpc_id": self._rpc_id, "pending": "", "close": ""})


class RpcSingleResult(RpcResult):
    def __init__(self, item: RpcResultItem):
        super(RpcSingleResult, self).__init__()
        self._item = item

    def get_result(self) -> List[RpcResultItem]:
        yield self._item


class RpcClient:
    DEFAULT_TIMEOUT = 60  # Unit: s

    def __init__(self, box: MessageBox, server_addr: str):
        """
        Create a RpcClient instance with unique session id and connect instance to sever.
        """
        self._result_handle: RpcResult = None
        self._box = box
        self.server_addr = server_addr

    def call(self, method: str, payload=None, timeout: int = DEFAULT_TIMEOUT, **kwargs) -> RpcResult:
        rpc_id = str(uuid.uuid4())
        headers = {"rpc_id": rpc_id, "ttl": str(timeout), "method": method, "args": json.dumps(kwargs, ensure_ascii=False)}
        res = self._box.get(self.server_addr, payload, headers)
        if res.type & MessageType.ERROR:
            raise RuntimeError(f"Server error: {res.body}")
        if "close" in res.headers:
            new_handle = RpcSingleResult(RpcResultItem(res.body, res.headers))
        else:
            new_handle = RpcYieldResult(rpc_id, self.server_addr, self._box, timeout)
        if self._result_handle:
            if not self._result_handle.closed:
                raise Exception("Last result default_handler hasn't been closed yet!")
        self._result_handle = new_handle
        return self._result_handle

    def close(self):
        if self._result_handle is not None and not self._result_handle.closed:
            self._result_handle.close()
