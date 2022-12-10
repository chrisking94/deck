# -*- coding: utf-8 -*-
# ********* RADIATION RISK ********* #
# @Time         : 12:27 2021/12/31
# @Author       : Chris
# @Description  :
import json
import uuid
from traceback import print_exc
from .rpc import RpcClient, RpcResult, RpcResultItem
from .e2e import MessageBox


class CliClient:
    def __init__(self, box: MessageBox, server_addr: str):
        self._client: RpcClient = RpcClient(box, server_addr)
        self._session_id = str(uuid.uuid4())

    def execute(self, cmd: str, payload=None, **extra_kwargs) -> RpcResult:
        """
        Execute and return response. This method won't touch the result.
        :param payload:
        :param cmd:
        :param extra_kwargs: Extra arguments.
        :return:
        """
        result = self._call(cmd, payload, **extra_kwargs)
        return result

    def run(self):
        """
        Run this client as CLI mode and block invoking thread.
        :return:
        """
        self._handshake()
        while True:  # Wait for user command.
            str_cmd = input("")
            result = self.execute(str_cmd)
            self._process_result(result)

    def close(self):
        """
        Close session.
        :return:
        """
        self._call("close_session")

    def _handshake(self):
        """
        Send handshake message and print the result.
        :return:
        """
        print("Handshaking...")
        hs_res = self._call("handshake")
        res_item = hs_res.get_single_result()
        print(res_item.body)
        print(res_item.headers.get("prompt"), end='')

    def _process_result(self, handle: RpcResult) -> RpcResult:
        try:
            for res_item in handle.get_result():
                res_item: RpcResultItem
                if isinstance(res_item, str):
                    print(str)
                elif isinstance(res_item, bytes):
                    print(res_item)
                else:  # Object.
                    res_type = res_item.headers.get("type")
                    if res_type is None:
                        res_type = "res_item"

                    if "error" == res_type:
                        print(res_item.body)
                        if "fix" in res_item.headers:
                            fix = res_item.headers["fix"]
                            if "handshake" == fix:
                                self._handshake()
                    else:
                        item_body = res_item.body
                        if item_body is not None and item_body != "":
                                    print(item_body)
                prompt = res_item.headers.get("prompt")
                if prompt is not None:
                    print(prompt, end='')

        except TimeoutError as e:
            print_exc()
        return handle

    def _call(self, method: str, payload=None, **kwargs):
        kwargs["$session_id"] = self._session_id
        return self._client.call(method, payload, **kwargs)

    def __enter__(self):
        return self
