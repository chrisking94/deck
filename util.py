# -*- coding: utf-8 -*-
# @Time         : 14:12 2022/12/10
# @Author       : Chris
# @Description  :
import datetime
import re


__FORMAT_ISO_JSON_DATE = "%Y-%m-%dT%H:%M:%S"
__REGEX_ISO_JSON_DATE = re.compile(r"^[1-2]\d{3}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d$")


def json_default_serialize(o):
    if isinstance(o, datetime.datetime):
        return o.strftime(__FORMAT_ISO_JSON_DATE)


def json_default_deserialize(pairs):
    """Load with dates"""
    d = {}
    for k, v in pairs:
        if isinstance(v, str):
            if __REGEX_ISO_JSON_DATE.match(v):
                d[k] = datetime.datetime.strptime(v, __FORMAT_ISO_JSON_DATE)
            else:
                d[k] = v
        else:
            d[k] = v
    return d
