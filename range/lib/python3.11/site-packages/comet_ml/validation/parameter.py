# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at https://www.comet.com
#  Copyright (C) 2015-2023 Comet ML INC
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************
from typing import Any, Tuple


class Parameter:
    __slots__ = ["name", "value", "types", "allow_empty"]

    def __init__(self, name: str, value: Any, types: Tuple, allow_empty: bool):
        self.name = name
        self.value = value
        self.types = types
        self.allow_empty = allow_empty


def create_str_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(name=name, value=value, types=(str,), allow_empty=allow_empty)


def create_int_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(name=name, value=value, types=(int,), allow_empty=allow_empty)


def create_float_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(name=name, value=value, types=(float,), allow_empty=allow_empty)


def create_numeric_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(
        name=name, value=value, types=(float, int), allow_empty=allow_empty
    )


def create_bool_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(name=name, value=value, types=(bool,), allow_empty=allow_empty)


def create_list_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(name=name, value=value, types=(list,), allow_empty=allow_empty)


def create_dict_parameter(name: str, value: Any, allow_empty: bool) -> Parameter:
    return Parameter(name=name, value=value, types=(dict,), allow_empty=allow_empty)
