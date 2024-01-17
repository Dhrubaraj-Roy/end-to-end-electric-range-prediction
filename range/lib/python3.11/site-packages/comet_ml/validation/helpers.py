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
from typing import Any, List, Optional, Tuple, Union

from .parameter import Parameter


def validate_type_str(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (str,), allow_empty=allow_empty)


def validate_type_int(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (int,), allow_empty=allow_empty)


def validate_type_float(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (float,), allow_empty=allow_empty)


def validate_type_numeric(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (float, int), allow_empty=allow_empty)


def validate_type_bool(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (bool,), allow_empty=allow_empty)


def validate_type_list(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (list,), allow_empty=allow_empty)


def validate_type_dict(value: Any, allow_empty: bool = True) -> bool:
    return validate_type(value, (dict,), allow_empty=allow_empty)


def validate_type(value: Any, types: Tuple, allow_empty: bool) -> bool:
    if value is None:
        return allow_empty

    return any(isinstance(value, t) for t in types)


def validate_parameter_type(parameter: Parameter) -> (bool, Optional[str]):
    valid = validate_type(
        value=parameter.value, types=parameter.types, allow_empty=parameter.allow_empty
    )
    param_type = None if parameter.value is None else type(parameter.value).__name__
    if not valid:
        if parameter.allow_empty:
            msg = "parameter %r must be of type(s) %r or None but %r was given" % (
                parameter.name,
                types_list(parameter.types),
                param_type,
            )
        else:
            msg = "parameter %r must be of type(s) %r but %r was given" % (
                parameter.name,
                types_list(parameter.types),
                param_type,
            )
        return False, msg
    return True, None


def types_list(types: Union[Tuple, List]) -> Union[str, List[str]]:
    type_names = []
    for t in types:
        type_names.append(t.__name__)

    if len(type_names) > 1:
        return type_names
    elif len(type_names) == 1:
        return type_names[0]
    else:
        return []
