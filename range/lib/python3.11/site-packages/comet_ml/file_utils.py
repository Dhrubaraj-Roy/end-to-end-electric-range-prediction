# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at https://www.comet.com
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************


import hashlib

from . import exceptions, file_uploader, logging_messages, utils
from ._typing import IO, Any, Optional, ValidFilePath


def io_sha1sum(io_object):
    # type: (IO[bytes]) -> str
    sha1sum = hashlib.sha1()

    block = io_object.read(2**16)
    while len(block) != 0:
        sha1sum.update(block)
        block = io_object.read(2**16)

    return sha1sum.hexdigest()


def file_sha1sum(file_path):
    # type: (str) -> str
    with open(file_path, "rb") as source:
        return io_sha1sum(source)


def get_extension(format: Optional[str], user_input: Any, file_name: Optional[str]):
    if format is not None:
        return format
    elif isinstance(user_input, ValidFilePath) or file_uploader.is_valid_file_path(
        user_input
    ):
        return utils.get_file_extension(user_input)
    elif hasattr(user_input, "read"):
        return utils.get_file_extension(file_name)
    return None
