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
from typing import List, Optional


class ValidationResult:
    def __init__(
        self, failed: bool = True, failure_reasons: Optional[List[str]] = None
    ):
        self._failed = failed
        self._failure_reasons = failure_reasons

    def failed(self):
        return self._failed

    def ok(self):
        return not self._failed

    def get_failure_reasons(self) -> List[str]:
        return self._failure_reasons

    def __bool__(self):
        return not self._failed
