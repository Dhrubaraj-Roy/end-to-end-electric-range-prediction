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
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from .helpers import validate_parameter_type
from .parameter import (
    create_bool_parameter,
    create_dict_parameter,
    create_float_parameter,
    create_int_parameter,
    create_list_parameter,
    create_numeric_parameter,
    create_str_parameter,
)
from .result import ValidationResult


class Validator(ABC):
    @abstractmethod
    def validate(self) -> ValidationResult:
        pass


class MethodParametersTypeValidator(Validator):
    def __init__(self, method_name: str, class_name: Optional[str] = None):
        if class_name is not None:
            self.prefix = "%s.%s" % (class_name, method_name)
        else:
            self.prefix = method_name
        self.parameters = []
        self.validation_result = None  # type: Optional[ValidationResult]

    def add_str_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_str_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def add_bool_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_bool_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def add_int_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_int_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def add_float_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_float_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def add_numeric_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_numeric_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def add_list_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_list_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def add_dict_parameter(self, value: Any, name: str, allow_empty: bool = True):
        self.parameters.append(
            create_dict_parameter(name=name, value=value, allow_empty=allow_empty)
        )

    def validate(self) -> ValidationResult:
        failures = []
        try:
            for parameter in self.parameters:
                valid, msg = validate_parameter_type(parameter)
                if not valid:
                    failures.append(msg)

            if len(failures) > 0:
                self.validation_result = ValidationResult(failure_reasons=failures)
            else:
                self.validation_result = ValidationResult(failed=False)
        except Exception as e:
            self.validation_result = ValidationResult(
                failure_reasons=["Unexpected validation error: %r" % e]
            )

        return self.validation_result

    def print_result(self, logger: logging.Logger, log_level: int = logging.ERROR):
        if self.validation_result is None:
            logger.log(
                level=log_level, msg="No validation result, please call validate first"
            )
            return

        for msg in self.validation_result.get_failure_reasons():
            logger.log(log_level, "%s: %s", self.prefix, msg)
