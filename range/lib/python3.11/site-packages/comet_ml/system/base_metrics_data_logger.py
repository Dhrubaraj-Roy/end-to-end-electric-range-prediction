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
#  This file can not be copied and/or distributed without
#  the express permission of Comet ML Inc.
# *******************************************************
import abc
import logging
import time
from typing import Any

from comet_ml.system.system_metrics_types import (
    CompatibleSystemMetrics,
    SystemMetricsCallable,
)

LOGGER = logging.getLogger(__name__)


class BaseMetricsDataLogger(abc.ABC):
    """The base class for all system metrics data loggers. It implements common scheduling and error handling."""

    def __init__(
        self,
        initial_interval: float,
        callback: SystemMetricsCallable,
        max_failed_attempts: int = 10,
        minimal_interval: float = 10,
    ):
        self.interval = _valid_interval(initial_interval, minimal_interval)
        self.callback = callback
        self.max_failed_attempts = max_failed_attempts

        self.last_log_attempt = 0.0
        self.failures_number = 0

        LOGGER.debug(
            "Metrics data logger created: %s with interval: %.1f",
            self.get_name(),
            initial_interval,
        )

    def log_metric_data(self):
        try:
            metrics = self.get_metrics()
            if _not_empty(metrics) > 0:
                self.callback(metrics)
        except Exception:
            LOGGER.warning(
                "Failed to log system metrics: %s", self.get_name(), exc_info=True
            )
            self.failures_number += 1

        self.last_log_attempt = time.time()

    def should_log_data(self) -> bool:
        if self.failed() or not self.available():
            return False

        # check interval
        next_run = self.last_log_attempt + self.interval  # seconds
        now = time.time()
        result = next_run <= now
        return result

    def failed(self) -> bool:
        return self.failures_number >= self.max_failed_attempts

    @abc.abstractmethod
    def get_metrics(self) -> CompatibleSystemMetrics:
        pass

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def available(self) -> bool:
        pass


def _valid_interval(interval: float, minimal_interval: float) -> float:
    if interval >= minimal_interval:
        return interval

    LOGGER.warning(
        "Provided interval is too low, falling back to the minimum interval (%.1f Seconds)",
        minimal_interval,
    )
    return minimal_interval


def _not_empty(metrics: Any):
    if metrics is None:
        return False

    if hasattr(metrics, "__len__") or hasattr(metrics, "len"):
        return len(metrics) > 0

    return True
