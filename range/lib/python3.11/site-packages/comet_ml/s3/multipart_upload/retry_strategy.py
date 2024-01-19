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
import time
from typing import Any, Dict, Optional, Tuple

from comet_ml.config import UPLOAD_FILE_MAX_RETRIES
from comet_ml.s3.multipart_upload.upload_error import S3UploadError

from requests import RequestException, Response, Session

MAX_UPLOAD_START_ATTEMPTS = UPLOAD_FILE_MAX_RETRIES
MAX_S3_PART_UPLOAD_ATTEMPTS = UPLOAD_FILE_MAX_RETRIES
MAX_UPLOAD_COMPLETE_ATTEMPTS = UPLOAD_FILE_MAX_RETRIES


LOGGER = logging.getLogger(__name__)


class UploadRetryStrategyOp(object):
    """A strategy to perform upload operations with defined retry attempt counts
    for different stages of S3 direct upload.

    Args:
        max_upload_start_attempts: number of attempts for upload-start call.
        max_upload_complete_attempts: number of attempts for S3 AWS part upload
        max_s3_file_part_upload_attempts: number of attempts for S3 AWS part upload
    """

    def __init__(
        self,
        max_upload_start_attempts: int,
        max_upload_complete_attempts: int,
        max_s3_file_part_upload_attempts: int,
    ):
        self.max_upload_start_attempts = max_upload_start_attempts
        self.max_upload_complete_attempts = max_upload_complete_attempts
        self.max_s3_file_part_upload_attempts = max_s3_file_part_upload_attempts

    @classmethod
    def default_upload_retry_strategy(cls):
        return UploadRetryStrategyOp(
            max_upload_start_attempts=MAX_UPLOAD_START_ATTEMPTS,
            max_upload_complete_attempts=MAX_UPLOAD_COMPLETE_ATTEMPTS,
            max_s3_file_part_upload_attempts=MAX_S3_PART_UPLOAD_ATTEMPTS,
        )

    def start_multipart_upload(
        self,
        session: Session,
        url: str,
        payload: Dict[str, Any],
        headers: Dict[str, Any],
    ) -> Tuple[Response, int]:
        response, attempts, failed = _request_with_retries(
            session=session,
            method="POST",
            url=url,
            json_payload=payload,
            data=None,
            headers=headers,
            max_retries=self.max_upload_start_attempts,
        )
        _raise_for_status_or_failure(
            response=response, failed=failed, operation="start S3 direct upload"
        )

        return response, attempts

    def complete_multipart_upload(
        self,
        session: Session,
        url: str,
        payload: Dict[str, Any],
        headers: Dict[str, Any],
    ) -> Tuple[Response, int]:
        response, attempts, failed = _request_with_retries(
            session=session,
            method="POST",
            url=url,
            json_payload=payload,
            data=None,
            headers=headers,
            max_retries=self.max_upload_complete_attempts,
        )
        _raise_for_status_or_failure(
            response=response, failed=failed, operation="complete S3 direct upload"
        )

        return response, attempts

    def upload_s3_file_part(
        self,
        session: Session,
        url: str,
        file_data: bytes,
    ) -> Tuple[Response, int, bool]:
        response, attempts, failed = _request_with_retries(
            session=session,
            method="PUT",
            url=url,
            json_payload=None,
            data=file_data,
            headers=None,
            max_retries=self.max_s3_file_part_upload_attempts,
        )
        return response, attempts, failed


def _raise_for_status_or_failure(response: Response, failed: bool, operation: str):
    if failed and response is not None:
        LOGGER.debug(
            "Bad response for %s, status: %d, text: %r",
            operation,
            response.status_code,
            response.text,
        )
        response.raise_for_status()

    if failed:
        if response is not None:
            LOGGER.warning(
                "Bad response received when trying to %s, code: %d, text: %r",
                operation,
                response.status_code,
                response.text,
            )
        raise S3UploadError(
            "Failed to %s due to recurrent connection error with Comet backend"
            % operation
        )


def _request_with_retries(
    session: Session,
    method: str,
    url: str,
    json_payload: Optional[Dict[str, Any]],
    data: Optional[bytes],
    headers: Optional[Dict[str, Any]],
    max_retries: int,
    retry_on_bad_response: bool = False,
) -> Tuple[Response, int, bool]:
    attempts = 0
    response = None
    failed = False
    while attempts < max_retries:
        try:
            response = session.request(
                method=method,
                url=url,
                json=json_payload,
                data=data,
                headers=headers,
            )
            failed = False
        except (ConnectionError, RequestException):
            failed = True
            LOGGER.debug(
                "ConnectionError when do %r, URL %r. Attempt: %d of %d.",
                method,
                url,
                attempts,
                max_retries,
                exc_info=True,
            )

        attempts += 1

        if response is None:
            failed = True
        elif response.status_code != 200:
            failed = True
            if not retry_on_bad_response:
                return response, attempts, failed

        if not failed:
            return response, attempts, failed

        if failed and attempts < max_retries:
            LOGGER.debug(
                "Failed to do %r, URL %r. Attempt: %d of %d. Retrying...",
                method,
                url,
                attempts,
                max_retries,
            )
            time.sleep(0.5)

    return response, attempts, failed
