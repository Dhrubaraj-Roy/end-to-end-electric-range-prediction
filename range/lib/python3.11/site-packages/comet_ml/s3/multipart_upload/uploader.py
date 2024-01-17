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
import pathlib
from typing import IO, Callable, Optional, Tuple

from comet_ml.file_upload_size_monitor import UploadSizeMonitor
from comet_ml.s3.multipart_upload.base_helper import (
    MultipartUploadMetadata,
    S3MultipartBaseHelper,
)
from comet_ml.s3.multipart_upload.file_parts_strategy import (
    BaseFilePartsStrategy,
    FileLikePartsStrategy,
    FilePartsStrategy,
)
from comet_ml.s3.multipart_upload.upload_error import S3UploadFileError

from requests import Response, Session

LOGGER = logging.getLogger(__name__)


class PartMetadata(object):
    __slots__ = ["e_tag", "part_number", "size"]

    def __init__(self, e_tag: str, part_number: int, size: int):
        self.e_tag = e_tag
        self.part_number = part_number
        self.size = size


class S3MultipartUploader(object):
    def __init__(
        self,
        file_parts_strategy: BaseFilePartsStrategy,
        s3_helper: S3MultipartBaseHelper,
    ):

        self.s3_helper = s3_helper
        self.file_parts_strategy = file_parts_strategy

        self.bytes_read = 0
        self.upload_parts = []
        self.upload_monitor = None

    def upload(
        self, session: Session, monitor: Optional[UploadSizeMonitor] = None
    ) -> Tuple[int, Response]:
        parts_number = self.file_parts_strategy.calculate()
        multipart_info = self.s3_helper.start_multipart_upload(
            session=session, parts_number=parts_number
        )
        self.upload_monitor = monitor
        return self._do_upload(session=session, multipart_info=multipart_info)

    def _do_upload(
        self, session: Session, multipart_info: MultipartUploadMetadata
    ) -> Tuple[int, Response]:
        try:
            if isinstance(self.file_parts_strategy, FilePartsStrategy):
                file_to_upload = pathlib.Path(self.file_parts_strategy.file)
                with file_to_upload.open("rb") as fp:
                    self._upload_fp(
                        fp=fp, session=session, multipart_info=multipart_info
                    )
            elif isinstance(self.file_parts_strategy, FileLikePartsStrategy):
                # rewind
                self.file_parts_strategy.file_like.seek(0)
                self._upload_fp(
                    fp=self.file_parts_strategy.file_like,
                    session=session,
                    multipart_info=multipart_info,
                )
        except Exception as ex:
            LOGGER.error(
                "Failed to upload file parts to S3, Comet request ID: %r",
                multipart_info.request_id,
                exc_info=True,
            )
            # complete upload with error status
            self.s3_helper.complete_multipart_upload(
                session=session,
                upload_metadata=multipart_info,
                parts=[],
                succeed=False,
                file_size=-1,
            )
            raise ex

        # complete upload with collected parts and success status
        response = self.s3_helper.complete_multipart_upload(
            session=session,
            upload_metadata=multipart_info,
            parts=[
                {"ETag": part.e_tag, "PartNumber": part.part_number}
                for part in self.upload_parts
            ],
            succeed=True,
            file_size=self.bytes_read,
        )
        return self.bytes_read, response

    def _on_part_complete(self, part: PartMetadata) -> None:
        self.upload_parts.append(part)
        self.bytes_read += part.size
        if self.upload_monitor is not None:
            self.upload_monitor.monitor_callback(self)

    def _upload_fp(
        self, fp: IO, session: Session, multipart_info: MultipartUploadMetadata
    ):
        max_file_part_size = self.file_parts_strategy.max_file_part_size
        for i, url in enumerate(multipart_info.parts_urls):
            part_number = i + 1
            file_data = fp.read(max_file_part_size)
            self._send_data_part(
                session=session,
                url=url,
                file_data=file_data,
                part_number=part_number,
                on_part_complete=self._on_part_complete,
            )

    def _send_data_part(
        self,
        session: Session,
        url: str,
        file_data: bytes,
        part_number: int,
        on_part_complete: Callable[[PartMetadata], None],
    ) -> None:
        (
            response,
            attempts,
            failed,
        ) = self.s3_helper.upload_retry_strategy_op.upload_s3_file_part(
            session=session,
            url=url,
            file_data=file_data,
        )

        if not failed and response.status_code == 200:
            e_tag = response.headers["ETag"]
            on_part_complete(
                PartMetadata(e_tag=e_tag, part_number=part_number, size=len(file_data))
            )
            return

        if response is not None:
            message = (
                "S3 file part #%d upload failed in %d attempt(s), got response - status: %d, text: %r"
                % (part_number, attempts, response.status_code, response.text)
            )
        else:
            message = "S3 file part #%d upload failed in %d attempt(s)" % (
                part_number,
                attempts,
            )

        LOGGER.debug(message)
        raise S3UploadFileError(
            self.file_parts_strategy.file,
            message,
        )
