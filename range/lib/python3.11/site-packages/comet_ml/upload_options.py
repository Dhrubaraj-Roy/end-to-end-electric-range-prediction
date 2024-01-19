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
from typing import IO, Any, Callable, Dict, Optional

from comet_ml.config import UPLOAD_FILE_MAX_RETRIES


class UploadOptions:
    def __init__(
        self,
        api_key: str,
        project_id: str,
        experiment_id: str,
        upload_endpoint: str,
        estimated_size: int,
        timeout: float,
        verify_tls: bool,
        additional_params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        clean: bool = True,
        on_asset_upload: Optional[Callable[[Any], None]] = None,
        on_failed_asset_upload: Optional[Callable[[Any], None]] = None,
    ):
        self.api_key = api_key
        self.project_id = project_id
        self.experiment_id = experiment_id
        self.upload_endpoint = upload_endpoint
        self.estimated_size = estimated_size
        self.timeout = timeout
        self.verify_tls = verify_tls
        self.additional_params = additional_params
        self.metadata = metadata
        self.clean = clean
        self.on_asset_upload = on_asset_upload
        self.on_failed_asset_upload = on_failed_asset_upload


class FileUploadOptions(UploadOptions):
    def __init__(
        self,
        file_path: str,
        upload_type: str,
        base_url: str,
        api_key: str,
        project_id: Optional[str],
        experiment_id: str,
        upload_endpoint: str,
        estimated_size: int,
        timeout: float,
        verify_tls: bool,
        additional_params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        clean: bool = True,
        on_asset_upload: Optional[Callable[[Any], None]] = None,
        on_failed_asset_upload: Optional[Callable[[Any], None]] = None,
        max_retries=UPLOAD_FILE_MAX_RETRIES,
    ):
        super(FileUploadOptions, self).__init__(
            api_key=api_key,
            project_id=project_id,
            experiment_id=experiment_id,
            upload_endpoint=upload_endpoint,
            estimated_size=estimated_size,
            timeout=timeout,
            verify_tls=verify_tls,
            additional_params=additional_params,
            metadata=metadata,
            clean=clean,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
        )
        self.file_path = file_path
        self.upload_type = upload_type
        self.base_url = base_url
        self.max_retries = max_retries


class FileLikeUploadOptions(UploadOptions):
    def __init__(
        self,
        file_like: IO,
        upload_type: str,
        base_url: str,
        api_key: str,
        project_id: str,
        experiment_id: str,
        upload_endpoint: str,
        estimated_size: int,
        timeout: float,
        verify_tls: bool,
        additional_params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        clean: bool = True,
        on_asset_upload: Optional[Callable[[Any], None]] = None,
        on_failed_asset_upload: Optional[Callable[[Any], None]] = None,
        max_retries=UPLOAD_FILE_MAX_RETRIES,
    ):
        super(FileLikeUploadOptions, self).__init__(
            api_key=api_key,
            project_id=project_id,
            experiment_id=experiment_id,
            upload_endpoint=upload_endpoint,
            estimated_size=estimated_size,
            timeout=timeout,
            verify_tls=verify_tls,
            additional_params=additional_params,
            metadata=metadata,
            clean=clean,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
        )
        self.file_like = file_like
        self.upload_type = upload_type
        self.base_url = base_url
        self.max_retries = max_retries


class RemoteAssetsUploadOptions(UploadOptions):
    def __init__(
        self,
        remote_uri: str,
        api_key: str,
        project_id: str,
        experiment_id: str,
        upload_endpoint: str,
        estimated_size: int,
        timeout: float,
        verify_tls: bool,
        additional_params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        clean: bool = True,
        on_asset_upload: Optional[Callable[[Any], None]] = None,
        on_failed_asset_upload: Optional[Callable[[Any], None]] = None,
    ):
        super(RemoteAssetsUploadOptions, self).__init__(
            api_key=api_key,
            project_id=project_id,
            experiment_id=experiment_id,
            upload_endpoint=upload_endpoint,
            estimated_size=estimated_size,
            timeout=timeout,
            verify_tls=verify_tls,
            additional_params=additional_params,
            metadata=metadata,
            clean=clean,
            on_asset_upload=on_asset_upload,
            on_failed_asset_upload=on_failed_asset_upload,
        )
        self.remote_uri = remote_uri
