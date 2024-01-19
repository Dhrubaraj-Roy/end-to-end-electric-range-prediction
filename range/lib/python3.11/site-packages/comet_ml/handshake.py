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

import logging
from typing import Any, Dict, Optional

from comet_ml.config import DEFAULT_ASSET_UPLOAD_SIZE_LIMIT, DEFAULT_UPLOAD_SIZE_LIMIT
from comet_ml.utils import log_once_at_level

LOGGER = logging.getLogger(__name__)


class ExperimentHandshakeResponse(object):
    def __init__(
        self,
        run_id: str,
        ws_server: str,
        project_id: Optional[str],
        is_github: bool,
        focus_link: Optional[str],
        last_offset: int,
        upload_limit: int,
        video_upload_limit: int,
        asset_upload_limit: int,
        feature_toggles: Dict[str, bool],
        web_asset_url: Optional[str],
        web_image_url: Optional[str],
        api_asset_url: Optional[str],
        api_image_url: Optional[str],
        experiment_name: Optional[str],
        s3_direct_access_enabled: bool,
    ) -> None:
        self.run_id = run_id
        self.ws_server = ws_server
        self.project_id = project_id
        self.is_github = is_github
        self.focus_link = focus_link
        self.last_offset = last_offset
        self.upload_limit = upload_limit
        self.video_upload_limit = video_upload_limit
        self.asset_upload_limit = asset_upload_limit
        self.feature_toggles = feature_toggles
        self.web_asset_url = web_asset_url
        self.web_image_url = web_image_url
        self.api_asset_url = api_asset_url
        self.api_image_url = api_image_url
        self.experiment_name = experiment_name
        self.s3_direct_access_enabled = s3_direct_access_enabled


def _preprocess_upload_limit(value: int, upload_type: str, default_value: int) -> int:
    if isinstance(value, int) and value > 0:
        # The limit is given in Mb, convert it back in bytes
        return value * 1024 * 1024
    else:
        LOGGER.debug(
            "Fallback to default %s upload size limit, %r value is invalid",
            upload_type,
            value,
        )
        return default_value


def parse_experiment_handshake_response(
    res_body: Dict[str, Any]
) -> ExperimentHandshakeResponse:
    run_id = res_body["runId"]  # type: str
    ws_server = res_body["ws_url"]  # type: str

    project_id = res_body.get("project_id", None)  # type: Optional[str]

    is_github = bool(res_body.get("githubEnabled", False))

    focus_link = res_body.get("focusUrl", None)  # type: Optional[str]

    last_offset = res_body.get("lastOffset", 0)  # type: int

    s3_direct_access_enabled = res_body.get("s3DirectAccessEnabled", False)

    # Upload limit
    upload_limit = _preprocess_upload_limit(
        res_body.get("upload_file_size_limit_in_mb", None),
        upload_type="",
        default_value=DEFAULT_UPLOAD_SIZE_LIMIT,
    )

    video_upload_limit = _preprocess_upload_limit(
        res_body.get("uploadVideoMaxSizeMB", None),
        upload_type="video",
        default_value=DEFAULT_UPLOAD_SIZE_LIMIT,
    )

    asset_upload_limit = _preprocess_upload_limit(
        res_body.get("asset_upload_file_size_limit_in_mb", None),
        upload_type="asset",
        default_value=DEFAULT_ASSET_UPLOAD_SIZE_LIMIT,
    )

    res_msg = res_body.get("msg")
    if res_msg:
        log_once_at_level(logging.INFO, res_msg)

    # Parse feature toggles
    feature_toggles = {}  # type: Dict[str, bool]
    LOGGER.debug("Raw feature toggles %r", res_body.get("featureToggles", []))
    for toggle in res_body.get("featureToggles", []):
        try:
            feature_toggles[toggle["name"]] = bool(toggle["enabled"])
        except (KeyError, TypeError):
            LOGGER.debug("Invalid feature toggle: %s", toggle, exc_info=True)
    LOGGER.debug("Parsed feature toggles %r", feature_toggles)

    # Parse URL prefixes
    web_asset_url = res_body.get("cometWebAssetUrl", None)  # type: Optional[str]
    web_image_url = res_body.get("cometWebImageUrl", None)  # type: Optional[str]
    api_asset_url = res_body.get("cometRestApiAssetUrl", None)  # type: Optional[str]
    api_image_url = res_body.get("cometRestApiImageUrl", None)  # type: Optional[str]

    experiment_name = res_body.get("name", None)  # type: Optional[str]

    return ExperimentHandshakeResponse(
        run_id=run_id,
        ws_server=ws_server,
        project_id=project_id,
        is_github=is_github,
        focus_link=focus_link,
        last_offset=last_offset,
        upload_limit=upload_limit,
        video_upload_limit=video_upload_limit,
        asset_upload_limit=asset_upload_limit,
        feature_toggles=feature_toggles,
        web_asset_url=web_asset_url,
        web_image_url=web_image_url,
        api_asset_url=api_asset_url,
        api_image_url=api_image_url,
        experiment_name=experiment_name,
        s3_direct_access_enabled=s3_direct_access_enabled,
    )
