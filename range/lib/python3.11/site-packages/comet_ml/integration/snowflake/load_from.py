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
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************

import logging

from ... import _reporting

LOGGER = logging.getLogger(__name__)


def get_artifact_v1(experiment, artifact_name, workspace=None, version_or_alias=None):
    """
    Get the SQL query for a Snowflake Artifact.

    Args:
        experiment: Experiment (required), instance of Experiment object
        artifact_name: string (required), artifact name
        workspace: string (optional), retrieve an artifact belonging to that workspace
        version_or_alias: Iterable of string (optional), retrieve the artifact by the given alias or version

    Returns: String or None
    """
    artifact = experiment.get_artifact(artifact_name, workspace, version_or_alias)

    metadata = artifact.metadata

    if "query" in metadata:
        experiment.__internal_api__report__(
            event_name=_reporting.GET_SNOWFLAKE_ARTIFACT,
        )
        return metadata["query"]
    else:
        LOGGER.error(
            "Artifact is not a valid Snowflake Artifact and doesn't have a query associated with it"
        )
        return None
