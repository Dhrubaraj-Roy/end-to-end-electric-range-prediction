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

try:
    from mypy_boto3_sagemaker import SageMakerClient
    from mypy_boto3_sagemaker.type_defs import DescribeTrainingJobResponseTypeDef
except ImportError:
    from typing import Any, Dict

    SageMakerClient = Any
    DescribeTrainingJobResponseTypeDef = Dict[str, Any]
