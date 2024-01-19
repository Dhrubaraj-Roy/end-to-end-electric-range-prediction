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
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************

import comet_ml.config
import comet_ml.connection


def setup(api_key, use_cache):
    config = comet_ml.config.get_config()
    api_key = comet_ml.config.get_api_key(api_key, config)
    client = comet_ml.connection.get_rest_api_client(
        "v2",
        api_key=api_key,
        use_cache=use_cache,
        headers={"X-COMET-SDK-SOURCE": "API"},
    )

    return client
