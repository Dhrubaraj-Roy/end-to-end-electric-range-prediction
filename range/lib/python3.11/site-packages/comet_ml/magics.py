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

from .exceptions import CometException

CELL_ID = None

try:
    from IPython import get_ipython
    from IPython.core.magic import register_cell_magic
    from IPython.core.magic_arguments import argument  # noqa: E501
    from IPython.core.magic_arguments import magic_arguments, parse_argstring
    from IPython.display import IFrame
except ImportError:
    raise ImportError(
        "Comet magics require IPython; Use `pip install IPython`"
    ) from None

try:
    from streamlit_jupyter_magic.server import DEBUG, get_streamlit_page
    from streamlit_jupyter_magic.utils import in_colab_environment
except ImportError:
    raise ImportError(
        "Comet magics require streamlit_jupyter_magic; Use `pip install streamlit-jupyter-magic`"
    ) from None


class CometMagicException(CometException):
    def __str__(self):
        return "Comet magic was unable to be registered"


try:

    def pre_run_cell(info):
        global CELL_ID
        if hasattr(info, "cell_id"):
            CELL_ID = info.cell_id

    get_ipython().events.register("pre_run_cell", pre_run_cell)

    @magic_arguments()
    @argument("workspace_project", help="Path of Comet workspace/project", type=str)
    @argument(
        "-n",
        "--name",
        help="A unique name for the Streamlit page",
        type=str,
        default=None,
    )
    @argument(
        "-h",
        "--host",
        help="Host for the Streamlit Server",
        default="localhost",  # noqa: E501
    )
    @argument(
        "-p", "--port", help="Port for the Streamlit Server", default=5000
    )  # noqa: E501
    @argument(
        "--width",
        help="Width, percent or pixels, for the iframe",
        default="100%",  # noqa: E501
    )
    @argument(
        "--height", help="Height, in pixels, for the iframe", default="300px"
    )  # noqa: E501
    @argument(
        "--use-colab-workaround",
        help="Use when colab won't open iframe",
        default=False,
        action="store_true",
    )
    @register_cell_magic
    def comet(line, cell):
        args = parse_argstring(comet, line)
        workspace, project = args.workspace_project.split("/")
        preamble = f"""import os
os.environ["COMET_WORKSPACE"] = "{workspace}"
os.environ["COMET_PROJECT_NAME"] = "{project}"
"""
        code = preamble + cell

        if args.name is None:
            args.name = CELL_ID if CELL_ID else "comet-default"

        results = get_streamlit_page(args.host, args.port, args.name, code)

        if in_colab_environment():
            from google.colab import output

            if args.use_colab_workaround:
                output.serve_kernel_port_as_window(
                    args.port,
                    path="/page_%d" % results["page"],
                    anchor_text="Open streamlit app in window",
                )
            else:
                output.serve_kernel_port_as_iframe(
                    args.port,
                    path="/page_%d" % results["page"],
                    width=args.width,
                    height=args.height,
                )
        else:
            return IFrame(
                src="http://%s:%s/page_%d"
                % (
                    args.host,
                    args.port,
                    results["page"],
                ),
                width=args.width,
                height=args.height,
            )

except Exception:
    raise CometMagicException() from None
