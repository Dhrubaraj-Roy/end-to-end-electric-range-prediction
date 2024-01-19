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
import json
import logging
import os.path
import shutil
import sqlite3
import tempfile
import threading
from collections import namedtuple
from enum import IntEnum, unique
from typing import Callable, Dict, List, Optional

from .json_encoder import NestedEncoder
from .messages import (
    BaseMessage,
    CloudDetailsMessage,
    FileNameMessage,
    GitMetadataMessage,
    GpuStaticInfoMessage,
    HtmlMessage,
    HtmlOverrideMessage,
    InstalledPackagesMessage,
    LogDependencyMessage,
    LogOtherMessage,
    MessageCallbacks,
    MetricMessage,
    ModelGraphMessage,
    OsPackagesMessage,
    ParameterMessage,
    RegisterModelMessage,
    RemoteAssetMessage,
    RemoteModelMessage,
    StandardOutputMessage,
    SystemDetailsMessage,
    SystemInfoMessage,
    UploadFileMessage,
    UploadInMemoryMessage,
    WebSocketMessage,
)
from .messages_utils import convert_upload_in_memory_to_file_message

DEFAULT_DB_FILE = "comet_messages.db"

LOGGER = logging.getLogger(__name__)


@unique
class MessageStatus(IntEnum):
    registered = 1
    delivered = 2
    failed = 3


DBMessage = namedtuple("DBMessage", ["id", "type", "json", "status"])

ReplayCallback = Callable[[BaseMessage], None]


class ReplayManager:
    def __init__(self, db_file: Optional[str] = None) -> None:
        self.tmp_dir = tempfile.mkdtemp()
        if db_file is None:
            db_file = os.path.join(self.tmp_dir, DEFAULT_DB_FILE)

        self.db_file = db_file
        # open DB connection
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self._create_db_schema()

        self.message_callbacks = {}  # type: Dict[int, MessageCallbacks]

        self.closed = False

        self.__lock__ = threading.RLock()

    def _create_db_schema(self):
        with self.conn:
            self.conn.execute(
                """CREATE TABLE messages
                                    (message_id INTEGER NOT NULL PRIMARY KEY,
                                    status INTEGER NOT NULL,
                                    message_type TEXT NOT NULL,
                                    message_json TEXT NOT NULL)"""
            )

    def close(self) -> None:
        if self.closed:
            return

        with self.__lock__:
            self.closed = True

            try:
                LOGGER.debug("Closing messages DB connection")
                self.conn.close()
            except Exception:
                LOGGER.debug("Failed to close messages DB connection", exc_info=True)

            # delete temporary data
            if self.tmp_dir is not None:
                try:
                    LOGGER.debug("Cleaning temporary data dir: %r", self.tmp_dir)
                    shutil.rmtree(self.tmp_dir)
                except Exception:
                    LOGGER.debug(
                        "Failed to clean temporary data dir: %r",
                        self.tmp_dir,
                        exc_info=True,
                    )

    def register_message(
        self, message: BaseMessage, status: MessageStatus = MessageStatus.registered
    ) -> None:
        with self.__lock__:
            if self.closed:
                LOGGER.warning("Already closed - register message ignored")
                return

            message_json = self._preprocess_registered_message(message)
            # insert into DB
            values = (
                message.message_id,
                status,
                message.type,
                message_json,
            )

            with self.conn:
                self.conn.execute("INSERT INTO messages VALUES (?,?,?,?)", values)

    def register_messages(
        self,
        messages: List[BaseMessage],
        status: MessageStatus = MessageStatus.registered,
    ):
        with self.__lock__:
            if self.closed:
                LOGGER.warning("Already closed - register message ignored")
                return

            values = []
            for message in messages:
                message_json = self._preprocess_registered_message(message)
                values.append(
                    (
                        message.message_id,
                        status,
                        message.type,
                        message_json,
                    )
                )

            with self.conn:
                self.conn.executemany("INSERT INTO messages VALUES (?,?,?,?)", values)

    def _preprocess_registered_message(self, message: BaseMessage):
        if message.message_id is None:
            raise ValueError("Message ID expected")

        if isinstance(message, UploadInMemoryMessage):
            message = convert_upload_in_memory_to_file_message(message, self.tmp_dir)

        # save callbacks to be used later
        callbacks = message.get_message_callbacks()
        if callbacks is not None:
            self.message_callbacks[message.message_id] = callbacks

        return json.dumps(
            message.to_db_message_dict(),
            sort_keys=True,
            separators=(",", ":"),
            cls=NestedEncoder,
        )

    def update_message(self, message_id: int, status: MessageStatus) -> None:
        with self.__lock__:
            if self.closed:
                LOGGER.warning(
                    "Already closed - message update ignored, id: %d, status: %r",
                    message_id,
                    status,
                )
                return

            if status == MessageStatus.delivered:
                with self.conn:
                    self.conn.execute(
                        "DELETE FROM messages WHERE message_id = ?", (message_id,)
                    )
                    # delete saved message callbacks
                    if message_id in self.message_callbacks:
                        del self.message_callbacks[message_id]
            else:
                with self.conn:
                    self.conn.execute(
                        "UPDATE messages SET status = ? WHERE message_id = ?",
                        (status, message_id),
                    )

    def replay_failed_messages(self, replay_callback: ReplayCallback) -> int:
        with self.__lock__:
            if self.closed:
                LOGGER.warning("Already closed - messages replay ignored")
                return 0

            db_messages = self._fetch_failed_messages()
            if len(db_messages) == 0:
                return 0

            messages_ids = []
            for message in db_messages:
                messages_ids.append((message.id,))

            # update DB records to mark failed messages as in progress
            with self.conn:
                c = self.conn.executemany(
                    "UPDATE messages SET status = %d WHERE message_id = ?"
                    % MessageStatus.registered,
                    messages_ids,
                )
                LOGGER.debug(
                    "Updated %d DB message records for %d failed messages",
                    c.rowcount,
                    len(db_messages),
                )
        return self._replay_messages(
            db_messages=db_messages, replay_callback=replay_callback
        )

    def _replay_messages(
        self, db_messages: List[DBMessage], replay_callback: ReplayCallback
    ) -> int:
        LOGGER.debug("Replaying %d failed messages to streamer", len(db_messages))
        for message in db_messages:
            if self.closed:
                return 0

            try:
                base_message = db_message_to_message(message)

                callbacks = self.message_callbacks.get(message.id, None)
                if callbacks is not None:
                    base_message.set_message_callbacks(callbacks)

                replay_callback(base_message)
            except Exception:
                LOGGER.error("Failed to replay message: %r", message)

        return len(db_messages)

    def get_message(self, message_id: int) -> Optional[BaseMessage]:
        db_message = self.get_db_message(message_id)
        if db_message is not None:
            return db_message_to_message(db_message)
        else:
            return None

    def get_db_message(self, message_id: int) -> Optional[DBMessage]:
        with self.conn:
            c = self.conn.execute(
                "SELECT message_id, message_type, message_json, status FROM messages WHERE message_id = ?",
                (message_id,),
            )
            row = c.fetchone()
            if row is not None:
                return DBMessage(id=row[0], type=row[1], json=row[2], status=row[3])
            else:
                return None

    def _fetch_failed_messages(self) -> List[DBMessage]:
        messages_db = []
        for row in self.conn.execute(
            "SELECT message_id, message_type, message_json FROM messages WHERE status = ?",
            (MessageStatus.failed,),
        ):
            messages_db.append(
                DBMessage(
                    id=row[0], type=row[1], json=row[2], status=MessageStatus.failed
                )
            )

        return messages_db


def db_message_to_message(db_message: DBMessage) -> BaseMessage:
    message_dict = json.loads(db_message.json)
    if db_message.type == CloudDetailsMessage.type:
        message = CloudDetailsMessage.from_db_message_dict(message_dict)
    elif db_message.type == FileNameMessage.type:
        message = FileNameMessage.from_db_message_dict(message_dict)
    elif db_message.type == GitMetadataMessage.type:
        message = GitMetadataMessage.from_db_message_dict(message_dict)
    elif db_message.type == GpuStaticInfoMessage.type:
        message = GpuStaticInfoMessage.from_db_message_dict(message_dict)
    elif db_message.type == HtmlMessage.type:
        message = HtmlMessage.from_db_message_dict(message_dict)
    elif db_message.type == HtmlOverrideMessage.type:
        message = HtmlOverrideMessage.from_db_message_dict(message_dict)
    elif db_message.type == InstalledPackagesMessage.type:
        message = InstalledPackagesMessage.from_db_message_dict(message_dict)
    elif db_message.type == LogDependencyMessage.type:
        message = LogDependencyMessage.from_db_message_dict(message_dict)
    elif db_message.type == LogOtherMessage.type:
        message = LogOtherMessage.from_db_message_dict(message_dict)
    elif db_message.type == MetricMessage.type:
        message = MetricMessage.from_db_message_dict(message_dict)
    elif db_message.type == ModelGraphMessage.type:
        message = ModelGraphMessage.from_db_message_dict(message_dict)
    elif db_message.type == OsPackagesMessage.type:
        message = OsPackagesMessage.from_db_message_dict(message_dict)
    elif db_message.type == ParameterMessage.type:
        message = ParameterMessage.from_db_message_dict(message_dict)
    elif db_message.type == RegisterModelMessage.type:
        message = RegisterModelMessage.from_db_message_dict(message_dict)
    elif db_message.type == RemoteAssetMessage.type:
        message = RemoteAssetMessage.from_db_message_dict(message_dict)
    elif db_message.type == RemoteModelMessage.type:
        message = RemoteModelMessage.from_db_message_dict(message_dict)
    elif db_message.type == StandardOutputMessage.type:
        message = StandardOutputMessage.from_db_message_dict(message_dict)
    elif db_message.type == SystemDetailsMessage.type:
        message = SystemDetailsMessage.from_db_message_dict(message_dict)
    elif db_message.type == SystemInfoMessage.type:
        message = SystemInfoMessage.from_db_message_dict(message_dict)
    elif db_message.type == UploadFileMessage.type:
        message = UploadFileMessage.from_db_message_dict(message_dict)
    elif db_message.type == WebSocketMessage.type:
        message = WebSocketMessage.from_db_message_dict(message_dict)
    else:
        raise ValueError("Unsupported message type: %r" % db_message.type)

    return message
