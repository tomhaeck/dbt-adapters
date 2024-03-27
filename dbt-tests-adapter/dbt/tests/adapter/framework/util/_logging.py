from functools import partial
import json
import os
from typing import Any, Callable, Dict, List, Optional

from dbt_common.events.base_types import EventLevel, EventMsg
from dbt_common.events.event_manager_client import (
    add_logger_to_manager,
    cleanup_event_logger,
    get_event_manager,
)
from dbt_common.events.functions import (
    env_scrubber,
    get_capture_stream,
    get_stdout_config,
    make_log_dir_if_missing,
)
from dbt_common.events.logger import LineFormat, LoggerConfig
from dbt_common.invocation import get_invocation_id


# These are the logging events issued by the "clean" command,
# where we can't count on having a log directory. We've removed
# the "class" flags on the events in types.py. If necessary we
# could still use class or method flags, but we'd have to get
# the type class from the msg and then get the information from the class.
_NOFILE_CODES = ["Z012", "Z013", "Z014", "Z015"]


def get_logging_events(log_output: str, event_name: str) -> List[Dict[str, Any]]:
    logging_events = []
    for log_line in log_output.split("\n"):
        # skip empty lines
        if len(log_line) == 0:
            continue
        # The adapter logging also shows up, so skip non-json lines
        if not log_line.startswith("{"):
            continue
        if event_name in log_line:
            log_dct = json.loads(log_line)
            if log_dct["info"]["name"] == event_name:
                logging_events.append(log_dct)
    return logging_events


def assert_message_in_logs(message: str, logs: str, expected_pass: Optional[bool] = True) -> None:
    # if the logs are json strings, then 'jsonify' the message because of things like escape quotes
    if os.environ.get("DBT_LOG_FORMAT", "") == "json":
        message = message.replace(r'"', r"\"")

    if expected_pass:
        assert message in logs
    else:
        assert message not in logs


def setup_event_logger(flags, callbacks: List[Callable[[EventMsg], None]] = []) -> None:
    """
    TODO: move this into `dbt_common.events.event_manager_client` with `cleanup_event_logger`
    """
    cleanup_event_logger()
    make_log_dir_if_missing(flags.LOG_PATH)
    event_manager = get_event_manager()
    event_manager.callbacks = callbacks.copy()

    if flags.LOG_LEVEL != "none":
        line_format = _line_format_from_str(flags.LOG_FORMAT, LineFormat.PlainText)
        log_level = (
            EventLevel.ERROR
            if flags.QUIET
            else EventLevel.DEBUG
            if flags.DEBUG
            else EventLevel(flags.LOG_LEVEL)
        )
        console_config = get_stdout_config(
            line_format,
            flags.USE_COLORS,
            log_level,
            flags.LOG_CACHE_EVENTS,
        )

        if get_capture_stream():
            # Create second stdout logger to support test which want to know what's
            # being sent to stdout.
            console_config.output_stream = get_capture_stream()
        add_logger_to_manager(console_config)

    if flags.LOG_LEVEL_FILE != "none":
        # create and add the file logger to the event manager
        log_file = os.path.join(flags.LOG_PATH, "dbt.log")
        log_file_format = _line_format_from_str(flags.LOG_FORMAT_FILE, LineFormat.DebugText)
        log_level_file = EventLevel.DEBUG if flags.DEBUG else EventLevel(flags.LOG_LEVEL_FILE)
        add_logger_to_manager(
            _get_logfile_config(
                log_file,
                flags.USE_COLORS_FILE,
                log_file_format,
                log_level_file,
                flags.LOG_FILE_MAX_BYTES,
            )
        )


def _line_format_from_str(format_str: str, default: LineFormat) -> LineFormat:
    if format_str == "text":
        return LineFormat.PlainText
    elif format_str == "debug":
        return LineFormat.DebugText
    elif format_str == "json":
        return LineFormat.Json

    return default


def _get_logfile_config(
    log_path: str,
    use_colors: bool,
    line_format: LineFormat,
    level: EventLevel,
    log_file_max_bytes: int,
    log_cache_events: bool = False,
) -> LoggerConfig:
    return LoggerConfig(
        name="file_log",
        line_format=line_format,
        use_colors=use_colors,
        level=level,  # File log is *always* debug level
        scrubber=env_scrubber,
        filter=partial(_logfile_filter, log_cache_events),
        invocation_id=get_invocation_id(),
        output_file_name=log_path,
        output_file_max_bytes=log_file_max_bytes,
    )


def _logfile_filter(log_cache_events: bool, msg: EventMsg) -> bool:
    return msg.info.code not in _NOFILE_CODES and not (
        msg.info.name in ["CacheAction", "CacheDumpGraph"] and not log_cache_events
    )
