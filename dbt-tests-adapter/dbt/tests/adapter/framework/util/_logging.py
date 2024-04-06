import json
import os
from typing import Any, Dict, List, Optional


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
