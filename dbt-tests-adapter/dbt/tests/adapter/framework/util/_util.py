from datetime import UTC, datetime
from typing import Optional


def check_datetime_between(
        timestr: str, start: datetime, end: Optional[datetime] = None
) -> None:
    """
    Assert `timestr` is between `start` and `end`.
    """
    if end is None:
        end = datetime.now(tz=UTC)
    parsed = datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S.%fZ")
    assert start <= parsed
    assert parsed <= end
