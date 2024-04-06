from io import StringIO
from typing import Any, List, Optional, Tuple
import warnings

from dbt_common.events.functions import (
    capture_stdout_logs,
    reset_metadata_vars,
    stop_capture_stdout_logs,
)


# TODO: remove dependency on dbt-core
from dbt.cli.main import dbtRunner
from dbt.logger import log_manager


def run_dbt(
    args: Optional[List[str]] = None,
    expect_pass: bool = True,
) -> Any:
    """
    Run dbt commands as cli args.

    Args:
        args: a list of dbt command line arguments, e.g.:
            run_dbt(["run", "--vars", "seed_name: base"])
        expect_pass: use if the command is expected to fail, e.g.:
            run_dbt(["test"], expect_pass=False)

    Returns: different objects depending on the command that is executed, e.g.:
        `run` (and most other commands) -> List[Any]
        `docs generate` -> CatalogArtifact
    """
    # Ignore logbook warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")

    # reset global vars
    reset_metadata_vars()

    # The logger will complain about already being initialized if we don't do this.
    log_manager.reset_handlers()
    if args is None:
        args = ["run"]

    print("\n\nInvoking dbt with {}".format(args))
    from dbt.flags import get_flags

    flags = get_flags()
    project_dir = getattr(flags, "PROJECT_DIR", None)
    profiles_dir = getattr(flags, "PROFILES_DIR", None)
    if project_dir and "--project-dir" not in args:
        args.extend(["--project-dir", project_dir])
    if profiles_dir and "--profiles-dir" not in args:
        args.extend(["--profiles-dir", profiles_dir])

    dbt = dbtRunner()
    res = dbt.invoke(args)

    # The exception is immediately raised to be caught in tests
    # using a pattern like `with pytest.raises(SomeException):`
    if res.exception is not None:
        raise res.exception

    if expect_pass is not None:
        assert res.success == expect_pass, "dbt exit state did not match expected"

    return res.result


def run_dbt_and_capture(
    args: Optional[List[str]] = None,
    expect_pass: bool = True,
) -> Tuple[Any, str]:
    """
    Use this if you need to capture the command logs in a test.
    If you want the logs that are normally written to a file, you must
    start with the "--debug" flag. The structured schema log CI test
    will turn the logs into json, so you have to be prepared for that.

    Args:
        args: a list of dbt command line arguments, e.g.:
            run_dbt(["run", "--vars", "seed_name: base"])
        expect_pass: use if the command is expected to fail, e.g.:
            run_dbt(["test"], expect_pass=False)

    Returns: the results of `run_dbt` + the logs
    """
    try:
        stringbuf = StringIO()
        capture_stdout_logs(stringbuf)
        res = run_dbt(args, expect_pass=expect_pass)
        stdout = stringbuf.getvalue()
    finally:
        stop_capture_stdout_logs()
    return res, stdout
