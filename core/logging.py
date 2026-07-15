import logging
import sys

DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"

_configured = False


def setup_logging(level: int = logging.INFO) -> None:
    """configure the root logger once: single stdout handler, timestamp/level/module format.

    call at the top of each endpoint's run(). idempotent - later calls are no-ops, so
    it's safe to call from multiple entrypoints in the same process.
    """
    global _configured
    if _configured:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)

    _configured = True
