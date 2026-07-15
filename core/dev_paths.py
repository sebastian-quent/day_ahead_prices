import sys
from pathlib import Path

# dev-only shim: makes `Database.*` (owned by the sibling Production repo) importable
# without a proper package install. Production can't be modified from here, and this
# project is likely to move into Production later, at which point Database will already
# be on the path and this becomes a no-op. Safe to delete once that happens.
_PRODUCTION_REPO = Path(__file__).resolve().parents[2] / "Production"

if _PRODUCTION_REPO.is_dir() and str(_PRODUCTION_REPO) not in sys.path:
    sys.path.insert(0, str(_PRODUCTION_REPO))
