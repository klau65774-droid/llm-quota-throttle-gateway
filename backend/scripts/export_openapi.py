"""Export FastAPI OpenAPI schema to backend/docs/openapi.json."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_DIR = _REPO_ROOT / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from llm_gateway.api import create_app  # noqa: E402
from llm_gateway.config import GatewaySettings  # noqa: E402


def main() -> None:
    """Write formatted OpenAPI 3.x specification without leaving test DB artifacts."""
    out_path = _BACKEND_DIR / "docs" / "openapi.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    app = create_app(GatewaySettings())
    schema = app.openapi()
    out_path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    print("[OK] Exported OpenAPI schema -> backend/docs/openapi.json")


if __name__ == "__main__":
    main()
