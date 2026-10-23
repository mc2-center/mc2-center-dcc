import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "portal_tables"))

# Never hit the network: model files come from the vendored v14.0.0 fixtures.
os.environ["MC2_MODEL_DIR"] = str(Path(__file__).parent / "fixtures")
