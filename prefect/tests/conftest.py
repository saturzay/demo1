import sys
from pathlib import Path

# Make `flows` importable regardless of how pytest is invoked (matches the
# layout inside the prefect-worker container, where flows/ lives at /app/flows
# next to this tests/ directory).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
