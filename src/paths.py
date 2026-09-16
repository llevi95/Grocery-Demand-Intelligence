from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"
GOLD = ROOT / "data" / "gold"
EXTERNAL = ROOT / "data" / "external"

def p(path: Path) -> str:
    """Spark wants strings, not Path objects."""
    return str(path)