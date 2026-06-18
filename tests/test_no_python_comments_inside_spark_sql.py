# Spark SQL regression checks.
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = [ROOT / "ml", ROOT / "spark_jobs", ROOT / "serving"]


def _spark_sql_blocks(text: str) -> list[str]:
    pattern = re.compile(r"spark\.sql\(\s*f?([\"']{3})(.*?)\1\s*\)", re.DOTALL)
    return [match.group(2) for match in pattern.finditer(text)]


def test_spark_sql_blocks_do_not_contain_python_hash_comments():
    offenders: list[str] = []

    for directory in SCAN_DIRS:
        for path in directory.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            for index, block in enumerate(_spark_sql_blocks(text), start=1):
                for line in block.splitlines():
                    if re.match(r"^\s*#", line):
                        offenders.append(f"{path.relative_to(ROOT)}:block{index}:{line.strip()}")

    assert not offenders, "Python-style comments inside spark.sql blocks break Spark SQL parsing:\n" + "\n".join(offenders)
