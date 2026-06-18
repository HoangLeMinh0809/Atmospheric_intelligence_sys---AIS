# CQL heredoc regression checks.
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"


def _cql_heredoc_blocks(text: str) -> list[str]:
    pattern = re.compile(r"cqlsh_stdin\s*<<CQL\s*\n(.*?)\nCQL", re.DOTALL)
    return [match.group(1) for match in pattern.finditer(text)]


def test_cql_heredocs_do_not_contain_shell_hash_comments():
    offenders: list[str] = []

    for path in SCRIPTS_DIR.rglob("*.sh"):
        text = path.read_text(encoding="utf-8")
        for index, block in enumerate(_cql_heredoc_blocks(text), start=1):
            for line in block.splitlines():
                if re.match(r"^\s*#", line):
                    offenders.append(f"{path.relative_to(ROOT)}:block{index}:{line.strip()}")

    assert not offenders, "Shell-style comments inside CQL heredocs break cqlsh parsing:\n" + "\n".join(offenders)
