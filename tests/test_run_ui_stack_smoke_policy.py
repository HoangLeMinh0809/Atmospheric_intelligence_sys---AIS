from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_run_ui_stack_manifest_smoke_is_non_strict_by_default():
    script = (ROOT / "scripts" / "run_ui_stack.ps1").read_text(encoding="utf-8")

    assert "[switch]$RequireVisualizationManifest" in script
    assert "function Test-VisualizationManifestEndpoint" in script
    assert "Visualization manifest is not ready yet; UI/API deployment is healthy" in script
    assert "Run TODO4 visualization/export steps" in script
    assert "if ($RequireVisualizationManifest) {" in script
    assert "Test-VisualizationManifestEndpoint -Url $manifestUrl" in script
