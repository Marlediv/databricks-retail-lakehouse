from pathlib import Path
import re

PLACEHOLDER_PATTERN = re.compile(r"\b(TODO|FIXME)\b", re.IGNORECASE)
TEXT_SUFFIXES = {".py", ".md", ".sql"}


def _gather_text_files(root: Path) -> list[Path]:
    files = []
    for path in root.rglob("*"):
        if path.is_file() and path.suffix in TEXT_SUFFIXES:
            files.append(path)
    return files


def test_notebook_or_exported_python_exists() -> None:
    ipynb_files = list(Path(".").rglob("*.ipynb"))
    dbx_export_files = (
        list(Path("notebooks").glob("*.py")) if Path("notebooks").is_dir() else []
    )
    assert ipynb_files or dbx_export_files, "No notebooks or exported .py files found in the repo."


def test_required_structure_exists() -> None:
    required = [Path("README.md"), Path("docs"), Path("notebooks"), Path("sql")]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, f"Missing required path(s): {', '.join(missing)}"


def test_no_placeholder_comments() -> None:
    files_to_scan = [Path("README.md")]
    for directory in ("docs", "notebooks", "sql"):
        directory_path = Path(directory)
        if directory_path.is_dir():
            files_to_scan.extend(_gather_text_files(directory_path))

    offending: list[str] = []
    for path in files_to_scan:
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:  # skip binary artifacts
            continue
        if PLACEHOLDER_PATTERN.search(content):
            offending.append(str(path))

    assert not offending, f"Placeholder keywords found in {', '.join(offending)}"
