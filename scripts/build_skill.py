"""Package the firebird-lib agent skill for distribution."""

import ast
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def lib_version(root: Path) -> str:
    """Read the package version without importing the library."""
    version_file = root / "src" / "firebird" / "lib" / "__about__.py"
    for node in ast.parse(version_file.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        ):
            version = ast.literal_eval(node.value)
            if isinstance(version, str):
                return version
    raise ValueError(f"No literal __version__ found in {version_file}")


def main() -> None:
    """Write a ZIP with the skill directory at its root."""
    root = Path(__file__).resolve().parent.parent
    skill = root / "skills" / "firebird-lib"
    if not (skill / "SKILL.md").is_file():
        raise FileNotFoundError(skill / "SKILL.md")
    archive_path = root / "dist" / f"firebird-lib-{lib_version(root)}-skill.zip"
    archive_path.parent.mkdir(exist_ok=True)

    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(skill.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(skill.parent))


if __name__ == "__main__":
    main()
