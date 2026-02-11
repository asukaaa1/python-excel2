from pathlib import Path
import sys


REQUIRED_FILES = ("login.html", "index.html")


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    output_dir = repo_root / "dashboard_output"

    if not output_dir.exists():
        print(f"ERROR: Missing directory: {output_dir}")
        return 1

    missing = []
    for filename in REQUIRED_FILES:
        file_path = output_dir / filename
        if not file_path.is_file():
            missing.append(str(file_path))

    if missing:
        print("ERROR: Missing required dashboard files:")
        for file_path in missing:
            print(f"- {file_path}")
        return 1

    print("dashboard_output verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
