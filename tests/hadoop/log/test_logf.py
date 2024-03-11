"""Test the aiutil.logf module.
"""

from pathlib import Path
import aiutil.hadoop.logf

BASE_DIR = Path(__file__).parent


def test_main():
    output = BASE_DIR / "log_s"
    args = aiutil.hadoop.logf.parse_args(
        [
            "filter",
            str(BASE_DIR / "application_1611634725250_1347938"),
            "-o",
            str(output),
            "-d",
        ]
    )
    aiutil.hadoop.logf.main(args)
    assert output.is_file()
    text = output.read_text()
    assert "- Possible Error Lines -" in text
    assert "- Deduped Error Lines -" in text
