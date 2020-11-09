from pathlib import Path
import dsutil.shebang
BASE_DIR = Path(__file__).parent


def test_shebang():
    dsutil.shebang.update_shebang(BASE_DIR / "script_dir", "#!/usr/bin/env python3")
    