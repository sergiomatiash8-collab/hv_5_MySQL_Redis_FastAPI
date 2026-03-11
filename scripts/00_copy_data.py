import shutil
from pathlib import Path

SOURCE_FILES = [
    r"C:\Users\Admin\Desktop\DE_HV_Overall\HV_1\Source\(USE THIS)ad_events_header_updated.csv",
    r"C:\Users\Admin\Desktop\DE_HV_Overall\HV_1\Source\campaigns.csv",
    r"C:\Users\Admin\Desktop\DE_HV_Overall\HV_1\Source\users.csv",
]

DEST_DIR = Path("data/raw")
DEST_DIR.mkdir(parents=True, exist_ok=True)

for source in SOURCE_FILES:
    source_path = Path(source)
    dest_path = DEST_DIR / source_path.name
    shutil.copy2(source_path, dest_path)
    print(f"Copied: {source_path.name} -> {dest_path}")

