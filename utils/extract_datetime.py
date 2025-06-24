import re
from datetime import datetime

def extract_datetime_from_filename(fname):
    match = re.search(r"@(\d{4})\.(\d{2})\.(\d{2}) (\d{6})", fname)
    if match:
        try:
            return datetime.strptime(f"{match.group(1)}-{match.group(2)}-{match.group(3)} {match.group(4)}", "%Y-%m-%d %H%M%S")
        except ValueError:
            return None
    return None

