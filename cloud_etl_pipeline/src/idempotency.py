import hashlib
from datetime import datetime

def generate_run_id(df):
    base = f"{len(df)}-{datetime.now().isoformat()}"
    return hashlib.md5(base.encode()).hexdigest()[:10]