import os

# Remote-only frontend mode:
# all scraper/download/corrigendum flows use server backend.
os.environ["BID_FRONTEND_REMOTE_ONLY"] = "1"

from bid_pyside6 import run


if __name__ == "__main__":
    raise SystemExit(run())
