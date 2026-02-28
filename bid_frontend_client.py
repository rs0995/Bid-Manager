import os

# Frontend client build profile:
# - scraper/tender processing is remote-only
# - local-only workflows remain projects/templates UI and local storage
os.environ["BID_FRONTEND_REMOTE_ONLY"] = "1"

from bid_pyside6 import run


if __name__ == "__main__":
    raise SystemExit(run())
