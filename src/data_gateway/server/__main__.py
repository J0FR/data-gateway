"""Run the relay service: ``python -m data_gateway.server`` (configure with RELAY_* env vars)."""
import logging
import os

import uvicorn

from .api import create_app


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run(create_app(), host=os.environ.get("RELAY_HOST", "0.0.0.0"),
                port=int(os.environ.get("RELAY_PORT", "8080")))


if __name__ == "__main__":
    main()
