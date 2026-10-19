"""Run the agent: ``python -m data_gateway.agent agent.yaml [--host 0.0.0.0] [--port 8765]``"""
import argparse
import logging

import uvicorn

from .app import AgentService, create_app, load_config


def main():
    p = argparse.ArgumentParser(description="data-gateway agent (passive)")
    p.add_argument("config", help="path to agent YAML")
    p.add_argument("--host", default="0.0.0.0", help="bind to the internal interface only")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app = create_app(AgentService(load_config(args.config)))
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
