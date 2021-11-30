import asyncio
import json
import sys

import click
import yaml

from .agent import run_agent


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1)
@click.option("--spec", type=str, default="", help="")
@click.version_option()
def main(args, spec: str):
    asyncio.get_event_loop().run_until_complete(run_agent(*args, spec=spec))


if __name__ == "__main__":
    main()
