
import asyncio
import json
import sys

import click
import yaml

from .agent import run_agent


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1)
@click.version_option()
def main(args):
    asyncio.get_event_loop().run_until_complete(run_agent(*args))


if __name__ == "__main__":
    main()
