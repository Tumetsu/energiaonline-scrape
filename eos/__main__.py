import datetime
import json
import logging

import click
import envparse

import eos.scrape.usage as us
from eos import get_energy_usage, get_delivery_sites
from eos.configuration import Configuration
from eos.context import Context
from eos.scrape.auth import do_login
from eos.utils import fix_date_defaults, find_site_with_code

envparse.Env.read_envfile()

log = logging.getLogger("eos")


@click.group()
@click.option("-u", "--username", envvar="EO_USERNAME", required=True)
@click.option("-p", "--password", envvar="EO_PASSWORD", required=True)
def main(*, username, password):
    cfg = Configuration(username=username, password=password)
    click.get_current_context().meta["ecs"] = Context(cfg=cfg)
    logging.basicConfig(level=logging.DEBUG)


@main.command(name="sites")
def list_delivery_sites():
    ctx: Context = click.get_current_context().meta["ecs"]
    sites = get_delivery_sites(ctx)
    print(json.dumps(sites))


def parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y-%m-%d").date()


@main.command(name="usage")
@click.option("-s", "--site", "site_id", required=True)
@click.option("--start-date", type=parse_date)
@click.option("--end-date", type=parse_date)
@click.option(
    "--resolution",
    default="hourly",
    type=click.Choice(["daily", "hourly"]),
)
def get_usage(site_id: str, start_date, end_date, resolution):
    start_date, end_date = fix_date_defaults(start_date, end_date)
    ctx: Context = click.get_current_context().meta["ecs"]

    result = get_energy_usage(ctx, site_id, start_date, end_date, resolution)
    print(json.dumps(result))


@main.command(name="update_database")
@click.option("-s", "--site", "site_id", envvar="EO_SITE_ID", required=True)
@click.option(
    "--db", "--database-url", "database_url", envvar="EO_DATABASE_URL", required=True
)
def update_database(site_id, database_url):
    ctx: Context = click.get_current_context().meta["ecs"]
    import sqlalchemy
    import eos.database as ed

    engine = sqlalchemy.create_engine(database_url)
    metadata = ed.get_metadata(engine)
    metadata.create_all()
    do_login(ctx.sess, ctx.cfg)
    site = find_site_with_code(ctx.sess, metering_point_code=site_id)
    usage = us.get_usage(
        sess=ctx.sess,
        site=site,
    )
    log.info(
        f"Hourly usage data entries: {len(usage.hourly_usage_data)}: "
        f"{min(usage.hourly_usage_data)} .. {max(usage.hourly_usage_data)}"
    )
    ed.populate_usage(metadata, usage)


if __name__ == "__main__":
    main()
