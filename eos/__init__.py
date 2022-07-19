import datetime

import eos.scrape.usage as us
import eos.scrape.delivery_sites as dss
from eos.context import Context
from eos.scrape.auth import do_login
from eos.utils import find_site_with_code, fix_date_defaults


def get_delivery_sites(ctx: Context):
    """
    Get list of all the delivery sites listed for the user.
    :param ctx:
    :return:
    """
    do_login(ctx.sess, ctx.cfg)
    sites = [site.asdict() for site in dss.get_delivery_sites(ctx.sess)]
    return sites


def get_energy_usage(ctx: Context, site_id: str, start_date=None, end_date=None, resolution="hourly"):
    """
    Get usage data.
    :param ctx:
    :param site_id: Metering point code. Fetch with list_delivery_sites function.
    :param start_date:
    :param end_date:
    :param resolution: hourly|daily
    :return:
    """

    start_date, end_date = fix_date_defaults(start_date, end_date)
    do_login(ctx.sess, ctx.cfg)
    site = find_site_with_code(ctx.sess, site_id)

    usage = us.get_usage(
        sess=ctx.sess,
        site=site,
    )

    usage_data = (
        usage.daily_usage_data if resolution == "daily" else usage.hourly_usage_data
    )
    start_datetime = datetime.datetime.combine(
        date=start_date, time=datetime.time(0, 0, 0)
    )
    end_datetime = datetime.datetime.combine(
        date=end_date, time=datetime.time(23, 59, 59)
    )
    for ts, datum in sorted(usage_data.items()):
        if start_datetime <= ts <= end_datetime:
            return datum.as_dict()
