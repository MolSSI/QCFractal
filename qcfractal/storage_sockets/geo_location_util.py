"""
This product includes GeoLite2 data created by MaxMind, available from
https://www.maxmind.com
(attribution requirement)
"""

import geoip2.database
from os.path import dirname, join, abspath
import logging


logger = logging.getLogger(__name__)

_app_path = dirname(dirname(abspath(__file__)))
_geo_file = "GeoLite2-City.mmdb"
geoip2_reader = geoip2.database.Reader(join(_app_path, 'data', 'geo',
                                            'GeoLite2-City_latest', _geo_file))


def get_geoip2_data(ip_address):
    out = {}
    try:
        loc_data = geoip2_reader.city(ip_address)
        out['city'] = loc_data.city.name
        out['country'] = loc_data.country.name
        out['country_code'] = loc_data.country.iso_code
        out['ip_lat'] = str(loc_data.location.latitude)
        out['ip_long'] = str(loc_data.location.longitude)
        out['postal_code'] = loc_data.postal.code
        out['subdivision'] = loc_data.subdivisions.most_specific.name
    except:
        logger.error('Problem getting geoip data for {}'.format(ip_address))

    return out