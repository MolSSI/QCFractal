"""
This product includes GeoLite2 data created by MaxMind, available from
https://www.maxmind.com
(attribution requirement)
"""


from os.path import dirname, join, abspath
import logging

logger = logging.getLogger(__name__)


_app_path = dirname(dirname(abspath(__file__)))
_geo_file = "GeoLite2-City.mmdb"
geo_file_path = join(_app_path, 'data', 'geo',
                                'GeoLite2-City_latest', _geo_file)
geoip2_reader = None

try:
    import geoip2.database
    geoip2_reader = geoip2.database.Reader(geo_file_path)
except ImportError as err:
    logger.error(f'Cannot import geoip2 module. To use API access logging, you need '
                 f'to install it manually using `pip install geoip2`')
except FileNotFoundError as err:
    logger.error(f'Geoip cites file cannot be read from {geo_file_path}.\n'
                f'Make sure to manually download the file from: \n'
                f'https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz\n'
                f'Then, set the geo_file_path in qcfractal_config.yaml in your base_folder '
                f'(default base_folder is ~/.qca/qcfractal/qcfractal_config.yaml).')


def get_geoip2_data(ip_address):
    out = {}

    if not geoip2_reader:
        return out

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