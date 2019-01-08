from six.moves import configparser
from six.moves import input
import getopt
import sys

from pycsw.core import admin, config

CONTEXT = config.StaticContext()

CFG = '/etc/pycsw/pycsw.cfg'

SCP = configparser.SafeConfigParser()
with open(CFG) as f:
    SCP.readfp(f)

DATABASE = SCP.get('repository', 'database')
URL = SCP.get('server', 'url')
HOME = SCP.get('server', 'home')
METADATA = dict(SCP.items('metadata:main'))
try:
    TABLE = SCP.get('repository', 'table')
except configparser.NoOptionError:
    TABLE = 'records'


def pycsw_load_records(xml_dir):
