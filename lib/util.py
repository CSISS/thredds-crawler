import re
import urllib.request
import os

import pathlib

def http_getfile(url, file):
    # print("http_getfile %s" % url)
    # if(not os.path.isfile(file)):
    urllib.request.urlretrieve(url, file)
    print("downloaded %s" % file)


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    # value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    return value


def mkdir_p(dirpath):
    pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)