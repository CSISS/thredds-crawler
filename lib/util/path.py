import re
import os
import pathlib


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    # value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = re.sub('[^\w\s-]', '-', value).strip().lower()
    value = re.sub('[\s]+', '-', value)
    return value


def mkdir_p(dirpath):
    pathlib.Path(dirpath).mkdir(parents=True, exist_ok=True)