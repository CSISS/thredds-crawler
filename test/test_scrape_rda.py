import env

from lib.siphon.catalog import TDSCatalog, Dataset

from lib.scraper.granule import GranuleScraper

from lib.util.path import slugify
from lib.util.http import http_getfile


out_dir = "test/tmp"

scraper = GranuleScraper(out_dir)


cat = TDSCatalog('https://rda.ucar.edu/thredds/catalog/files/g/ds631.1/asr15.anl.2D/catalog.xml')
ds = cat.datasets['asr15km.anl.2D.20000101.nc']

scraper.process_dataset(ds)
