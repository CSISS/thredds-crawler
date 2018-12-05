import re

from .siphon.catalog import TDSCatalog, Dataset

from .timestamp_util import timestamp_re, timestamp_parser
from . import siphon_ext

from .util import slugify, http_getfile

from queue import Queue

from datetime import datetime, timedelta



class CollectionGranuleIndexer():
    def __init__(self):
        self.queue = Queue(maxsize=0)
        self.download_dir = '../records/scraped'
        self.indexes = []


    # FROM: {'start': '2018-09-11T06:00:00Z', 'end': None, 'duration': 5 minutes}
    # TO: (2018-09-11T06:00:00Z, 2018-09-11T06:05:00Z)
    def time_coverage_to_time_span(self, start, end, duration):
        if start != None:
            start = timestamp_parser.parse_datetime(start)
        else:
            start = datetime.now()

        if duration != None:
            end = start + timestamp_parser.parse_duration(duration)
        elif end != None:
            end = timestamp_parser.parse_datetime(end)
        else:
            end = start

        result = (start, end)
        return result


    def index_dataset(self, catalog, ds):
        # print("index ds %s" % ds.id)
        # print(ds.time_coverage)
        iso_url = catalog.iso_md_url(ds)
        access_url = ds.access_urls.get('HTTPServer') or ds.access_urls.get('OPENDAP')

        start, end = self.time_coverage_to_time_span(**ds.time_coverage)
        result = {'name': ds.id, 'iso_url': iso_url, 'access_url': access_url, 'time_start': start, 'time_end': end}
        self.indexes.append(result)

    def scrape_catalog(self, catalog):
        # print("TRAVERSE  %s" % catalog.ref_name)
        for ref_name, ref in catalog.catalog_refs.items():
            self.queue.put(ref)

        for ds_name, ds in catalog.datasets.items():
            if(timestamp_re.search_date(ds_name) != None):
                self.index_dataset(catalog, ds)
            

