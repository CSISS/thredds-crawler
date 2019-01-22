import re


from queue import Queue

from datetime import datetime, timedelta

from ..siphon.catalog import TDSCatalog, Dataset

from ..scraper.base_scraper import BaseScraper

from ..util.datetime import timestamp_re, timestamp_parser
from ..util.path import slugify
from ..util.http import http_getfile


class CollectionGranuleIndexer(BaseScraper):
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


    def index_dataset(self, ds):
        # print("index ds %s" % ds.id)
        # print(ds.time_coverage)
        access_url = ds.access_urls.get('HTTPServer') or ds.access_urls.get('OPENDAP')

        start, end = self.time_coverage_to_time_span(**ds.time_coverage)
        result = {'name': ds.authority_ns_id, 'iso_url': ds.iso_md_url, 'access_url': access_url, 'time_start': start, 'time_end': end}
        self.indexes.append(result)

        
    def process_catalog(self, catalog):
        print("process catalog " + catalog.catalog_url)
        for ds_name, ds in catalog.datasets.items():
            self.add_queue_item(ds)
 
        for ref_name, ref in catalog.catalog_refs.items():
            self.add_queue_item(ref)

    def process_catalog_ref(self, catalog_ref):
        print("process catalog_ref " + catalog_ref.href)
        catalog = catalog_ref.follow()
        self.add_queue_item(catalog)

    def process_dataset(self, ds):
        if(timestamp_re.search_date(ds.name) != None):
            print("index dataset id %s" % ds.id)
            self.index_dataset(ds)
