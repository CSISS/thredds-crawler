from ..siphon.catalog import TDSCatalog, Dataset, CatalogRef

from queue import Queue


class BaseScraper():
    def __init__(self):
        self.queue = Queue(maxsize=0)

    def add_queue_item(self, item):
        self.queue.put(item)


    def process_queued_item(self, item):
        if(type(item) == TDSCatalog):
            self.process_catalog(item)
        elif(type(item) == CatalogRef):
            self.process_catalog_ref(item)
        elif(type(item) == Dataset):
            self.process_dataset(item)
