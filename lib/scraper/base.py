from ..siphon.catalog import TDSCatalog, Dataset, CatalogRef

from queue import Queue

def fread(path):
    with open(path, 'r') as f:
        return(f.read())

def fwrite(path, text):
    with open(path, 'w') as f:
        f.write(text)

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


    def apply_filter(self, fl, ds, path):
        text = fread(path)
        
        if fl.is_required(ds, text):
            text = fl.apply(ds, text)
            fwrite(path, text)
       
