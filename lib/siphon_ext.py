from .siphon.catalog import TDSCatalog, Dataset
import urllib.parse


def follow_refs(self, *names):
    catalog = self
    for n in names:
        catalog = catalog.catalog_refs[n].follow()
    return catalog


def iso_md_url(self, ds):
    try:
        return ds.access_urls['ISO'] + \
            '?catalog=' + \
            urllib.parse.quote_plus(self.catalog_url) + \
            '&dataset=' + \
            urllib.parse.quote_plus(ds.id)
    except Exception as e:
        print("bad iso_md_url", self.catalog_url, ds.id)
        print(e)
        return ""

TDSCatalog.follow_refs = follow_refs
TDSCatalog.iso_md_url = iso_md_url