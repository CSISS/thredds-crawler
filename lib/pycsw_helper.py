from six.moves import configparser
from six.moves import input
import getopt
import sys

from pycsw.core import admin, config, repository



class PycswHelper():
    def __init__(self):
        try:
            self.context = config.StaticContext()

            config_path = '/etc/pycsw/pycsw.cfg'

            safe_config = configparser.SafeConfigParser()
            with open(config_path) as f:
                safe_config.readfp(f)

            self.config = safe_config

            self.database = safe_config.get('repository', 'database')
            self.url = safe_config.get('server', 'url')
            self.home = safe_config.get('server', 'home')
            self.metdata = dict(safe_config.items('metadata:main'))
            try:
                self.table = safe_config.get('repository', 'table')
            except configparser.NoOptionError:
                self.table = 'records'
        except:
            print("[ERROR] failed to load pycsw config")
            self.context = None 


    def load_records(self, xml_dir):
        if self.context == None:
            print("[WARN] pycsw config not available. NOT LOADING " + xml_dir)
            return
        # admin.load_records(CONTEXT, DATABASE, TABLE, XML_DIRPATH, RECURSIVE, FORCE_CONFIRM)
        admin.load_records(self.context, self.database, self.table, xml_dir, True, True)


    def delete_records(self, constraint={'where': '', 'values': []}):
        if self.context == None:
            print("[WARN] pycsw config not available. NOT DELETING RECORDS")
            return
        repo = repository.Repository(self.database, self.context, table=self.table)
        repo.delete(constraint=constraint)







