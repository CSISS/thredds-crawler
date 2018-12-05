import sys, os
assert sys.version_info >= (3,6)

from datetime import datetime, timedelta

# relative path for loading lib modules
sys.path.append(os.path.abspath('.'))
sys.path.append(os.path.abspath('..'))

from lib.indexdb import IndexDB
from lib.config import config


db = IndexDB(config['index_db_url'], False)


print(type(db.get_collection_updated_at('col_url1')))
print(db.get_collection_updated_at('col_url1'))