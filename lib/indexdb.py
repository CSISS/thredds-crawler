# import records
import sqlalchemy as sql
import datetime

import threading

mutex = threading.RLock()

class IndexDB():
    def __init__(self, db_url, echo=False):
        self.db_url = db_url
        self.sql_engine = sql.create_engine(db_url, echo=echo)
        self.sql_metadata = sql.MetaData(self.sql_engine)

        self.define_sql_schema()
        self.create_sql_tables()


    def define_sql_schema(self):
        self.collections = sql.Table('collections', self.sql_metadata,
            sql.Column('id', sql.Integer, primary_key=True),
            sql.Column('name', sql.String),
            sql.Column('url', sql.String),
            sql.Column('updated_at', sql.DateTime)
        )

        self.granules = sql.Table('granules', self.sql_metadata,
            sql.Column('id', sql.Integer, primary_key=True),
            sql.Column('collection_id', None, sql.ForeignKey('collections.id')),
            sql.Column('name', sql.String),
            sql.Column('iso_url', sql.String),
            sql.Column('access_url', sql.String),
            sql.Column('time_start', sql.DateTime),
            sql.Column('time_end', sql.DateTime)
        )

    def create_sql_tables(self):
        self.sql_metadata.create_all(self.sql_engine)

    def drop_sql_tables(self):
        self.sql_metadata.drop_all(self.sql_engine)
        # for tbl in reversed(self.sql_metadata.sorted_tables):
        #     self.sqlengine.execute(tbl.delete())

    def create_collection(self, **kwargs):
        with mutex:
            result = None
            with self.sql_engine.begin() as conn:
                result = conn.execute(self.collections.insert(kwargs))
            cid = result.inserted_primary_key[0]
            self.touch_collection(cid)
            return cid

    def find_collection(self, name=None, url=None):
        with mutex:
            result = None
            if not (name == None and url == None):    
                select = sql.select([self.collections.c.id]).where(sql.or_(self.collections.c.name == name, self.collections.c.url == url))
                with self.sql_engine.begin() as conn:
                    row = conn.execute(select).fetchone()
                    result = row and row[0]

            return result


    def find_granule(self, name):
        with mutex:
            select = sql.select([self.granules.c.id]).where(self.granules.c.name == name)
            with self.sql_engine.begin() as conn:
                row = conn.execute(select).fetchone()
                return(row and row[0])

    def find_or_create_collection(self, name, url):
        with mutex:
            cid = self.find_collection(name=name, url=url)
            return cid if cid else self.create_collection(name=name, url=url)

    def touch_collection(self, cid):
        now = datetime.datetime.now()
        with mutex:
            update = self.collections.update().values(updated_at=now).where(self.collections.c.id == cid)
            with self.sql_engine.begin() as conn:
                conn.execute(update)

    def delete_collection_granules(self, collection_name):
        with mutex:
            cid = self.find_collection(name=collection_name)
            delete = self.granules.delete().where(self.granules.c.collection_id == cid)
            with self.sql_engine.begin() as conn:
                conn.execute(delete)

    def get_collection_updated_at(self, name=None, url=None):
        with mutex:
            if name == None and url == None:
                return None
            select = sql.select([self.collections.c.updated_at]).where(sql.or_(self.collections.c.name == name, self.collections.c.url == url))
            with self.sql_engine.begin() as conn:
                row = conn.execute(select).fetchone()
                return row and row[0]

    def get_collection_catalog_url(self, name=None):
        with mutex:
            if name == None:
                return None
            select = sql.select([self.collections.c.url]).where(self.collections.c.name == name)
            with self.sql_engine.begin() as conn:
                row = conn.execute(select).fetchone()
                return row and row[0]



    def create_granule(self, collection_id, **kwargs):
        # with mutex:
        kwargs['collection_id'] = collection_id
        with self.sql_engine.begin() as conn:
            result = conn.execute(self.granules.insert(kwargs))

    def create_granules(self, collection_id, granules):
        # with mutex:
        for g in granules:
            g.pop('collection_name')
            g['collection_id'] = collection_id

        with self.sql_engine.begin() as conn:
            result = conn.execute(self.granules.insert(), granules)

    def add_collection_granules(self, cid, granules):
        # with mutex:
        # g = self.find_granule(granule['name'])
        # if g == None:
        self.create_granules(cid, granules)

        self.touch_collection(cid)


    def get_collection_granules(self, collection_name, time_start, time_end):
        with mutex:
            cid = self.find_collection(name=collection_name)
            if cid == None:
                return([])

            gs = self.granules
            select = sql.select([gs]).where(sql.and_(gs.c.time_start >= time_start, gs.c.time_end <= time_end, gs.c.collection_id == cid))
            with self.sql_engine.begin() as conn:
                results = conn.execute(select)
                return [dict(r) for r in results] 


