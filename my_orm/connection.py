# coding=UTF-8

__author__ = 'Konstantin Kolesnikov'
__all__ = [
    'Connect'
    'Connection'
]


import MySQLdb
from threading import Lock
from collections import OrderedDict

import exc
from utils import url
from dialect import MySQLDialect
from schema import Table


def Connect(url_or_connection_str):
    return Database(url_or_connection_str)


class Connection(object):
    _instance_lock = Lock()

    @staticmethod
    def instance(url_or_conn_str=None):
        if url_or_conn_str is None and not hasattr(Connection, '_instance'):
            raise exc.ArgumentError('Connection should be first established.')
        if not hasattr(Connection, '_instance'):
            with Connection._instance_lock:
                if not hasattr(Connection, '_instance'):
                    Connection._instance = Connection(url_or_conn_str)
        return Connection._instance

    def __init__(self, url_or_connection_str, read_schema_from_db=True):
        if isinstance(url_or_connection_str, str):
            self.url = url.make_url(url_or_connection_str)
        elif isinstance(url_or_connection_str, url.URL):
            self.url = url_or_connection_str

        self.connection = MySQLdb.connect(**self.url.to_connection_args())
        self.cursor = self.connection.cursor()
        self.dialect = MySQLDialect()
        # self.tables_collection = dict()

        # self._explore_database(self.url.db)

    def explore_database(self, schema=None):
        tables_collection = dict()
        if schema is None:
            schema = self.url.db
        tables = self.get_table_names(schema)
        for table in tables:
            if table in tables_collection:
                raise exc.ArgumentError('Table "%s" is already defined'
                                        ' in this database' % table)
            tables_collection[table] = Table(table, None)
            for column in self.get_columns(table, schema):
                tables_collection[table].add_column(column)

        return tables_collection

        # self.get_foreign_keys(schema, tables[0])

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
            return self.cursor
        except Exception as e:
            print(e)
            return None

    def get_table_names(self, schema):
        return self.dialect.get_table_names(self, schema)

    def get_foreign_keys(self, table, schema):
        return self.dialect.get_foreign_keys(self, table, schema)

    def get_columns(self, table, schema):
        return self.dialect.get_columns(self, table, schema)


class Database(object):

    def __init__(self, url_or_connection_str):
        self.connection = Connection.instance(url_or_connection_str)
        # self.tables_collection = dict()

        self.tables_collection = self.connection.explore_database()

    def __getattr__(self, item):
        try:
            return self.tables_collection[item]
        except KeyError:
            raise AttributeError('Table "%s" is not defined in database "%s'
                                 % (item, self.connection.url.db))

