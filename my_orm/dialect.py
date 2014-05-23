# coding=UTF-8


__author__ = 'Konstantin Kolesnikov'
__all__ = (
    'Select',
    'Update'
)


import re
import datetime
import MySQLdb
# from collections import OrderedDict

# from my_orm.statement import Query
import exc
import connection
from schema import Column
from schema import Table


sql_types = {
    'int': MySQLdb.NUMBER,
    'varchar': MySQLdb.STRING,
    'datetime': MySQLdb.DATETIME
}


class Node(object):
    def __init__(self):
        self.next_node = None

    # def next(self):
    #     if self.next_node is None:
    #         raise StopIteration
    #     return self.next_node

    def add_node(self, node):
        self.next_node = node


class SelectNode(Node):
    expected_nodes = ['FromNode']

    def __init__(self, columns, **kwargs):
        super(SelectNode, self).__init__()
        self.columns = columns
        self.distinct = False
        if 'distinct' in kwargs:
            self.distinct = True

    def get_query(self):
        query = 'SELECT '
        if self.distinct:
            query += 'DISTINCT '
        query += ','.join(['%s.%s' % (column.key, column.name_) for column
                           in self.columns])
        return query + ' '


class UpdateNode(Node):
    expected_nodes = ['SetNode']

    def __init__(self, table):
        super(UpdateNode, self).__init__()
        self.table = table

    def get_query(self):
        query = 'UPDATE %s ' % self.table.name_
        return query


class SetNode(Node):
    expected_nodes = ['WhereNode', 'OrderNode']

    def __init__(self, **kwargs):
        super(SetNode, self).__init__()
        self.params = kwargs

    def get_query(self):
        query = 'SET '
        params = list()
        for key, value in self.params.items():
            if not isinstance(value, int):
                value = '\x27%s\x27' % str(value)
            params.append('%s=%s' % (key, value))
        return query + ','.join(params)


class FromNode(Node):
    expected_nodes = ['WhereNode', 'OrderNode']

    def __init__(self, table_or_join):
        super(FromNode, self).__init__()
        self.table_or_join = table_or_join

    def get_query(self):
        query = 'FROM '
        if isinstance(self.table_or_join, Table):
            query += '%s ' % self.table_or_join.name_
        return query


class WhereNode(Node):
    expected_nodes = ['AndNode', 'OrNode', 'OrderNode']

    def __init__(self, condition):
        super(WhereNode, self).__init__()
        self.condition = condition

    def get_query(self):
        query = 'WHERE %s ' % self.condition
        return query


class AndNode(Node):
    expected_nodes = ['AndNode', 'OrNode', 'OrderNode']

    def __init__(self, condition):
        super(AndNode, self).__init__()
        self.condition = condition

    def get_query(self):
        query = 'AND %s ' % self.condition
        return query


class OrNode(Node):
    expected_nodes = ['AndNode', 'OrNode', 'OrderNode']

    def __init__(self, condition):
        super(OrNode, self).__init__()
        self.condition = condition

    def get_query(self):
        query = 'OR %s ' % self.condition
        return query


class OrderNode(Node):
    next_nodes = []

    def __init__(self, column, direction):
        super(OrderNode, self).__init__()
        self.column = column
        self.direction = direction

    def get_query(self):
        query = 'ORDER BY %s %s ' % (self.column, self.direction)
        return query



class SqlTree(object):
    def __init__(self):
        self._root = None
        self._current_node = self._root
        self.expected_nodes = ['SelectNode', 'UpdateNode']

    def __iter__(self):
        self._current_node = None
        # return self._current_node
        return self

    def next(self):
        if self._current_node is None:
            self._current_node = self._root
        else:
            if self._current_node.next_node is not None:
                self._current_node = self._current_node.next_node
            else:
                raise StopIteration
        return self._current_node

    def add_node(self, node, *args, **kwargs):
        assert(issubclass(node, Node))

        if node.__name__ in self.expected_nodes:
            if self._root is None:
                self._root = node(*args)
                self._current_node = self._root
            else:
                self._current_node.add_node(node(*args, **kwargs))
                self._current_node = self._current_node.next_node
            self.expected_nodes = self._current_node.expected_nodes
        else:
            raise exc.ClauseError('Unexpected clause "%s", expected "%s"'
                              % (node.__name__, self.expected_nodes))

    def dump_query(self):
        query = ''
        for node in self:
            query += node.get_query()
        query += ';'
        return query


class RowResult(object):

    def __init__(self, names, result):
        self.result = result
        self.dicted_result = dict(zip(names, result))

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.result[item]
        elif isinstance(item, str):
            return self.dicted_result.get(item)


class ResultCollection(object):

    def __init__(self, names, result):
        self.rows_collection = list()
        for row in result:
            self.rows_collection.append(RowResult(names, row))

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.rows_collection[item]
        return None

    def count(self):
        return len(self.rows_collection)


class Statement(object):

    def __init__(self):
        self.sql_tree = SqlTree()
        self.connection = connection.Connection.instance()
        self.result = None

    def dump_query(self):
        return self.sql_tree.dump_query()

    def Where(self, condition):
        assert(isinstance(condition, str))

        self.sql_tree.add_node(WhereNode, condition)
        return self

    def And(self, condition):
        assert(isinstance(condition, str))

        self.sql_tree.add_node(AndNode, condition)
        return self

    def Or(self, condition):
        assert(isinstance(condition, str))

        self.sql_tree.add_node(OrNode, condition)
        return self


class Select(Statement):

    def __init__(self, *args, **kwargs):
        assert(all([isinstance(arg, Column) for arg in args]))
        super(Select, self).__init__()

        self.columns = args[0:]
        distinct = False
        if 'distinct' in kwargs:
            distinct = True
        self.sql_tree = SqlTree()
        # self.query = None

        self.sql_tree.add_node(SelectNode, self.columns, distinct=distinct)

    def __iter__(self):
        if self.result is None:
            columns_name = [column.name_ for column in self.columns]
            query = self.dump_query()
            self.result = ResultCollection(columns_name,
                                           self.connection.execute(query).fetchall())
        return iter(self.result.rows_collection)

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.result.rows_collection[item]
        return None

    def count(self):
        if self.result is None:
            columns_name = [column.name_ for column in self.columns]
            query = self.dump_query()

            self.result = ResultCollection(columns_name,
                                           self.connection.execute(query).fetchall())
        return self.result.count()

    def From(self, table_or_join):
        assert(isinstance(table_or_join, Table))

        self.sql_tree.add_node(FromNode, table_or_join)
        return self


class Update(Statement):

    def __init__(self, table):
        assert(isinstance(table, Table))
        super(Update, self).__init__()

        self.table = table

        self.sql_tree.add_node(UpdateNode, self.table)

    def __str__(self):
        if self.result is None:
            query = self.dump_query()
            self.result = self.connection.execute(query)
        return str(self.result.rowcount)

    def Set(self, **kwargs):
        assert(all([k in self.table for k in kwargs.keys()]))

        self.sql_tree.add_node(SetNode, **kwargs)
        return self


class MySQLDialect(object):
    def __init__(self):
        self.parser = MySQLTableDefinitionParser()

    def get_table_names(self, connection, schema):
        string = 'SHOW TABLES FROM %s;' % schema
        rv = None
        try:
            rv = connection.execute(string)
        except Exception as e:
            print(e)

        if rv is not None:
            return [row[0] for row in rv.fetchall()]

    def get_foreign_keys(self, connection, table_name, schema):
        state = self._get_parsed_definition(connection, table_name)
        print('STATE %s' % state.table_name)
        return state.constraints

    def get_columns(self, connection, table_name, schema):
        state = self._get_parsed_definition(connection, table_name)
        return state.columns

    def _get_parsed_definition(self, connection, table_name):
        definition = self._get_show_create_table(connection, table_name)
        state = self.parser.parse_definition(definition)
        return state

    def _get_show_create_table(self, connection, table_name):
        string = 'SHOW CREATE TABLE %s;' % table_name
        rv = None
        try:
            rv = connection.execute(string)
        except Exception as e:
            print(e)

        row = rv.fetchone()
        return row[1].strip()


class ReflectedState(object):

    def __init__(self):
        self.columns = []
        self.table_name = None
        self.keys = []
        self.constraints = []


class MySQLTableDefinitionParser(object):

    def __init__(self):
        self._prepare_regexps()

    def parse_definition(self, definition):
        state = ReflectedState()
        for line in re.split('\r?\n', definition):
            if line.startswith('CREATE'):
                self._parse_table_name(line, state)
            elif line.startswith('  '):
                self._parse_column(line, state)

        return state

    def _parse_table_name(self, line, state):
        mr = re.match(self._re_table_name, line)
        if mr is not None:
            state.table_name = mr.group('name')

    def _parse_column(self, line, state):
        mr = re.match(self._re_column_def, line)
        if mr is not None:
            spec = mr.groupdict()
            name, type_, args, not_null = \
                spec['name'], spec['col_type'], spec['arg'], spec['notnull']

            type_ = sql_types[type_]

            state.columns.append(Column(name, type_, key=state.table_name))

    def _prepare_regexps(self):
        self._re_table_name = re.compile(
            r'CREATE\s+(?:\w+\s+)?TABLE `(?P<name>.+)`\s+\($', re.I | re.UNICODE)

        self._re_column_def = re.compile(
            r'\s+`(?P<name>\w+)`\s+'
            r'(?P<col_type>\w+)'
            r'(?:\((?P<arg>\d+|\d+,\d+)\))?'
            r'(?:\s+(?P<unsigned>UNSIGNED))?'
            r'(?:\s+(?P<zerofill>ZEROFILL))?'
            r'(?:\s+CHARACTER SET\s+(?P<charset>\w+))?'
            r'(?:\s+COLLATE\s+(?P<collate>\w+))?'
            r'(?:\s+(?P<notnull>NOT NULL))?'
            r'(?:\s+(?:DEFAULT\s+(?P<default>NULL)))?'
            r'(?:\s+(?P<autoincrement>AUTO_INCREMENT))?'
            r'(?:\s+(?:COMMENT\s+\'(?P<comment>\w+)\'))?'
            r'(?:\s+COLUMN FORMAT\s+(?P<format>FIXED|DYNAMIC|DEFAULT))?'
            r'(?:\s+STORAGE\s+(?P<storage>DISK|MEMORY|DEFAULT))?'
            r'.$', re.I | re.UNICODE)

        self._re_key = re.compile(
            r'\s+(?:(?P<type>\w+) )?KEY'
            r'(?:\s+`(?P<name>\w+)`)?'
            r'(?:\s+USING\s+(?P<using>BTREE|HASH))?'
            r'\s+\((?P<column>\.+)\)'
            r'(?:\s+USING\s+(?P<using_post>\w+))?'
            r'(?:\s+KEY_BLOCK_SIZE\s*[ =]? *(?P<keyblock>\w+))?'
            r'(?:\s+WITH PARSER\s+(?P<parser>\w+))?'
            r',?$', re.I | re.UNICODE)

        self._re_constraint = re.compile(
            r'\s+CONSTRAINT'
            r'\s+`(?P<name>\w+)`'
            r'\s+FOREIGN KEY'
            r'\s+\((?P<local>[^\)]+?)\) REFERENCES'
            r'\s+`(?P<table>\w+)`'
            r'\s+\((?P<foreign>[^\)]+?)\)'
            r'(?:\s+(?P<match>MATCH \w+))?'
            r'(?:\s+ON DELETE\s+(?P<ondelete>RESTRICT|CASCADE|SET NULL|NOACTION))?'
            r'(?:\s+ON UPDATE\s+(?P<onupdate>RESTRICT|CASCADE|SET NULL|NOACTION))?',
            re.I | re.UNICODE)


if __name__ == '__main__':
    posts = Table('posts', None)
    result = Select(Column('id', int, posts)).From(posts).Where(Column('created_at', datetime.datetime, posts)>=datetime.datetime(2014, 04, 24))
    # print(result.sql_tree._root.next_node.next_node.next_node)
    print(result.dump_query())
