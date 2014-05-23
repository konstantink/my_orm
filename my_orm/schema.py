# coding=UTF-8


__author__ = 'Konstantin Kolesnikov'
__all__ = (
    'Table',
    'Column',
)


from collections import OrderedDict

import exc


# class SchemaItem(object):
#     pass


class Table(object):

    def __init__(self, name, metadata):
        self.name_ = name
        self.column_collection = dict()

    def __getattr__(self, item):
        try:
            return self.column_collection[item]
        except KeyError:
            raise AttributeError('Column "%s" is not defined in table "%s'
                                 % (item, self.name_))

    def __contains__(self, item):
        return item in self.column_collection

    def add_column(self, column):
        if not column.name_:
            raise exc.ArgumentError('Column should have a name')
        if column.name_ not in self.column_collection:
            self.column_collection[column.name_] = column
        else:
            raise exc.ArgumentError('Column "%s" is already defined'
                                    ' for table "%s"' % (column.name_, self.name_))


class Column(object):

    def __init__(self, name, type_, key=None):
        self.name_ = name
        self.type_ = type_
        self.key = key
        # self.table = table

    def __eq__(self, loperand):
        return '%s = %s' % (self.name_, self._quote_operand(loperand))

    def __ne__(self, loperand):
        return '%s != %s' % (self.name_, self._quote_operand(loperand))

    def __gt__(self, loperand):
        return '%s > %s' % (self.name_, self._quote_operand(loperand))

    def __ge__(self, loperand):
        return '%s >= %s' % (self.name_, self._quote_operand(loperand))

    def __lt__(self, loperand):
        return '%s < %s' % (self.name_, self._quote_operand(loperand))

    def __le__(self, loperand):
        return '%s <= %s' % (self.name_, self._quote_operand(loperand))

    def _quote_operand(self, operand):
        if isinstance(operand, int):
            return operand
        return '\x27%s\x27' % str(operand)


# class MetaData(SchemaItem):
#
#     def __init__(self, connection=None):
#         pass