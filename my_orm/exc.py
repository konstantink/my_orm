# coding=UTF-8


__author__ = 'Konstantin Kolesnikov'


class MyORMError(Exception):
    pass


class ArgumentError(MyORMError):
    pass


class ClauseError(MyORMError):
    pass
