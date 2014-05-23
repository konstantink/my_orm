# coding=UTF-8


__author__ = 'Konstantin Kolesnikov'
__all__ = [
    'URL'
]


import re

from .. import exc


class URL(object):

    def __init__(self, user=None, passwd=None, host=None, port=None, db=None):
        self.user = user
        self.passwd = passwd
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.db = db

    def _to_string(self):
        string = 'mysql://'
        string += self.user
        if self.passwd is not None:
            string += ':%s' % self.passwd
        string += '@%s' % self.host
        if self.port is not None:
            string += ':%s' % self.port
        string += '/%s' % self.db

        return string

    def __str__(self):
        return self._to_string()

    def __repr__(self):
        return '<my_orm.url.URL: %s>' % self._to_string()

    def to_connection_args(self):
        transformed = dict()
        args = ['user', 'passwd', 'host', 'port', 'db']
        for name in args:
            if getattr(self, name, False):
                transformed[name] = getattr(self, name)
        return transformed


def _parse_url(url_string):
    """
    url = mysql://<user>[:<passwd>]@<host>[:<port>]/<db>
    """
    pattern = re.compile(r'''
         (?:mysql://)
         (?P<user>\w*)
         (?::(?P<passwd>.+))?
         (?:@(?P<host>[^/:]+))
         (?::(?P<port>\d+))?
         (?:/(?P<db>\w+))
     ''', re.X)

    mr = re.match(pattern, url_string)
    if mr is not None:
        parts = mr.groupdict()
        return URL(**parts)
    else:
        raise exc.ArgumentError('Could not parse a url string.')


def make_url(url_string):
    return _parse_url(url_string)