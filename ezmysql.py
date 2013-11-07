#!/usr/bin/env python
#coding: utf8
#
# Copyright 2013 Ebuinfo
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A lightweight wrapper around umysql (ultramysql).
"""

from __future__ import absolute_import, division, with_statement

import itertools
import logging
import time

import umysql


__title__ = 'ezmysql'
__version__ = "1.0"
__author__ = 'Veelion Chong'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2013 Ebuinfo'


class Connection(object):
    """A lightweight wrapper around umysql connections.

    It provides wrapped rows in a dict/object so that
    columns can be accessed by name. Typical usage::

        db = ezmysql.Connection("localhost", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title

    USING NOTES:
        0. Don not quote the '%s' in sql, umysql will process it, e.g.
            Bad:
                sql = 'update tbl set title="%s" where id=1'
                sql = 'insert into tbl(title, author) values("%s", "%s")'
            Good:
                sql = 'update tbl set title=%s where id=1'
                sql = 'insert into tbl(title, author) values(%s, %s)'
        1. umysql will do escape_string job for MySQL,
           you don't need to warry about it
        2. We explicitly set the character encoding to UTF-8
           on all connections to avoid encoding errors.
    """
    def __init__(self, host, user, password,
                 database='',
                 charset='utf8'):
        self.host = host
        self.charset = charset

        args = dict(
            user=user,
            password=password,
            charset="utf8",
            db=database,
            autocommit=1,
        )
        # We accept a host(:port) string
        pair = host.split(":")
        if len(pair) == 2:
            args["host"] = pair[0]
            args["port"] = int(pair[1])
        else:
            args["host"] = host
            args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        print 'reconnecting MySQL ...'
        self.close()
        self._db = umysql.Connection()
        self._db.connect(
            self._db_args['host'],
            self._db_args['port'],
            self._db_args['user'],
            self._db_args['password'],
            self._db_args['db'],
            self._db_args['autocommit'],
            self._db_args['charset'],
        )

    def escape(self, s):
        return s.replace('\\', '\\\\').replace('"', '\\\"').replace("'", "\\\'")

    def literal(self, args):
        args_escaped = []
        for arg in args:
            if isinstance(arg, (int, long)):
                args_escaped.append(arg)
                continue
            if isinstance(arg, unicode):
                arg = arg.encode('utf8')
            arg = self.escape(arg)
            args_escaped.append(arg)
        return tuple(args_escaped)

    def execute(self, query, *args):
        """Executes the given query, returning what returned from the query."""
        try:
            r = self._db.query(query, args)
        except Exception, e:
            if (e[0] == 0 or
                e[0] == 2006):
                ## 0 : Connection reset by peer when receiving
                ## 2006: MySQL server has gone away
                ## try to reconnect MySQL for these errors
                self.reconnect()
                r = self._db.query(query, args)
            else:
                raise e
        return r

    def query(self, query, *args):
        """Returns a row list for the given query and args."""
        r = self.execute(query, *args)
        column_names = [d[0] for d in r.fields]
        return [Row(itertools.izip(column_names, row)) for row in r.rows]

    def get(self, query, *args):
        """Returns the first row returned for the given query."""
        r = self.execute(query, *args)
        if not r.rows:
            return None
        else:
            column_names = [d[0] for d in r.fields]
            return Row(itertools.izip(column_names, r.rows[0]))

    ## high-level interface to interactive MySQL
    def is_in_table(self, table_name, field, value):
        sql = 'SELECT %s FROM %s WHERE %s="%s"' % (field, table_name, field, value)
        d = self.get(sql)
        if d is not None: return True
        return False

    def update_table(self, table_name, updates,
                     where_field, where_value):
        '''updates is a dict of {field_update:value_update}'''
        sets = []
        for k in updates.keys():
            s = '%s=%%s' % k
            sets.append(s)
        sets = ','.join(sets)
        sql = 'UPDATE %s SET %s WHERE %s=%%s' % (
            table_name,
            sets,
            where_field
        )
        args = updates.values()
        args.append(where_value)
        return self.execute(sql, *args)

    def item_to_table(self, table_name, item):
        '''item if a dict : key is mysql table field'''
        fields = ','.join(item.keys())
        valstr = ','.join(['%s'] * len(item))
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fields, valstr)
        try:
            r = self.execute(sql, *(item.values()))
            return r
        except Exception, e:
            #traceback.print_exc()
            if e[0] == 1062: # just skip duplicated item
                pass
            else:
                print 'item:'
                for k,v in item.items():
                    print k, ' : ', v
                raise e

    def items_to_table(self, table_name, items):
        '''insert multi-item to the table
            items is a list of dict
        '''
        if not items:return
        result = [0,0]
        for item in items:
            r = self.item_to_table(table_name, item)
            result[0] += r[0]
            result[1] = r[1]
        return result


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)



