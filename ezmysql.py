# -*- coding: utf-8 -*-


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
import sys
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

        db = umysql_helper.Connection("localhost", "mydatabase")
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
    def __init__(self, host, port, user, password,
                 database='',
                 charset='utf8'):


        self.host = host
        self.port = port
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
            args["port"] = port

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)
        #print 'obj init end'

    # @classmethod
    # def getConnect(cls, host, port, user, password, db):
    #     conn = Connection(host, user, password, db)
    #     sql = 'create database if not exists %s' % db
    #     conn.execute(sql)
    #     conn.execute('use %s' % db)
    #     return conn



    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        #print 'reconnecting MySQL ...'
        #self.close()
        self._db = umysql.Connection()

        # print self._db_args['host']
        # print self._db_args['port']
        # print self._db_args['user']
        # print self._db_args['password']
        # print self._db_args['db']
        # print self._db_args['autocommit']
        # print self._db_args['charset']

        self._db.connect(
            self._db_args['host'],
            (int)(self._db_args['port']),
            self._db_args['user'],
            self._db_args['password'],
            self._db_args['db'],
            self._db_args['autocommit'],
            self._db_args['charset']
        )

        #print 'self._db.connect:', self._db

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


    def select_table_by_fields(self, table_name, select_fields,
                     where_dict, limit_conf=None, select_type="list"):
        selects = []
        for k in select_fields:
            s = '%s' % k
            selects.append(s)
        selects = ','.join(selects)


        wheres = []
        for k in where_dict.keys():
            s = '%s=%%s' % k
            wheres.append(s)
        wheres = " AND ".join(wheres)

        sql = 'SELECT %s FROM %s WHERE %s' % (
            selects,
            table_name,
            wheres
        )

        if limit_conf is not None:
            select_type = "list"
            sql += ' LIMIT %s, %s' % (limit_conf['start'], limit_conf['count'])

        args = where_dict.values()
        
        print sql, args

        if select_type=="get":
            return self.get(sql+" LIMIT 1", *args)
        else:
            return self.query(sql, *args)





    def update_table_by_fields(self, table_name, updates,
                     where_fields, where_values):
        sets = []
        for k in updates.keys():
            s = '%s=%%s' % k
            sets.append(s)
        sets = ','.join(sets)
        wheres = []
        for k in where_fields:
            s = '%s=%%s' % k
            wheres.append(s)
        wheres = " AND ".join(wheres)

        sql = 'UPDATE %s SET %s WHERE %s' % (
            table_name,
            sets,
            wheres
        )
        args = updates.values()
        args += where_values

        print sql, args
        return self.execute(sql, *args)


    def delete_table_by_fields(self, table_name,
                     where_fields, where_values):
        wheres = []
        for k in where_fields:
            s = '%s=%%s' % k
            wheres.append(s)
        wheres = " AND ".join(wheres)

        sql = 'DELETE FROM %s WHERE %s' % (
            table_name,
            wheres
        )
        args = where_values
        print sql
        print args
        return self.execute(sql, *args)


    def item_to_table(self, table_name, item):
        '''item if a dict : key is mysql table field'''
        fields = ','.join(item.keys())
        valstr = ','.join(['%s'] * len(item))
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (table_name, fields, valstr)
        try:
            print sql, item.values()
            r = self.execute(sql, *(item.values()))
            return r
        except Exception, e:
            #traceback.print_exc()
            if e[0] == 1062: # just skip duplicated item
                pass
            else:
                # print 'item:'
                # for k,v in item.items():
                #     print k, ' : ', v
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



if __name__ == "__main__":
    cnn = umysql.Connection()
    cnn.connect ("127.0.0.1", 3306, "root", "123456", "tracking_db")
    try:
        #cnn = umysql.Connection()
        cnn.connect ("127.0.0.1", 3306, "root", "123456", "tracking_db")
    except Exception, e:
     print e[1]
    pass
    cnn.close()

