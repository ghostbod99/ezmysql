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
                 charset='utf8',
                 autocommit=1):


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


    def start_transaction(self):
        return self.execute("START TRANSACTION")

    def commit(self):
        return self.execute("COMMIT")

    def rollback(self):
        return self.execute("ROLLBACK")


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


    def is_in_table_by_wheres(self, table_name, field, where_dict):
        
        wheres = []
        for k in where_dict.keys():
            s = '%s=%%s' % k
            wheres.append(s)
        wheres = " AND ".join(wheres)

        sql = 'SELECT %s FROM %s WHERE %s' % (field, table_name, wheres)
        args = where_dict.values()

        print sql, args

        d = self.get(sql, *args)
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

    select_process = {
        "__in_": lambda k, where_dict: '%s in (%s)' % ( k, where_dict[k]['__in_'] ),
        "__ne_": lambda k, where_dict: '%s!=%s' % (k, where_dict[k]['__ne_']),
        "__lt_": lambda k, where_dict: '%s<%s' % (k, where_dict[k]['__lt_']),
        "__lte_": lambda k, where_dict: '%s<=%s' % (k, where_dict[k]['__lte_']),
        "__gt_": lambda k, where_dict: '%s>%s' % (k, where_dict[k]['__gt_']),
        "__gte_": lambda k, where_dict: '%s>=%s' % (k, where_dict[k]['__gte_']),
        "__like_": lambda k, where_dict: "%s LIKE '%s%%%%'" % (k, where_dict[k]['__like_']),
        "__all_like_": lambda k, where_dict: "%s LIKE '%%%%%s%%%%'" % (k, where_dict[k]['__all_like_']),
    }

    def select_table_by_wheres(self, table_name, select_fields, where_dict, limit_conf=None, select_type="list", group_by_fields=None, order_by_fields=None, lock=False):
        '''根据条件查询记录  add by ghostbod'''


        selects = ','.join(select_fields)

        wheres = []
        for k in where_dict.keys():
            if type(where_dict[k])==dict:
                protype = where_dict[k].keys()[0]
                if protype in self.select_process.keys():
                    s = self.select_process[protype](k, where_dict)
                where_dict.pop(k)
            else:
                s = '%s=%%s' % k
            wheres.append(s)

        if len(wheres) != 0:
            wheres = " AND ".join(wheres)

        if len(wheres)==0:
            sql = 'SELECT %s FROM %s ' % (selects, table_name)
        else:
            sql = 'SELECT %s FROM %s WHERE %s' % (selects, table_name, wheres)

        if group_by_fields is not None:
            group_bys = ','.join(group_by_fields)
            sql += ' GROUP BY %s' % group_bys

        if order_by_fields is not None:
            order_bys = ','.join(order_by_fields)
            sql += ' ORDER BY %s' % order_bys

        if limit_conf is not None:
            # select_type = "list"
            sql += ' LIMIT %s, %s' % (limit_conf['start'], limit_conf['count'])

        args = where_dict.values()
        lock_str = " FOR UPDATE " if lock == True else ""
        
        if select_type=="get":
            print sql+" LIMIT 1"+lock_str, args
            return self.get(sql+" LIMIT 1"+lock_str, *args)
        else:
            print sql+lock_str, args
            return self.query(sql+lock_str, *args)



    def select_tables_by_wheres(self, table, join_tables, select_fields, where_dict, limit_conf=None, select_type="list", group_by_fields=None, order_by_fields=None, lock=False):
        '''
            根据条件查询记录  add by ghostbod

            select_fields =['*']
            table = {"name": "table1", "alias": "t1"}
            join_tables=[{
                "name": "table2",
                "alias": "t2",
                "on": ["t1.id=t2.id", "t1.cid=t2.cid"]
            },{
                "name": "table3",
                "alias": "t3",
                "on": ["t3.id=t2.id", "t3.cid=t2.cid"]
            }]

            where_dict = {
                "t1.id": 1,
                "t2.id": 2
            }
        '''

        # print table, join_tables, select_fields, where_dict, limit_conf, select_type, group_by_fields, order_by_fields, lock
        selects = ','.join(select_fields)
        joins = []
        for k in join_tables:
            s = 'LEFT JOIN %s %s ON %s' % (k['name'], k['alias'], ' AND '.join(k['on']) )
            joins.append(s)
        joins = ' '.join(joins)
        print where_dict
        print where_dict.keys()
        wheres = []
        for k in where_dict.keys():
            if type(where_dict[k])==dict:
                print k, where_dict[k].keys()
                protype = where_dict[k].keys()[0]
                if protype in self.select_process.keys():
                    s = self.select_process[protype](k, where_dict)
                where_dict.pop(k)
            else:
                s = '%s=%%s' % k
            wheres.append(s)
        if len(wheres) != 0:
            wheres = " AND ".join(wheres)

        print wheres
        args = where_dict.values()

        if len(wheres)==0:
            sql = 'SELECT %s FROM %s ' % (selects, "%s %s %s" % (table['name'], table['alias'], joins))
        else:
            sql = 'SELECT %s FROM %s WHERE %s' % (
                selects,
                "%s %s %s" % (table['name'], table['alias'], joins),
                wheres
            )

        if group_by_fields is not None:
            group_bys = ','.join(group_by_fields)
            sql += ' GROUP BY %s' % group_bys

        if order_by_fields is not None:
            order_bys = ','.join(order_by_fields)
            sql += ' ORDER BY %s' % order_bys

        if limit_conf is not None:
            # select_type = "list"
            sql += ' LIMIT %s, %s' % (limit_conf['start'], limit_conf['count'])

        lock_str = " FOR UPDATE " if lock == True else ""

        if select_type=="get":
            print sql+" LIMIT 1"+lock_str, args
            return self.get(sql+" LIMIT 1"+lock_str, *args)
        else:
            print sql+lock_str, args
            return self.query(sql+lock_str, *args)


    update_process = {
        "__dec_": lambda k, updates: ('%s=%s-%%s' % (k, k), updates[k]['__dec_']),
        "__inc_": lambda k, updates: ('%s=%s+%%s' % (k, k), updates[k]['__inc_']),
        "__eq_": lambda k, updates: ('%s=%s' % (k, updates[k]['__eq_']), None)
    }

    update_where_process = {
        "__in_": lambda k, where_dict: '%s in (%s)' % ( k, where_dict[k]['__in_'] ),
        # "__ne_": lambda k, where_dict: '%s!=%s' % (k, where_dict[k]['__ne_']),
        # "__lt_": lambda k, where_dict: '%s<%s' % (k, where_dict[k]['__lt_']),
        # "__lte_": lambda k, where_dict: '%s<=%s' % (k, where_dict[k]['__lte_']),
        # "__gt_": lambda k, where_dict: '%s>%s' % (k, where_dict[k]['__gt_']),
        # "__gte_": lambda k, where_dict: '%s>=%s' % (k, where_dict[k]['__gte_']),
        # "__like_": lambda k, where_dict: "%s LIKE '%s%%%%'" % (k, where_dict[k]['__like_']),
        # "__all_like_": lambda k, where_dict: "%s LIKE '%%%%%s%%%%'" % (k, where_dict[k]['__all_like_']),
    }


    def update_table_by_wheres(self, table_name, updates, where_dict):
        '''
            根据条件更新记录 add by ghostbod
            {"current_quantity": {'__inc_':ibp_item['quantity_actual']} }
            {"current_quantity": {'__eq_':'quantity_actual'} }

        '''
        sets = []
        for k in updates.keys():
            if type(updates[k])==dict:
                protype = updates[k].keys()[0]
                if protype in self.update_process.keys():
                    s, updates[k] = self.update_process[protype](k, updates)
                    if updates[k] == None:
                        updates.pop(k)
            else:
                s = '%s=%%s' % k
            sets.append(s)
        sets = ','.join(sets)

        wheres = []
        for k in where_dict.keys():
            if type(where_dict[k])==dict:
                protype = where_dict[k].keys()[0]
                if protype in self.select_process.keys():
                    s = self.select_process[protype](k, where_dict)
                where_dict.pop(k)
            else:
                s = '%s=%%s' % k
            wheres.append(s)
        wheres = " AND ".join(wheres)

        sql = 'UPDATE %s SET %s WHERE %s' % (
            table_name,
            sets,
            wheres
        )
        args = updates.values()

        args += where_dict.values()

        print sql, args
        return self.execute(sql, *args)



    def update_table_by_fields(self, table_name, updates, where_fields, where_values):
        '''根据条件更新记录'''
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

    def delete_table_by_wheres(self, table_name, where_dict):
        '''根据字典条件删除记录'''
        wheres = []
        for k in where_dict.keys():
            s = '%s=%%s' % k
            wheres.append(s)
        wheres = " AND ".join(wheres)

        sql = 'DELETE FROM %s WHERE %s' % (
            table_name,
            wheres
        )
        args = where_dict.values()
        print sql, args
        return self.execute(sql, *args)


    def delete_table_by_fields(self, table_name,
                     where_fields, where_values):
        '''根据条件删除记录'''
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
        print sql, args
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
            print '--------DB Exception-------\n', e
            #traceback.print_exc()
            if e[0] == 1062: # just skip duplicated item
                return -1062, 0
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

