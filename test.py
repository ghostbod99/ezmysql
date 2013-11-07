#!/usr/bin/env python
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

import ezmysql
import lzma
import zlib

if __name__ == '__main__':
    user = 'blabla'
    password = '*****'
    db = ezmysql.Connection(
        'localhost',
        user,
        password,
        'testdb',
    )
    database = 'testdb'
    sql = 'create database if not exists %s' % database
    db.execute(sql)
    db.execute('use %s' % database)

    sql = '''CREATE TABLE IF NOT EXISTS `simple` (
      `id` int unsigned NOT NULL AUTO_INCREMENT,
      `title` varchar(1000) CHARACTER SET utf8mb4 DEFAULT NULL,
      `text` mediumtext CHARACTER SET utf8mb4,
      `author` varchar(1000) NOT NULL DEFAULT 'Jim',
      `length` tinyint(3) unsigned NOT NULL DEFAULT '0',
      `pubtime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
      `bin` blob,
      PRIMARY KEY (`id`)
    ) ENGINE=MyISAM  DEFAULT CHARSET=utf8mb4 ;'''
    db.execute(sql)

    ## db.execute()...
    sql = "insert into simple(title, text) values(%s, %s)"
    title = 'ezultramysql'
    text = 'text\nez\r\nultramysql%%\\123,item of the first'
    r = db.execute(sql, title, text)
    row = db.get('select * from simple where id=%s' % r[1])
    assert row['title'] == title
    assert row['text'] == text

    print '## test WARNING-0'
    sql = 'update simple set title="%s" where id=1'
    r = db.execute(sql, 'apple')
    row = db.get('select title from simple where id=1')
    print '%s != %s' % (row['title'], 'apple')
    assert row['title'] != 'apple'

    print '\n## db.get()...'
    row = db.get('select * from simple limit %s', 1)
    for k,v in row.items():
        print '%s:%s' % (k,v)

    print '\n## high-level interface testing...'
    g = db.is_in_table('simple', 'id', 3)
    print 'is_ in:', g
    bin_zip = 'this is zlib to compress string'
    updates = {
        'title': 'by_"update"_table()',
        'text': 'by_update_table()\n\rzzz',
        'length': 123,
        'bin': zlib.compress(bin_zip)
    }
    r = db.update_table('simple', updates, 'id', 1)
    print 'update_table:', r
    r = db.get('select * from simple where id=1')
    assert r['title'] == updates['title']
    assert r['text'] == updates['text']
    assert bin_zip == zlib.decompress(r['bin'])

    new_item = {
        'title':'item_to_table_title',
        'text':'item_to_table_text\r\na%\t\t\\',
        'bin':lzma.compress('this is text stored as binary')
    }
    r = db.item_to_table('simple', new_item)
    print 'item_to_table():', r
    row = db.get('SELECT * from simple where id=%s' % r[1])
    assert row['title'] == new_item['title']
    assert row['text'] == new_item['text']
    assert row['bin'] == new_item['bin']
    print lzma.decompress(row['bin'])
    print 'text:',row['text']

    items = [
        {
            'title':'items_to_table_1',
            'text':'items_to_table_1',
        },
        {
            'title':'items_to_table_2',
            'text':'items_to_table_2',
        }
    ]
    r = db.items_to_table('simple', items)
    print 'items_to_table():', r

    print '\n## db.query()...'
    rows = db.query('select * from simple where text like %s limit %s', '%item%', 10)
    for r in rows:
        for k,v in r.items():
            print '%s:%s' % (k,v)
        print '======================'

    #db.execute('drop database %s' % database)
    db.close()

    print 'testing succeed!'


