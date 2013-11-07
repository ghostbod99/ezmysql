ezmysql - Easily use umysql/ultramysql
=======
A lightweight wrapper around umysql connections, which is,
A fast MySQL driver written in pure C/C++ for Python. Compatible with gevent through monkey patching.

It provides wrapped rows in a dict/object so that
columns can be accessed by name. Typical usage::
``` python
    db = ezmysql.Connection("localhost", "mydatabase")
    for article in db.query("SELECT * FROM articles"):
        print article.title
```

USING NOTES:
``` bash
    0. Don not quote the '%s' in sql, umysql will process it, e.g.
        Bad:
            sql = 'update tbl set title="%s" where id=1'
            sql = 'insert into tbl(title, author) values("%s", "%s")'
        Good:
            sql = 'update tbl set title=%s where id=1'
            sql = 'insert into tbl(title, author) values(%s, %s)'
    1. umysql will do escape_string job for MySQL,
       you don't need to warry about it
    2. It sets the character encoding to UTF-8 by default
       on all connections to avoid encoding errors.
```

Installation
============
```bash
    python setup.py install
```

Example
=======
See the file 'test.py'
