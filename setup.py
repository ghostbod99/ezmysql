#!/usr/bin/env python

from distutils.core import setup
from ezmysql import __version__, __author__, __license__


setup(
    name='ezmysql',
    version=__version__,
    author=__author__,
    description='Easily use umysql/ultramysql',
    author_email='veelion@ebuinfo.com',
    url='https://github.com/veelion/ezmysql.git',
    py_modules=['ezmysql'],
    license=__license__
)


