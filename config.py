# -*- coding: utf-8 -*-

from collections import namedtuple

from infi.clickhouse_orm.database import Database


TABLE_NAMES = ['a_table', 'b_table']

DATE_FORMAT = '%Y-%m-%d'
START_DATE = '2017-01-01'
VALID_GROUP_BY_ARGS = ['date', 'app', 'country']

# Config
DB = namedtuple('Db', 'host port db_name user password')
db_config = DB(host='localhost', port=8123, db_name='test', user='default', password='')

# Database
db = Database(db_name=db_config.db_name,
              db_url='http://' + db_config.host.strip() + ':' + str(db_config.port),
              username=db_config.user,
              password=db_config.password)
