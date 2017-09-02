# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

from infi.clickhouse_orm import models, fields, engines

import config


class BaseModel(models.Model):

    def __init__(self, **kwargs):
        super(BaseModel, self).__init__(**kwargs)

    @classmethod
    def get_last_id(cls, db):
        query_last_id = """
            SELECT
                _id FROM {table_name}
            ORDER BY
                _id
            DESC
            LIMIT 1
            """.format(table_name=cls.table_name())
        return int(db.raw(query_last_id) or 0)

    @classmethod
    def get_last_date(cls, db):
        query_last_date = """
            SELECT
                date FROM {table_name}
            ORDER BY
                _id
            DESC
            LIMIT 1
            """.format(table_name=cls.table_name())
        last_date = db.raw(query_last_date).strip()
        if last_date:
            last_date = datetime.strftime(
                datetime.strptime(last_date, config.DATE_FORMAT) + timedelta(days=1),
                config.DATE_FORMAT)
        return last_date or config.START_DATE


class A(BaseModel):

    _id = fields.Int64Field(default=0)
    date = fields.DateField()
    app = fields.StringField()
    country = fields.StringField()
    users = fields.Int32Field()

    engine = engines.MergeTree('date', ('app', 'country'))

    @classmethod
    def table_name(cls):
        return 'a_table'

    @classmethod
    def insert_rows(cls, db, rows):
        """
        :param db: clickhouse.Database object
        :param rows: [LIST] of dictionaries
            [
                { "date": "2017-08-22", "app": "Evernote", "country": "BY", "users": 431, },
            ]
        :return: [BOOL]
        """
        offset_id = cls.get_last_id(db) + 1
        data = [
            A(_id=i,
              date=row.get('date'),
              app=row.get('app'),
              country=row.get('country', 'ZZ'),
              users=row.get('users', 0)) for i, row in enumerate(rows, offset_id)]
        db.insert(data)

        return True


class B(BaseModel):

    _id = fields.Int64Field(default=0)
    date = fields.DateField()
    app = fields.StringField()
    country = fields.StringField()
    revenue = fields.Int32Field()

    engine = engines.MergeTree('date', ('app', 'country'))

    @classmethod
    def table_name(cls):
        return 'b_table'

    @classmethod
    def insert_rows(cls, db, rows):
        """
        :param db: clickhouse.Database object
        :param rows: [LIST] of dictionaries
            [
                { "date": "2017-08-22", "app": "Evernote", "country": "BY", "revenue": 78, },
            ]
        :return: [BOOL]
        """
        offset_id = cls.get_last_id(db) + 1
        data = [
            B(_id=i,
              date=row.get('date'),
              app=row.get('app'),
              country=row.get('country', 'ZZ'),
              revenue=row.get('revenue', 0)) for i, row in enumerate(rows, offset_id)]
        db.insert(data)

        return True
