#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

from config import db, VALID_GROUP_BY_ARGS, TABLE_NAMES
from models import A, B
from utils import fill_db


# CREATE IF NOT EXISTS
db.create_table(A)
db.create_table(B)

# fill tables
fill_db.run(db, A, B)

query_select_template = """
SELECT
    {group_by}, users_sum, revenue_sum
FROM (
    SELECT
        {group_by}, sum(users) as users_sum
    FROM
        {t1}
    GROUP BY
        {group_by}
)

ALL FULL JOIN

(
    SELECT
        {group_by}, sum(revenue) as revenue_sum
    FROM
        {t2}
    GROUP BY
        {group_by}
)

USING {group_by}
"""


def select_group_by(group_by):

    # validate input
    if not all([i in VALID_GROUP_BY_ARGS for i in group_by]):
        group_by = ['date', 'app', 'country']

    # compose query
    query_select = query_select_template.format(group_by=", ".join(group_by), t1=TABLE_NAMES[0], t2=TABLE_NAMES[1])

    return db.select(query_select)


def print_selected_data(group_by, selected_data):
    for row in selected_data:
        keys = []
        keys.extend(group_by)
        keys.extend(['users_sum', 'revenue_sum'])
        print [row.__dict__.get(key) for key in keys]


if __name__ == '__main__':
    group_by = ['date', 'app', 'country']
    data = select_group_by(group_by)
    print_selected_data(group_by, data)
