# -*- coding: utf-8 -*-


t1_rows = [
    {'date': '2017-09-01', 'app': 'cs', 'country': 'BY', 'users': 5},
    {'date': '2017-09-01', 'app': 'cs', 'country': 'US', 'users': 7},
    {'date': '2017-09-02', 'app': 'wo', 'country': 'US', 'users': 3},
]

t2_rows = [
    {'date': '2017-09-01', 'app': 'cs', 'country': 'BY', 'revenue': 4030},
    {'date': '2017-09-01', 'app': 'cs', 'country': 'US', 'revenue': 1270},
    {'date': '2017-09-02', 'app': 'wo', 'country': 'CA', 'revenue': 7},
]


def run(db, model_t1, model_t2):
    model_t1.insert_rows(db, t1_rows)
    model_t2.insert_rows(db, t2_rows)
