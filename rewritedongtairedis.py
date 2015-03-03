#!/usr/bin/env python
# coding=utf8

"""
向 ForumDongtaiStore 中添加数据
"""
from pyutil.program.conf2 import Conf
from ss_data.db import config_dongtai_readonly_session, DongTaiReadOnlySession
from ss_data.domain.dongtai.lib import get_dongtai_raw_redis
from ss_data.domain.forum.models import Talk
from ss_dongtai.dongtai_server.forum_dongtai_store import ForumDongtaiStore, get_dongtai_attr_from_db


config_dongtai_readonly_session(Conf('/opt/tiger/ss_conf/ss/db_dongtaidb.conf'))

redis_client = get_dongtai_raw_redis()

session = DongTaiReadOnlySession()

try:
    dongtais = session.query(
        Talk
    ).all()

    for dongtai in dongtais:
        store = ForumDongtaiStore(dongtai.forum_id, redis_client)
        attr = get_dongtai_attr_from_db(dongtai.id)
        store.add_dongtai(attr)
finally:
    session.close()
