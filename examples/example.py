#!/usr/bin/env python
# -*- coding: utf-8 -*-

import models  # noqa
import pre_dump  # noqa
from constant import UNIT_10K
from factories import fuzzyTodayDate
from fuzzy_models import FuzzyModel, FuzzyModelV2, PATCH_COLUMNS
from utils import show_run_time, timeit


PATCH_COLUMNS["*"].update({
    "last_modified": (fuzzyTodayDate, {})
})


@show_run_time
def run1():
    # bucket = []
    fuzzy_cpl = FuzzyModel(models.Cpl)
    cpl = fuzzy_cpl.fuzzy_one().as_orm()

    # bucket.append(cpl)
    fuzzy_cpl_rating = FuzzyModel(models.CplRatings)

    for _ in range(UNIT_10K):
        cpl_rating = fuzzy_cpl_rating.fuzzy_one(**dict(cpl_uuid=cpl.uuid))
        # bucket.append(cpl_rating.as_orm())


@timeit
def create_cpls():
    from database import DatabaseController

    runtime_db = DatabaseController(
        src_dsn='postgresql+psycopg2://postgres:postgres@pgsql.lan:32100/cpl_service',
        dst_dsn='postgresql+psycopg2://postgres:postgres@pgsql.lan:32100/test_cpl_service',
    )
    dst_session = runtime_db.new_session(runtime_db.dst_engine)

    tables = runtime_db.get_all_tables()

    # V1
    # target_table = FuzzyModel(tables.cpl)

    # V2
    target_table = FuzzyModelV2("cpl", engine=runtime_db.engine, tables=tables)

    for _ in range(10):
        dst_session.bulk_save_objects([target_table.fuzzy_one().as_orm() for _ in range(100)])
    dst_session.commit()


@timeit
def create_cpl_with_locations():
    from database import DatabaseController

    runtime_db = DatabaseController(
        src_dsn='postgresql+psycopg2://postgres:postgres@pgsql.lan:32100/cpl_service',
        dst_dsn='postgresql+psycopg2://postgres:postgres@pgsql.lan:32100/test_cpl_service',
    )
    dst_session = runtime_db.new_session(runtime_db.dst_engine)

    tables = runtime_db.get_all_tables()

    tb_cpl = FuzzyModelV2("cpl", engine=runtime_db.engine, tables=tables)
    tb_cpl.fuzzy_one().as_sql()
    tb_cpl_location = FuzzyModelV2("cpl_location", engine=runtime_db.engine, tables=tables)

    db_bucket = []
    # 1 cpl : 10 cpl_location
    for _ in range(100000):
        cpl = tb_cpl.fuzzy_one()
        dst_session.execute(cpl.as_sql())
        dst_session.execute(";".join(
            [tb_cpl_location.fuzzy_one().as_sql(cpl_uuid=cpl.as_orm().uuid) for _ in range(10)]
        ))

    dst_session.commit()


@timeit
def create_complex_with_screen_device():
    """
    pypy37: create_complex_with_screen_device => 49.23895192146301 s
    python36: create_complex_with_screen_device => 10.673043966293335 s
    python38: create_complex_with_screen_device => 9.132057905197144 s

    """
    from database import DatabaseController

    runtime_db = DatabaseController(
        src_dsn='postgresql+psycopg2://postgres:postgres@pgsql.lan:32100/complex_service',
        dst_dsn='postgresql+psycopg2://postgres:postgres@pgsql.lan:32100/complex_service',
    )
    runtime_db = DatabaseController(
        src_dsn='postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/complex_service',
        dst_dsn='postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/complex_service',
    )
    dst_session = runtime_db.new_session(runtime_db.dst_engine)

    tables = runtime_db.get_all_tables()

    tb_cpx = FuzzyModelV2("complex", engine=runtime_db.engine, tables=tables)
    tb_sc = FuzzyModelV2("screen", engine=runtime_db.engine, tables=tables)
    tb_device = FuzzyModelV2("device", engine=runtime_db.engine, tables=tables)

    var = {'organization_uuid': "59fec944-5b5e-49db-a5a8-2707bd973480"}

    stmt = ""
    for n in range(3000):
        cpx = tb_cpx.fuzzy_one(**var)
        cpx_do = cpx.as_orm()

        screens = [tb_sc.fuzzy_one(**{
            "complex_uuid": cpx_do.uuid
        }) for _ in range(10)]

        devices = [tb_device.fuzzy_one(**{
            "screen_uuid": s.as_orm().uuid,
            "complex_uuid": cpx_do.uuid
        }) for s in screens]

        stmt += ";".join(map(lambda x: x.as_sql(), [cpx] + screens + devices))

    for cursor in runtime_db.get_cursor():
        cursor.execute(stmt)

    # sql = ";".join(map(lambda x: x.as_sql(), [cpx] + screens + devices))
    # stmt += sql + ";"

    # dst_session.execute(stmt)
    # dst_session.execute(map(lambda f: f.as_orm(), [cpx] + screens + devices))
    # dst_session.commit()


if __name__ == '__main__':
    create_complex_with_screen_device()
