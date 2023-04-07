#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
from queue import Queue

import models  # noqa
import pre_dump  # noqa
from database import DatabaseController
from factories import fuzzyTodayDate
from fuzzy_models import FuzzyModelV2, PATCH_COLUMNS
from utils import async_timeit


DO_QUEUE = Queue()
COMMIT_SIZE = 20 * 1000
PATCH_COLUMNS["*"].update({
    "last_modified": (fuzzyTodayDate, {})
})

runtime_db = DatabaseController(
    src_dsn='postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/complex_service',
    dst_dsn='postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/complex_service',
)
dst_session = runtime_db.new_session(runtime_db.dst_engine)

tables = runtime_db.get_all_tables()

tb_cpx = FuzzyModelV2("complex", engine=runtime_db.engine, tables=tables)
tb_sc = FuzzyModelV2("screen", engine=runtime_db.engine, tables=tables)
tb_device = FuzzyModelV2("device", engine=runtime_db.engine, tables=tables)

global_vars = {'organization_uuid': "59fec944-5b5e-49db-a5a8-2707bd973480"}

features = []


async def write_to_db(objs: list):
    if not objs:
        return

    with runtime_db.get_cursor(runtime_db.dst_engine) as cursor:
        cursor.execute(";".join(objs))
    await asyncio.sleep(0.1)


async def consumer():
    await asyncio.sleep(1)
    bucket = []

    while True:
        if not DO_QUEUE.empty():
            task = DO_QUEUE.get(timeout=2)
            bucket += list(task)

            if len(bucket) > COMMIT_SIZE:
                io_task = asyncio.create_task(write_to_db(bucket))
                await io_task
                bucket.clear()

        else:
            io_task = asyncio.create_task(write_to_db(bucket))
            await io_task
            break

        DO_QUEUE.task_done()


async def producer():
    for n in range(10000):
        cpx = tb_cpx.fuzzy_one(**global_vars)
        cpx_do = cpx.as_orm()

        screens = [tb_sc.fuzzy_one(**{
            "complex_uuid": cpx_do.uuid
        }) for _ in range(10)]

        devices = [tb_device.fuzzy_one(**{
            "screen_uuid": s.as_orm().uuid,
            "complex_uuid": cpx_do.uuid
        }) for s in screens]

        do_task = map(lambda x: x.as_sql(), [cpx] + screens + devices)
        DO_QUEUE.put(do_task)


@async_timeit
async def main():
    """
    data_size:21W  batch_size: 1W
    >>> 33.203601121902466 >>> 32.45762801170349
    data_size:21W  batch_size: 1.5W-2W-3W
    >>> 31.627033710479736 >>> 30.176783800125122 >>> 31.004827976226807

    python39 data_size:21W  batch_size: 3W
    >>> 33.33909511566162 >>> 29.5040020942688
    """
    await asyncio.gather(
        consumer(),
        producer(),
    )


if __name__ == '__main__':
    asyncio.run(main())
