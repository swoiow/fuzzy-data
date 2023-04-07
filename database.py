import warnings
from contextlib import contextmanager

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool


try:
    from psycopg2cffi import compat


    compat.register()
except ImportError:
    warnings.warn("missing psycopg2cffi")

Session = scoped_session(sessionmaker())


def init_db(dsn='postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/complex_service'):
    _engine = create_engine(
        dsn,
        pool_pre_ping=True,
        echo=False,
        pool_recycle=1800
    )
    session = Session()
    session.configure(bind=_engine)
    return _engine


def create_all():
    import models

    engine = init_db()
    models.Base.metadata.create_all(engine)


class DatabaseController(object):

    def __init__(self, src_dsn, dst_dsn=None):
        self.src_dsn = src_dsn
        self.engine = self.get_engine()

        self.dst_dsn = dst_dsn
        if self.dst_dsn:
            self.dst_engine = self.get_engine(self.dst_dsn)

    def get_engine(self, dsn=None):
        if not dsn:
            dsn = self.src_dsn

        eng = create_engine(
            dsn,
            pool_pre_ping=False,
            echo=False,
            pool_recycle=1800,
            client_encoding='utf8',
            poolclass=NullPool
            # psycopg2.extras.execute_batch mode
            # https://docs.sqlalchemy.org/en/13/core/tutorial.html#executing-multiple-statements

            # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html
            # use_batch_mode=True,  # deprecated! equal to `executemany_mode='batch'`
            # executemany_mode='values', executemany_values_page_size=10000, executemany_batch_page_size=500
        )
        eng.dispose()
        return eng

    def get_inspect(self):
        return inspect(self.engine)

    def get_all_tables(self):
        from models import DeclarativeBaseClass

        default_base = declarative_base(cls=DeclarativeBaseClass)

        table_mappings = automap_base(declarative_base=default_base)
        table_mappings.prepare(self.engine, reflect=True)

        return table_mappings.classes

    @contextmanager
    def new_session(self, engine: Engine):
        Session.configure(bind=engine)
        sess = Session()
        yield sess

    @contextmanager
    def get_cursor(self, engine: Engine):
        conn = engine.pool._creator()
        with conn:
            cursor = conn.cursor()
            yield cursor
            conn.commit()

    # def __call__(self, *args, **kwargs):
    #     return self.update_session()
