from datetime import date, datetime, timezone
from random import choices, randint
from string import ascii_letters
from time import time
from typing import Generator
from uuid import uuid4

import sqlalchemy as sqla
import sqlalchemy.dialects.postgresql as pg


__all__ = [
    'FUZZY_MAPS',
    'fuzzyBool',
    'fuzzyDateTime',
    'fuzzyDict',
    'fuzzyEmail',
    'fuzzyEnum',
    'fuzzyEnumOne',
    'fuzzyInt',
    'fuzzyList',
    'fuzzyString',
    'fuzzyTimeStamp',
    'fuzzyUUID',
    'fuzzyUUIDStr'
]

ALPHABET = tuple(ascii_letters)
PRINTABLE = tuple("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
BOOL = [True, False]

TODAY = str(date.today())


def int_seed(size) -> Generator:
    """ 返回随机数，用于随机Enum的属灵。The seed for enum.
        :param size: the size of array
    """
    while True:
        yield int(time() % size)


def fuzzyEnumOne(bucket, **kwargs):
    """ 返回字符串 """
    return choices(bucket, k=1)[0]


def fuzzyEnum(bucket, size, **kwargs):
    """ 返回列表  """
    return set(choices(bucket, k=size))


def fuzzyBool(**kwargs):
    """  """
    return fuzzyEnumOne(BOOL)


def fuzzyInt(max=99, **kwargs):
    return randint(1, max)


def fuzzyBigInt(**kwargs):
    """  """
    return fuzzyInt(max=99999999)


def fuzzyTimeStamp(**kwargs):
    return time()


def fuzzyDateTime(**kwargs):
    return datetime.utcnow()


def fuzzyDateTimeWithTZ(**kwargs):
    return datetime.now(tz=timezone.utc)


def fuzzyTodayDate(**kwargs):
    return TODAY


def fuzzyString(size=6, **kwargs):
    return "".join(choices(ALPHABET, k=size))


def fuzzyUUID(**kwargs):
    return uuid4()


def fuzzyUUIDStr(**kwargs):
    return str(uuid4())


def fuzzyEmail(domain="localhost.dev", **kwargs):
    return next(fuzzyString()) + "@" + domain


def fuzzyDict(**kwargs):
    return {}


def fuzzyList(**kwargs):
    return []


def fuzzySet(**kwargs):
    return {}


FUZZY_MAPS = dict([
    ("BIGINT", fuzzyBigInt),
    ("ENUMS", fuzzyEnum),
    ("BOOLEAN", fuzzyBool),
    ("VARCHAR", fuzzyString),
    ("UUID", fuzzyUUIDStr),
    ("DATETIME", fuzzyDateTime),
    ("INTEGER", fuzzyInt),
    ("JSONB", fuzzyDict),
    ("TIMESTAMP", fuzzyTimeStamp),
    ("TIMESTAMPZ", fuzzyDateTimeWithTZ),
    ("DATE", fuzzyDateTime),

    (pg.UUID, fuzzyUUIDStr),
    (pg.JSONB, fuzzyDict),
    (pg.ARRAY, fuzzySet),
    (pg.TIMESTAMP, fuzzyTimeStamp),
    (pg.DOUBLE_PRECISION, fuzzyInt),
    (pg.ENUM, fuzzyEnum),

    (sqla.VARCHAR, fuzzyString),
    (sqla.INTEGER, fuzzyInt),
    (sqla.BIGINT, fuzzyBigInt),
    (sqla.BOOLEAN, fuzzyBool),
    (sqla.DATE, fuzzyDateTime),
    (sqla.DateTime, fuzzyDateTime),
    (sqla.String, fuzzyString),
    (sqla.Integer, fuzzyInt),
    (sqla.BigInteger, fuzzyInt),
    (sqla.Boolean, fuzzyBool),
    (sqla.Enum, fuzzyEnum),
])

if __name__ == '__main__':
    print()
