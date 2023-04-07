from collections import defaultdict

from cachetools import cached, MRUCache
from sqlalchemy import inspect

from app_logger import factory_logger
from constant import DatabaseTYPE
from factories import FUZZY_MAPS, fuzzyEnum, fuzzyEnumOne
from models import Base


# PATCH_COLUMNS = {
#   table_name: {
#       column: (fuzzyObject, kwargs)
#   }
# }
PATCH_COLUMNS = defaultdict(dict)


def format_array_as_sql(value):
    return "'{%s}'" % ",".join([f'"{i}"' for i in value])


class FuzzyData(object):
    INSERT_SQL = r"""INSERT INTO {table_name} ({columns}) VALUES ({values});"""
    __slots__ = ("_data_", "_model_")

    def __init__(self, data: tuple, model=None):
        self._data_ = data
        self._model_ = model

    @cached(MRUCache(maxsize=256))
    def as_dict(self, **kwargs):
        return {**dict(self._data_), **kwargs}

    @cached(MRUCache(maxsize=256))
    def as_orm(self, **kwargs):
        return self._model_(**self.as_dict(**kwargs))

    def as_row(self, **kwargs):
        return tuple(k for k, v in self._data_), tuple(kwargs.get(k, v) for k, v in self._data_)

    def as_sql(self, **kwargs):
        columns, values = self.as_row(**kwargs)
        values = tuple(format_array_as_sql(v) if isinstance(v, (set, list, dict)) else f"'{v}'" for v in values)
        return self.INSERT_SQL.format(
            table_name=self._model_.__name__, columns=",".join(columns), values=",".join(values)
        )

    def __call__(self, *args, **kwargs):
        return self._data_

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self._model_.__name__} at {hex(id(self))}>"


class FuzzyModel(object):
    __slots__ = ("_model", "_mapping")

    def __init__(self, model: Base):
        self._mapping = []
        self._model = model

        self._init_fuzzy_class()

    @property
    def fuzzy_mapping(self):
        return tuple(self._mapping)

    def fuzzy_one(self, **kwargs) -> tuple or Base:
        """ 生成一个model的测试数据。

            kwargs: 传入所需的默认值或外键
        """
        return FuzzyData(tuple([
            (name, kwargs[name] if name in kwargs else fc(**kw))
            for name, fc, kw in self._mapping
        ]), self._model)

    def _get_column_and_type(self):
        mapper = inspect(self._model)
        for c in mapper.columns:
            name = c.name
            column_type = c.type
            foreign_keys = c.foreign_keys

            length = getattr(column_type, "length", None)
            enums = getattr(column_type, "enums", None)
            if enums:
                column_type = DatabaseTYPE.ENUMS

            if hasattr(column_type, "timezone"):
                column_type = DatabaseTYPE.DT if column_type.timezone else column_type.python_type.__name__.upper()

            factory_logger.debug("%s %s %s %s %s", name, column_type, foreign_keys, length, enums)
            yield name, column_type, foreign_keys, length, enums

    def _init_fuzzy_class(self):
        """ 根据 model 生成，model字段对应的fuzzy字段 """
        for name, column_type, foreign_keys, length, enums in self._get_column_and_type():
            column_type = str(column_type).split("(")[0]
            column_type = str(column_type).split("[")[0]

            fuzzier = FUZZY_MAPS[column_type]
            kwargs = {}

            if enums:
                kwargs["bucket"] = enums
                kwargs["size"] = 1  # TODO: 根据enums的数量选择enums函数
                fuzzier = fuzzyEnumOne if kwargs["size"] == 1 else fuzzyEnum

            elif length:
                kwargs["max"] = length
                kwargs["size"] = length < 100 and length or 32

            # 組合字段對應關係
            self._mapping.append(
                (name, fuzzier, kwargs)
            )

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self._model.__name__} at {hex(id(self))}>"


class FuzzyModelV2(FuzzyModel):

    def __init__(self, table_name: str, engine, tables):
        self._mapping = []
        self.input_table_name = table_name
        self._model = getattr(tables, self.input_table_name)

        self.insp = inspect(engine)

        self._init_fuzzy_class()

    def _init_fuzzy_class(self):
        for column in self.insp.get_columns(self.input_table_name):
            name = column['name']

            if name in PATCH_COLUMNS["*"]:
                fuzzier, kwargs = PATCH_COLUMNS["*"][name]

            elif name in PATCH_COLUMNS[self.input_table_name]:
                fuzzier, kwargs = PATCH_COLUMNS[self.input_table_name][name]

            else:
                column_type = column["type"]

                fuzzier = FUZZY_MAPS[type(column_type)]
                kwargs = {}

                if getattr(column_type, "enums", None):
                    kwargs["bucket"] = column_type.enums
                    kwargs["size"] = len(column_type.enums) // 2
                    fuzzier = fuzzyEnumOne if kwargs["size"] == 1 else fuzzyEnum

                elif getattr(column_type, "length", None):
                    length = column_type.length
                    kwargs["max"] = length
                    kwargs["size"] = length < 100 and length or 32

            self._mapping.append(
                (name, fuzzier, kwargs)
            )
