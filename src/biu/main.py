import functools

from pydantic import BaseModel
from rich.pretty import Pretty

from typing import (
    Any,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

T = TypeVar('T')

__all__ = [
    'get_field',
    'it',
    'FieldSet',
    'Map',
    'pipe',
    'Record',
    'stream',
    'Stream',
    'unwrap',
    'Sink',
    'Value',
    'sink',
    'alter_stream',
    'get',
    'chain',
    'table',
]


def unwrap(x, *args):
    if isinstance(x, Map):
        x = object.__getattribute__(x, 'f')
        if args:
            return x(args[0])

    return x


def get_field(x, name):
    try:
        return getattr(x, name)
    except AttributeError:
        pass

    return x[name]


def _fields(fields):
    if isinstance(fields, type) and issubclass(fields, BaseModel):
        return fields.model_fields.keys()
    return fields


class FieldSet:
    def __init__(self, fields):
        self._fields = dict.fromkeys(fields)

    def __iter__(self):
        return iter(self._fields)

    def add(self, field):
        self._fields[field] = None

    def extend(self, fields):
        self._fields.update(dict.fromkeys(fields))


class Record(dict):
    def __init__(self, arg, fields=None):
        fields = _fields(fields)

        if isinstance(arg, BaseModel):
            arg = arg.model_dump()
        elif not isinstance(arg, dict):
            if not fields:
                arg = arg.__dict__

        if fields:
            arg = {field: get_field(arg, field) for field in fields}

        super(Record, self).__init__(arg)
        self.__dict__ = self


class Sink:
    def __init__(self, value):
        self.value = value


class Value:
    def __init__(self, value):
        self.value = value


class alter_stream:
    def __init__(self, value, updates):
        self.value = value
        self.updates = updates


class chain:
    def __init__(self, value, chain):
        self.value = value
        self.chain = chain


def sink(iter):
    for _ in iter:
        pass


class Stream(Generic[T]):
    value: Iterator[T] = None
    is_value: bool = True
    is_table: bool = False
    fields: Optional[FieldSet] = None
    pretty: bool = False
    len: Optional[int] = None

    def __init__(self, iter: Iterator[T] = None, **kwargs):
        self.update(iter, **kwargs)

    def update(
        self,
        iter_or_value=None,
        iter: Iterator[T] = None,
        value: Optional[Any] = None,
        **kwargs,
    ) -> 'Stream[T]':
        if iter_or_value is not None:
            if self.is_value:
                value = next(iter_or_value)
                try:
                    next(iter_or_value)
                    raise ValueError(
                        'update() got multiple values while expecting one'
                    )
                except StopIteration:
                    pass
            else:
                iter = iter_or_value

        if iter:
            self.value = iter
            if hasattr(iter, '__len__'):
                len = iter.__len__()
                if len is not None:
                    self.len = len

            self.is_value = False

        if value is not None:
            self.value = value
            self.is_value = True
            self.is_table = isinstance(value, Record)

        try:
            fields = _fields(kwargs.pop('fields'))
            self.fields = FieldSet(fields) if fields else None
        except KeyError:
            pass

        self.__dict__.update(kwargs)
        return self

    def __iter__(self) -> Iterator[T]:
        if self.is_value:
            return iter((self.value,))
        else:
            return iter(self.value)

    def __len__(self):
        return 1 if self.is_value else self.len

    def __or__(self, pipe) -> Union['Stream[T]', Any]:
        if hasattr(pipe, '__pipe__'):
            resp = pipe.__pipe__(self)
        else:
            resp = pipe(self)

        updates = None
        if isinstance(resp, alter_stream):
            updates = resp.updates
            resp = resp.value

        next = None
        if isinstance(resp, chain):
            next = resp.chain
            resp = resp.value

        if pipe in (list, set, tuple):
            resp = Sink(resp)

        if isinstance(resp, Stream):
            self.value = resp.value
            self.is_value = resp.is_value
            self.fields = resp.fields
            self.pretty = resp.pretty
            self.len = resp.len
        elif isinstance(resp, Sink):
            if next:
                raise ValueError('chain after sink')
            return resp.value
        elif isinstance(resp, Value):
            self.value = resp.value
            self.is_value = True
        else:
            self.value = resp
            self.is_value = not hasattr(resp, '__next__')

        if updates:
            self.update(**updates)

        if next:
            return self | next

        return self

    def __rich__(self):
        from rich.table import Table

        table = Table()

        format = Pretty if self.pretty else str

        if not self.is_value:
            table.add_column('#')

        if self.fields:
            for field in self.fields:
                table.add_column(field)

            for i, v in enumerate(self):
                table.add_row(
                    *(() if self.is_value else (Pretty(i),)),
                    *(format(get_field(v, field)) for field in self.fields),
                )

        else:
            table.add_column('item')

            for i, v in enumerate(self):
                table.add_row(
                    *(() if self.is_value else (Pretty(i),)), format(v)
                )

        return table


def get(s: Stream):
    if not s.is_value:
        raise ValueError('can only get on a single value')

    return Sink(s.value)


class Map:
    def __init__(self, f=lambda v: v):
        self.f = f

    @staticmethod
    def chain(*maps) -> 'Map':
        maps = [unwrap(mp) for mp in maps]
        return Map(lambda v: functools.reduce(lambda x, f: f(x), maps, v))

    def __getattribute__(self, name):
        if name == '__pipe__':
            return object.__getattribute__(self, name)

        return Map(lambda v: getattr(unwrap(self, v), unwrap(name, v)))

    def __getitem__(self, key):
        return Map(lambda v: unwrap(self, v)[unwrap(key, v)])

    def __call__(self, *args, **kwargs):
        return Map(
            lambda v: unwrap(self, v)(
                *(unwrap(x, v) for x in args),
                **{key: unwrap(val, v) for key, val in kwargs.items()},
            )
        )

    def __add__(self, other):
        return Map(lambda v: unwrap(self, v) + unwrap(other, v))

    def __radd__(self, other):
        return Map(lambda v: unwrap(other, v) + unwrap(self, v))

    def __sub__(self, other):
        return Map(lambda v: unwrap(self, v) - unwrap(other, v))

    def __rsub__(self, other):
        return Map(lambda v: unwrap(other, v) - unwrap(self, v))

    def __mul__(self, other):
        return Map(lambda v: unwrap(self, v) * unwrap(other, v))

    def __rmul__(self, other):
        return Map(lambda v: unwrap(other, v) * unwrap(self, v))

    def __truediv__(self, other):
        return Map(lambda v: unwrap(self, v) / unwrap(other, v))

    def __rtruediv__(self, other):
        return Map(lambda v: unwrap(other, v) / unwrap(self, v))

    def __floordiv__(self, other):
        return Map(lambda v: unwrap(self, v) // unwrap(other, v))

    def __rfloordiv__(self, other):
        return Map(lambda v: unwrap(other, v) // unwrap(self, v))

    def __mod__(self, other):
        return Map(lambda v: unwrap(self, v) % unwrap(other, v))

    def __rmod__(self, other):
        return Map(lambda v: unwrap(other, v) % unwrap(self, v))

    def __pow__(self, other):
        return Map(lambda v: unwrap(self, v) ** unwrap(other, v))

    def __rpow__(self, other):
        return Map(lambda v: unwrap(other, v) ** unwrap(self, v))

    def __lshift__(self, other):
        return Map(lambda v: unwrap(self, v) << unwrap(other, v))

    def __rlshift__(self, other):
        return Map(lambda v: unwrap(other, v) << unwrap(self, v))

    def __rshift__(self, other):
        return Map(lambda v: unwrap(self, v) >> unwrap(other, v))

    def __rrshift__(self, other):
        return Map(lambda v: unwrap(other, v) >> unwrap(self, v))

    def __and__(self, other):
        return Map(lambda v: unwrap(self, v) & unwrap(other, v))

    def __rand__(self, other):
        return Map(lambda v: unwrap(other, v) & unwrap(self, v))

    def __xor__(self, other):
        return Map(lambda v: unwrap(self, v) ^ unwrap(other, v))

    def __rxor__(self, other):
        return Map(lambda v: unwrap(other, v) ^ unwrap(self, v))

    def __or__(self, other):
        return Map(lambda v: unwrap(self, v) | unwrap(other, v))

    def __ror__(self, other):
        return Map(lambda v: unwrap(other, v) | unwrap(self, v))

    def __neg__(self):
        return Map(lambda v: -unwrap(self, v))

    def __pos__(self):
        return Map(lambda v: +unwrap(self, v))

    def __abs__(self):
        return Map(lambda v: abs(unwrap(self, v)))

    def __invert__(self):
        return Map(lambda v: ~unwrap(self, v))

    def __lt__(self, other):
        return Map(lambda v: unwrap(self, v) < unwrap(other, v))

    def __le__(self, other):
        return Map(lambda v: unwrap(self, v) <= unwrap(other, v))

    def __eq__(self, other):
        return Map(lambda v: unwrap(self, v) == unwrap(other, v))

    def __ne__(self, other):
        return Map(lambda v: unwrap(self, v) != unwrap(other, v))

    def __gt__(self, other):
        return Map(lambda v: unwrap(self, v) > unwrap(other, v))

    def __ge__(self, other):
        return Map(lambda v: unwrap(self, v) >= unwrap(other, v))

    def __pipe__(self, stream: Stream) -> Stream:
        f = object.__getattribute__(self, 'f')
        if stream.is_value:
            return Stream(value=f(stream.value))
        else:
            return Stream(f(x) for x in stream)


it = Map()


class PipeWrapper:
    def __init__(self, f, fields=None, input=True, sink=False, value=False):
        if sink and value:
            raise ValueError('sink and value are mutually exclusive')

        self.f = f
        self.fields = fields
        self.input = input
        self.sink = sink
        self.value = value

    def __call__(self, *args, **kwargs):
        return PipeWrapper(
            lambda s, *args2, **kwargs2: self.f(
                s, *args, *args2, **kwargs, **kwargs2
            )
        )

    def __or__(self, pipe):
        return Stream() | self | pipe

    def __pipe__(self, stream: Stream):
        resp = self.f(stream) if self.input else self.f()
        if self.sink:
            return Sink(resp)
        if self.value:
            return Value(resp)

        if self.fields:
            resp = chain(resp, table(self.fields))

        return resp


def pipe(f=None, /, **kwargs):
    def wrapper(f, kwargs):
        wrp = PipeWrapper(f, **kwargs)
        functools.update_wrapper(wrp, f)
        return wrp

    if kwargs:
        return lambda f: wrapper(f, kwargs)
    else:
        return wrapper(f, {})


class stream:
    def __call__(self, iter: Iterator[T]) -> Stream[T]:
        return Stream[T](iter)

    def __ror__(self, iter: Iterator[T]) -> Stream[T]:
        return Stream[T](iter)


@pipe
def table(s: Stream, fields=None):
    if s.is_table:
        return s

    return s.update(
        (Record(x, fields) for x in iter(s)), fields=fields, is_table=True
    )
