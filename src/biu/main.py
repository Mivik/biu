import functools

from dataclasses import dataclass
from pydantic import BaseModel
from rich.pretty import Pretty

from typing import (
    Any,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Union,
    overload,
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


def _to_fields(fields):
    if fields is None:
        return None

    if isinstance(fields, FieldSet):
        return fields

    if isinstance(fields, type) and issubclass(fields, BaseModel):
        return FieldSet(fields.model_fields.keys())

    return FieldSet(fields)


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
        fields = _to_fields(fields)

        if isinstance(arg, BaseModel):
            arg = arg.model_dump()
        elif not isinstance(arg, dict):
            if not fields:
                arg = arg.__dict__

        if fields:
            arg = {field: get_field(arg, field) for field in fields}

        super(Record, self).__init__(arg)
        self.__dict__ = self


class StreamOp:
    pass


class Sink(StreamOp):
    def __init__(self, value):
        self.value = value


class Value(StreamOp):
    def __init__(self, value):
        self.value = value


class alter_stream(StreamOp):
    def __init__(self, value, **updates):
        self.value = value
        self.updates = updates


class chain(StreamOp):
    def __init__(self, value, chain):
        self.value = value
        self.chain = chain


def sink(iter):
    for _ in iter:
        pass


class Stream(Generic[T]):
    EMPTY: 'Stream'

    item: Any
    is_value: bool
    is_table: bool
    pretty: bool
    len: Optional[int]

    @property
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, fields):
        self._fields = _to_fields(fields)

    def __init__(
        self,
        item: Any,
        is_value=False,
        is_table=False,
        fields: Optional[Any] = None,
        pretty=False,
        len=None,
    ):
        self.item = item
        self.is_value = is_value
        self.is_table = is_table
        self.fields = fields
        self.pretty = pretty
        self.len = len

    def copy(self) -> 'Stream[T]':
        return Stream(
            self.item,
            is_value=self.is_value,
            is_table=self.is_table,
            fields=self.fields,
            pretty=self.pretty,
            len=self.len,
        )

    def update(
        self,
        any: Optional[Any] = None,
        /,
        iter: Iterator[T] = None,
        value: Optional[Any] = None,
        **kwargs,
    ) -> 'Stream[T]':
        if any is not None:
            if iter is not None or value is not None:
                raise ValueError('only one of iter, value, or any can be used')

            # TODO: why pylance says this is inaccessible?
            if self.is_value:
                value = next(any)
                try:
                    next(any)
                    raise ValueError(
                        'update() got multiple values while expecting one'
                    )
                except StopIteration:
                    pass
            else:
                iter = any

        if value is not None and iter is not None:
            raise ValueError('only one of iter or value can be used')

        s = self.copy()
        if iter is not None:
            s.item = iter
            if hasattr(iter, '__len__'):
                len = iter.__len__()
                if len is not None:
                    s.len = len

            s.is_value = False

        elif value is not None:
            s.item = value
            s.is_value = True
            s.is_table = isinstance(value, Record)
            s.len = None

        if kwargs:
            for key, value in kwargs.items():
                setattr(s, key, value)

        return s

    def __iter__(self) -> Iterator[T]:
        if self.is_value:
            return iter((self.item,))
        else:
            return iter(self.item)

    def __len__(self):
        return 1 if self.is_value else self.len

    def __or__(self, pipe) -> Union['Stream[T]', Any]:
        if hasattr(pipe, '__pipe__'):
            resp = pipe.__pipe__(self)
        else:
            resp = pipe(self)

        updates = None
        while isinstance(resp, alter_stream):
            resp.updates.update(updates or {})
            updates = resp.updates
            resp = resp.value

        next = None
        if isinstance(resp, chain):
            next = resp.chain
            resp = resp.value

        if pipe in (list, set, tuple):
            resp = Sink(resp)

        if isinstance(resp, Stream):
            s = resp
        else:
            s = self.copy()
            if isinstance(resp, Sink):
                if next:
                    raise ValueError('chain after sink')
                return resp.value
            elif isinstance(resp, Value):
                s.item = resp.value
                s.is_value = True
            else:
                s.item = resp
                s.is_value = not hasattr(resp, '__next__')

        if updates:
            s = s.update(**updates)

        if next:
            return s | next

        return s

    def __rich__(self):
        from rich.table import Table

        if self.is_value:
            return Pretty(self.item)

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


Stream.EMPTY = Stream(None, is_value=True)


def get(s: Stream):
    if not s.is_value:
        raise ValueError('can only get on a single value')

    return Sink(s.item)


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
        return stream.update((f(x) for x in stream), fields=None)


it = Map()


@dataclass
class PipeOptions:
    fields: Optional[Any] = None
    input: bool = True
    sink: bool = False
    value: bool = False

    def __post_init__(self):
        if self.sink and self.value:
            raise ValueError('sink and value are mutually exclusive')


class PipeWrapper:
    def __init__(self, f, options: PipeOptions):
        self.f = f
        self.options = options

    def __call__(self, *args, **kwargs):
        return PipeWrapper(
            (
                (
                    lambda s, *args2, **kwargs2: self.f(
                        s, *args, *args2, **kwargs, **kwargs2
                    )
                )
                if self.options.input
                else lambda *args2, **kwargs2: self.f(
                    *args, *args2, **kwargs, **kwargs2
                )
            ),
            self.options,
        )

    def __or__(self, pipe):
        if isinstance(pipe, StreamHelper):
            return pipe.__ror__(self)

        return Stream.EMPTY | self | pipe

    def __pipe__(self, stream: Stream):
        resp = self.f(stream) if self.options.input else self.f()
        if self.options.sink:
            return Sink(resp)
        if self.options.value:
            return Value(resp)

        if self.options.fields:
            resp = chain(resp, table(self.options.fields))

        return resp


def pipe(f=None, /, **kwargs):
    def wrapper(f, kwargs):
        wrp = PipeWrapper(f, PipeOptions(**kwargs))
        functools.update_wrapper(wrp, f)
        return wrp

    if kwargs:
        return lambda f: wrapper(f, kwargs)
    else:
        return wrapper(f, {})


class StreamHelper:
    def __call__(self, iter: Iterator[T]) -> Stream[T]:
        return Stream[T](iter)

    @overload
    def __ror__(self, iter: Iterator[T]) -> Stream[T]: ...
    @overload
    def __ror__(self, wrapper: PipeWrapper) -> Stream[T]: ...

    def __ror__(self, x):
        if isinstance(x, StreamOp):
            return Stream.EMPTY | (lambda _: x)
        if isinstance(x, PipeWrapper):
            return Stream.EMPTY | x

        return Stream(x)


stream = StreamHelper()


@pipe
def table(s: Stream, fields=None):
    if s.is_table:
        return s

    return s.update(
        (Record(x, fields) for x in iter(s)), fields=fields, is_table=True
    )
