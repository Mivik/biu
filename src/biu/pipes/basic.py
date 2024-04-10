from collections import deque
from rich import print

from ..main import unwrap, pipe, Stream, Record, FieldSet, get_field, Value

__all__ = [
    'echo',
    'where',
    'head',
    'tail',
    'select',
    'extend',
    'sort_by',
    'enumerated',
    'each',
    'eachi',
    'pretty',
    'wait',
    'tee',
    'first',
    'last',
    'flatten',
]


@pipe
def echo(s: Stream, msg=None):
    if msg is not None:
        return Value(msg)
    return s


def where(mp):
    return lambda s: s.update(
        iter=(x for x in iter(s) if unwrap(mp, x)), len=None
    )


@pipe
def head(s: Stream, n: int = 10):
    return s.update(iter=(x for _, x in zip(range(n), iter(s))), len=None)


@pipe
def tail(s: Stream, n: int = 10):
    return s.update(iter=deque(iter(s), maxlen=n), len=None)


def select(*field_names, **fields):
    def wrapper(s: Stream):
        def to_record(x):
            record = {field: get_field(x, field) for field in field_names}
            record.update(
                {field: unwrap(val, x) for field, val in fields.items()}
            )
            return Record(record)

        return s.update(
            map(to_record, iter(s)),
            fields=FieldSet(field_names + tuple(fields.keys())),
            is_table=True,
        )

    return wrapper


def extend(**fields):
    def wrapper(s: Stream):
        if not s.is_table:
            raise ValueError('extend can only be used on a table')

        def update(x):
            for field, val in fields.items():
                x[field] = unwrap(val, x)

            return x

        s.fields.extend(fields.keys())

        return s.update(map(update, iter(s)))

    return wrapper


@pipe
def sort_by(s: Stream, *fields):
    return s.update(
        sorted(
            iter(s), key=lambda x: tuple(unwrap(field, x) for field in fields)
        )
    )


@pipe
def enumerated(s: Stream, name: str = 'item'):
    return s.update(
        (Record({name: x}) for x in iter(s)),
        fields=FieldSet((name,)),
    )


def each(f, fields=None):
    return lambda s: s.update((f(x) for x in iter(s)), fields=fields)


def eachi(f, fields=None):
    return lambda s: s.update(
        (f(i, x) for i, x in enumerate(iter(s))), fields=fields
    )


def pretty(s: Stream):
    s.pretty = True
    return s


def wait(s: Stream):
    ls = list(iter(s))
    return s.update(ls, len=len(ls))


def tee(s: Stream):
    s = s | wait
    print(s)
    return s


def first(s: Stream):
    return s.update(value=next(iter(s)))


def last(s: Stream):
    return s.update(value=deque(iter(s), maxlen=1)[0])


def flatten(s: Stream):
    it = iter(s)
    return s.update((y for x in it for y in x), len=None, fields=None)
