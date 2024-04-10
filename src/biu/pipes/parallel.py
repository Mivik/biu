from ..main import unwrap, Stream

__all__ = ['parallel']


def parallel(mp, **kwargs):
    def wrapper(s: Stream):
        if s.is_value:
            return unwrap(mp, s.value)

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(**kwargs) as executor:
            return Stream(executor.map(unwrap(mp), iter(s)))

    return wrapper
