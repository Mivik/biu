from tqdm import tqdm

from ..main import Stream

__all__ = ['pv']


def pv(s: Stream):
    return s.update(tqdm(iter(s), total=s.len))
