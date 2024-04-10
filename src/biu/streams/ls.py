import os

from datetime import datetime
from pathlib import Path
from pydantic import BaseModel

from ..main import pipe


class ListDirEntry(BaseModel):
    path: Path
    name: str
    type: str
    size: int
    modified: datetime


@pipe(input=False, fields=ListDirEntry)
def ls(path: str = '.'):
    for entry in os.scandir(path):
        stat = os.stat(entry.path)
        if entry.is_file():
            type = 'file'
        elif entry.is_dir():
            type = 'dir'
        elif entry.is_symlink():
            type = 'symlink'
        else:
            type = 'unknown'

        yield ListDirEntry(
            path=Path(entry.path),
            name=entry.name,
            type=type,
            size=stat.st_size,
            modified=datetime.fromtimestamp(stat.st_mtime),
        )
