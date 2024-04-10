import sys

from biu import *  # noqa

maybe_stream = eval(sys.argv[1])
if isinstance(maybe_stream, Stream):
    maybe_stream | print
