import sys

from biu import *  # noqa
from biu.main import PipeWrapper

maybe_stream = eval(sys.argv[1])

if isinstance(maybe_stream, Stream) or isinstance(maybe_stream, PipeWrapper):
    maybe_stream | print
else:
    print(maybe_stream)
