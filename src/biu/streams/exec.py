import subprocess

from pydantic import BaseModel

from ..main import Stream, pipe


from typing import Optional


class CallOutput(BaseModel):
    stdout: str
    stderr: str
    status: int


def wrap(
    s: Stream,
    args,
    shell: bool,
    detailed: bool = False,
    checked: bool = True,
    timeout: Optional[float] = None,
    **kwargs,
):
    input = None
    if s.is_value:
        input = s.value
        if input is not None and not (
            isinstance(input, str) or isinstance(input, bytes)
        ):
            raise ValueError(
                f'input must be a string or bytes, got {type(input)}'
            )
    else:
        input = '\n'.join(iter(s))

    if isinstance(input, str):
        input = input.encode()

    if not shell:
        old_args = args
        args = []
        for arg in old_args:
            if isinstance(arg, str):
                args.append(arg)
            elif hasattr(arg, '__iter__'):
                args.extend(arg)
            else:
                args.append(str(arg))

    p = subprocess.Popen(
        args,
        **kwargs,
        shell=shell,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate(input, timeout=timeout)
    if checked and p.returncode != 0:
        raise Exception(
            f'command {args!r} returned non-zero exit code {p.returncode}'
        )

    if detailed:
        return Stream(
            value=CallOutput(
                stdout=out.decode(),
                stderr=err.decode(),
                status=p.returncode,
            ),
            fields=CallOutput,
        )
    else:
        return Stream(value=out.decode())


@pipe
def sh(s: Stream, cmd: str, **kwargs):
    return wrap(s, cmd, shell=True, **kwargs)


@pipe
def exec(s: Stream, *args, **kwargs):
    return wrap(s, args, shell=False, **kwargs)
