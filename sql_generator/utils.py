"""
The MIT License (MIT)

Copyright (c) 2020 Nils T.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import random
import string
from typing import Callable

from sql_generator.analyser import Column


class Result:
    """Wrapper for data type and column generators."""
    __slots__ = ("raw", "result", "extra", "use_repr")

    def __init__(self, result, extra=None, use_repr=True):
        """
        :param result: The value to wrap.
        :param extra: Extra information about a value.
        :param use_repr: Whether __repr__ should be applied to the value.
        """
        self.raw = result
        self.result = repr(result) if use_repr else result
        self.extra = extra
        self.use_repr = use_repr

    def __repr__(self):
        return self.result


GEN_FUNC = Callable[[Column], Result]


def choices_as_string(seq, k=1):
    """Format random choices as a joined string."""
    return ''.join(map(str, random.choices(seq, k=k)))


def get_random_string(length):
    """Generate a random string from lowercase ascii letters."""
    letters = string.ascii_lowercase
    return ''.join(random.choices(letters, k=length))
