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
import time
from uuid import uuid4

from sql_generator.utils import Result, get_random_string


def _time_prop(start, end, prop):
    ptime = start + prop * (end - start)
    return ptime


def _time_const_generator(fmt, start=0, end=None):
    start = time.mktime(time.localtime(start))
    end = end or time.time()
    ptime = _time_prop(start, end, random.random())
    return time.strftime(fmt, time.localtime(ptime))


def _generate_integer(lower, upper):
    return random.randint(lower, upper)


def date_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-datetime.html"""
    return Result(_time_const_generator(fmt="%Y-%m-%d"))


def time_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-datetime.html"""
    return Result(_time_const_generator(fmt="%H:%M:%S"))


def timestamp_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-datetime.html"""
    return Result(_time_const_generator(fmt="%Y-%m-%d %H:%M:%S"))


def interval_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-datetime.html"""
    # We're generating the year separately.
    year = f"{random.randint(1, 100)} years"
    value = _time_const_generator(" %m months %d days %H hours %M minutes %S seconds")
    return Result(year + value.replace(" 0", " "))


def text_generator(column):
    """Generator for https://www.postgresql.org/docs/current/datatype-character.html"""
    if length := column.max_length:
        length = random.randint(1, length)
    else:
        length = random.randint(60, 300)

    return Result(get_random_string(length))


def smallint_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-numeric.html"""
    return Result(_generate_integer(-32768, 32767))


def integer_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-numeric.html"""
    return Result(_generate_integer(-2147483648, 2147483647))


def bigint_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-numeric.html"""
    return Result(_generate_integer(-9223372036854775808, 9223372036854775807))


def numeric_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-numeric.html"""
    # Technically NUMERIC supports up to 131072 digits past the decimal point.
    return Result(f'{random.randint(0, 1_000_000_000):d}.{random.randint(0, 10000000):d}')


def money_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-money.html"""
    return Result(_generate_integer(-92233720368547758.08, 92233720368547758.07))


def bit_generator(column):
    """Generator for https://www.postgresql.org/docs/current/datatype-bit.html"""
    length = column.max_length
    if column.data_type == "bit varying":
        # Length only has an upper bound.
        length = random.randint(1, length)

    value = f"B'{random.getrandbits(length):0{length}b}'"
    return Result(value, use_repr=False)


def uuid_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-uuid.html"""
    return Result(str(uuid4()))


def boolean_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-boolean.html"""
    return Result(random.choice(("TRUE", "FALSE")), use_repr=False)


def bytea_generator(_):
    """Generator for https://www.postgresql.org/docs/current/datatype-binary.html"""
    value = fr"'\x{''.join(f'{i:02x}' for i in random.randbytes(10))}'"
    return Result(value, use_repr=False)


def array_generator(column):
    """
    Generator for https://www.postgresql.org/docs/current/arrays.html

    Note: This only offers partial support for arrays
    due to type inference issues.
    """
    # This is not fully supported because postgresql's arrays
    # are horrible (no type inference for N-dim arrays).
    d_type = column.udt_name.lower()[:-2]
    if d_type == "character":
        # Problematic, because character arrays are capped at 1 char each.
        generator = lambda _: Result(get_random_string(1))
    else:
        try:
            generator = get_generator(d_type)
        except KeyError:
            return Result("{}")

    # Mangle the data-type for the pass-through func.
    column.data_type = d_type
    elements = "{" + ", ".join(f"\"{generator(column).raw}\"" for _ in range(random.randint(1, 20))) + "}"
    return Result(elements)


# aliases
character_generator = text_generator
globals()["character varying_generator"] = text_generator
globals()["timestamp without time zone_generator"] = timestamp_generator
globals()["time without time zone_generator"] = time_generator
globals()["bit varying_generator"] = bit_generator


def get_generator(t):
    return globals()[t + "_generator"]
