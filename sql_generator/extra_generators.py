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

import os
import random
import string

from sql_generator.utils import Result, choices_as_string


def __read_resource(path):
    with open(path) as f:
        return f.readlines()


# https://github.com/imsky/wordlists, Licenced: Copyright MIT (c) 2017-2019 Ivan Malopinsky
base = os.path.dirname(os.path.abspath(__file__))
FIRSTS = __read_resource(base + "/resources/first.txt")
LASTS = __read_resource(base + "/resources/last.txt")


def _name_generator(is_first=True):
    source = FIRSTS if is_first else LASTS
    return random.choice(source).strip().capitalize()


def first_name_generator(_):
    """Generator for randomised first names."""
    return Result(_name_generator(is_first=True))


def last_name_generator(_):
    """Generator for randomised last names."""
    return Result(_name_generator(is_first=False))


def email_generator(_):
    """Generator for email addresses."""
    domain = choices_as_string(string.ascii_lowercase, k=5)
    tld = random.choice(("com", "net", "nl", "de", "co.uk"))
    name = choices_as_string(list(string.digits + string.ascii_lowercase), k=random.randint(4, 10))
    return Result(f"{name}@{domain}.{tld}")


def phone_generator(_):
    """Generator for phone number strings with plus prefixed country code."""
    numbers = choices_as_string(list(range(1, 9)), k=12)
    country = random.choice(["49", "53", "10", "11", "43"])
    return Result(f"+{country} {numbers}")
