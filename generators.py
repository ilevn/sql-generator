import random
import string
import time

from argon2 import PasswordHasher

# https://github.com/imsky/wordlists, Licenced: Copyright (c) 2017-2019 Ivan Malopinsky
FIRSTS = open("first.txt").readlines()
LASTS = open("last.txt").readlines()

PW_HASHER = PasswordHasher()
PRINTABLE = list(string.ascii_letters + string.digits + string.punctuation)


class Result:
    def __init__(self, result, extra=None):
        self.result = result
        self.extra = extra

    def __repr__(self):
        return repr(self.result)


def _choices_str(seq, k=1):
    return ''.join(map(str, random.choices(seq, k=k)))


def str_time_prop(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def date_generator(_):
    # TODO: Events are usually scheduled in the future.
    start = "1999-01-01"
    end = "2020-01-01"
    return Result(str_time_prop(start, end, '%Y-%m-%d', random.random()))


def _get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


def text_generator(column):
    if length := column.character_maximum_length:
        length = random.randint(1, length)
    else:
        length = random.randint(60, 300)

    return _get_random_string(length)


def _name_generator(is_first=True):
    if is_first:
        source = FIRSTS
    else:
        source = LASTS

    return random.choice(source).strip().capitalize()


def first_name_generator(_):
    return Result(_name_generator(is_first=True))


def last_name_generator(_):
    return Result(_name_generator(is_first=False))


def integer_generator(column):
    limit = 32767 if column.data_type == "smallint" else 2147483646  # postgres limit.
    return Result(random.randint(1, limit))


# aliases
smallint_generator = integer_generator
numeric_generator = integer_generator


def password_generator(_):
    random.shuffle(PRINTABLE)
    random_password = ''.join(_choices_str(PRINTABLE, k=random.randint(6, 20)))
    # Return the clear-text password as well.
    return Result(PW_HASHER.hash(random_password), random_password)


def email_generator(column):
    is_staff = column.table_name in ("moderator", "admin")
    domain = "tickify" if is_staff else _choices_str(string.ascii_lowercase, k=5)
    tld = random.choice(("com", "net", "nl", "de", "co.uk"))
    name = _choices_str(list(string.digits + string.ascii_lowercase), k=random.randint(4, 10))
    return Result(f"{name}@{domain}.{tld}")


def phone_generator(_):
    numbers = _choices_str(list(range(1, 9)), k=12)
    country = random.choice(["49", "53", "10", "11", "43"])
    return Result(f"+{country} {numbers}")


def boolean_generator(_):
    return Result(random.choice(("TRUE", "FALSE")))


def get_converter(t):
    return globals()[t + "_generator"]
