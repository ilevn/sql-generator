import random
import string
import time

from argon2 import PasswordHasher

FIRSTS = open("first.txt").readlines()
LASTS = open("last.txt").readlines()
PW_HASHER = PasswordHasher()
PRINTABLE = list(string.ascii_letters + string.digits + string.punctuation)


def _choices_str(seq, k=1):
    return ''.join(map(str, random.choices(seq, k=k)))


def str_time_prop(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def date_generator(_):
    # TODO: Events future
    start = "01/01/1999"
    end = "01/01/2020"
    return str_time_prop(start, end, '%d/%m/%Y', random.random())


def _get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


def text_generator(column):
    length = column.character_maximum_length
    return _get_random_string(length or random.randint(60, 300))


def _name_generator(is_first=True):
    if is_first:
        source = FIRSTS
    else:
        source = LASTS

    return random.choice(source).strip().capitalize()


def first_name_generator(_):
    return _name_generator(is_first=True)


def last_name_generator(_):
    return _name_generator(is_first=False)


def integer_generator(column):
    limit = 32767 if column.data_type == "smallint" else 2147483646  # postgres limit.
    return random.randint(1, limit)


def password_generator(_):
    random.shuffle(PRINTABLE)
    random_password = ''.join(_choices_str(PRINTABLE, k=random.randint(6, 20)))
    return PW_HASHER.hash(random_password)


def email_generator(column):
    is_staff = column.table_name in ("moderator", "admin")
    domain = "tickify" if is_staff else _choices_str(string.ascii_lowercase, k=5)
    tld = random.choice(("com", "net", "nl", "de", "co.uk"))
    name = _choices_str(list(string.digits + string.ascii_lowercase), k=random.randint(4, 10))
    return f"{name}@{domain}.{tld}"


def phone_generator(_):
    numbers = _choices_str(list(range(1, 9)), k=12)
    country = random.choice(["49", "53", "10", "11", "43"])
    return f"+{country} {numbers}"


def boolean_generator(_):
    return bool(random.choice((1, 0)))


def get_converter(t):
    return globals()[t + "_generator"]
