import json
import lzma
import random

from qcportal.utils import hash_dict

letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def random_key():
    return "".join(random.choices(letters, k=3))


def random_string():
    return "".join(random.choices(letters, k=20))


def random_float():
    return random.random()


def random_int():
    return random.randint(-1000, 1000)


def random_list():
    return [random.random() for _ in range(15)]


def shuffle_dict(d):
    def shuffle_inner(x):
        a, b = x
        if isinstance(b, dict):
            b = shuffle_dict(b)
        return a, b

    d2 = [shuffle_inner(x) for x in d.items()]
    random.shuffle(d2)
    return dict(d2)


generated = {}


def random_dict():
    d = {}
    for i in range(5):
        key = "".join(random.choices(letters, k=3))
        value = "".join(random.choices(letters, k=20))
        d[random_key()] = random_string()
        d[random_key()] = random_int()
        d[random_key()] = random_float()
        d[random_key()] = random_list()


for n in range(10):
    d = {}
    for i in range(5):
        d[random_key()] = random_string()
        d[random_key()] = random_int()
        d[random_key()] = random_float()
        d[random_key()] = random_list()
        d[random_key()] = random_dict()

    h = hash_dict(d)
    generated[h] = shuffle_dict(d)

    # Shuffle it!
    for i in range(10):
        d2 = shuffle_dict(d)
        h2 = hash_dict(d2)
        assert h2 == h


with lzma.open("test_kw_hash.json.xz", "rt") as f:
    existing = json.load(f)

existing.update(generated)

with lzma.open("test_kw_hash.json-new.xz", "wt") as f:
    json.dump(existing, f, indent=2)
