"""
This tests the Mongoenegine performance when handling Results and procedures

"""

import itertools
import mongoengine as db
from time import time
import pymongo
from pymongo import MongoClient
import datetime

reset_db = False

db_name = "bench_qc_mongoengine"
db_client = db.connect(db_name)
if reset_db:
    db_client.drop_database(db_name)

# mongoengine_socket = MongoengineSocket("mongodb://localhost", db_name)

n_tasks = 10000
n_query = 200


class Task(db.DynamicDocument):

    spec = db.DynamicField()

    # others
    tag = db.StringField(default=None)
    program = db.StringField(default=None)
    procedure = db.StringField(default=None)
    created_on = db.DateTimeField(required=True)
    status = db.StringField(default=None)
    priority = db.IntField(default=None)

    def save(self, *args, **kwargs):
        """Override save to update modified_on"""
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(Task, self).save(*args, **kwargs)

    meta = {
        "collection": "task_queue",
        "indexes": [
            "created_on",
            "status",
            "priority",
            "tag",
            "program",
            "procedure",
            {"fields": ("program", "procedure"), "unique": False},
        ],
    }


def insert_task(n_tasks):
    # add Molecules
    ncombo = 3

    tags = ["tag" + str(x) for x in range(ncombo)] + [None]
    program = ["prog" + str(x) for x in range(ncombo)]
    procedure = ["proc" + str(x) for x in range(ncombo)] + [None]
    status = ["I", "C"]
    priority = [1, 2, 3]

    combos = list(itertools.product(tags, program, procedure, status, priority))

    nloops = int(n_tasks / len(combos)) + 1
    for n in range(nloops):
        for c in combos:
            Task(spec={}, tag=c[0], program=c[1], procedure=c[2], status=c[3], priority=c[4]).save()

    # for x in range(n_tasks):

    return nloops * len(combos)


def get_queue():
    found = TaskQueueORM.objects(**query).limit(limit).order_by("created_on")


def bench():
    ninsert = insert_task(1)

    if reset_db:
        tstart = time()
        ninsert = insert_task(n_tasks)
        dtime = (time() - tstart) * 1000  # msec
        print("{} Task inserted in an avg {:0.3f} ms / doc".format(ninsert, dtime / ninsert))

    tstart = time()
    query = {"procedure__in": ["proc0"], "program__in": ["prog0"]}
    nquery = Task.objects(**query).limit(n_query).order_by("created_on", "-priority")
    cnt = nquery.to_json()
    print(len(cnt))
    nquery = nquery.count()

    dtime = (time() - tstart) * 1000  # msec
    print("{} Task queried in an avg {:0.3f} ms / doc".format(nquery, dtime / nquery))
    print("Total time {}".format(dtime))


if __name__ == "__main__":
    bench()
