"""
Queue adapter for Fireworks
"""

import logging


class FireworksAdapter:
    def __init__(self, lpad, logger=None):
        """
        Parameters
        ----------
        lpad : fireworks.LaunchPad
            A activte Fireworks LaunchPad
        logger : None, optional
            A optional logging object to write output to
        """

        self.lpad = lpad
        self.queue = {}
        self.logger = logger or logging.getLogger('FireworksAdapter')

    def submit_tasks(self, tasks):
        ret = []

        import fireworks
        for task in tasks:
            tag = task["id"]

            fw = fireworks.Firework(
                fireworks.PyTask(
                    func=task["spec"]["function"],
                    args=task["spec"]["args"],
                    kwargs=task["spec"]["kwargs"],
                    stored_data_varname="fw_results"),
                spec={"_launch_dir": "/tmp/"})
            launches = self.lpad.add_wf(fw)

            self.queue[list(launches.values())[0]] = (tag, task["parser"], task["hooks"])
            ret.append(tag)

        return ret

    def aquire_complete(self):
        ret = {}

        # Pull out completed results that match our queue ids
        cursor = self.lpad.launches.find({
            "fw_id": {
                "$in": list(self.queue.keys())
            },
            "state": {
                "$in": ["COMPLETED", "FIZZLED"]
            },
        }, {
            "action.stored_data.fw_results": True,
            "action.stored_data._task.args": True,
            "action.stored_data._exception": True,
            "_id": False,
            "fw_id": True,
            "state": True
        })

        for tmp_data in cursor:
            key, parser, hooks = self.queue.pop(tmp_data["fw_id"])
            if tmp_data["state"] == "COMPLETED":
                ret[key] = (tmp_data["action"]["stored_data"]["fw_results"], parser, hooks)
            else:
                blob = tmp_data["action"]["stored_data"]["_task"]["args"][0]
                msg = tmp_data["action"]["stored_data"]["_exception"]["_stacktrace"]
                blob["error_message"] = msg
                blob["success"] = False
                ret[key] = (blob, parser, hooks)

        return ret

    def await_results(self):

        # Try to get each results
        import fireworks.core.rocket_launcher
        fireworks.core.rocket_launcher.rapidfire(self.lpad, strm_lvl="CRITICAL")

    def list_tasks(self):
        return list(self.queue.values())

    def task_count(self):
        return len(self.queue)
