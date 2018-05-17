
class FireworksNanny(object):
    """
    This object can add to the Dask queue and watches for finished jobs. Jobs that are finished
    are automatically posted to the associated MongoDB and removed from the queue.
    """

    def __init__(self, queue_socket, mongod_socket, logger=None):

        self.queue_socket = queue_socket
        self.mongod_socket = mongod_socket
        self.queue = []
        self.errors = []

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('FireworksNanny')

    def add_future(self, future):
        self.queue.append(future)  # Should be unique via fireworks
        self.logger.info("MONGO ADD: FUTURE %s" % future)
        return future

    def update(self):
        # Fireworks
        import fireworks

        # Find completed projects
        fireworks_db = self.mongod_socket.client.fireworks
        cursor = fireworks_db.launches.find({
            "fw_id": {
                "$in": self.queue
            },
            "state": "COMPLETED"
        }, {"action.stored_data.results": True,
            "_id": False,
            "fw_id": True})

        for data in cursor:

            try:
                result_page = data["action"]["stored_data"]["results"]
                if not result_page["success"]:
                    raise ValueError("Computation (%s, %s) did not complete successfully!:\n%s\n" %
                                     (result_page["molecule_hash"], result_page["modelchem"], result_page["error"]))

                res = self.mongod_socket.add_page(result_page)
                self.logger.info("MONGO ADD: (%s, %s) - %s" % (result_page["molecule_hash"], result_page["modelchem"],
                                                               str(res)))

            except Exception as e:
                ename = str(type(e).__name__) + ":" + str(e)
                msg = "".join(traceback.format_tb(e.__traceback__))
                msg += str(type(e).__name__) + ":" + str(e)
                self.errors.append(msg)
                self.logger.info("MONGO ADD: ERROR\n%s" % msg)

            self.queue.remove(data["fw_id"])
