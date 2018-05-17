"""
A command line interface to the qcfractal.
"""

from tornado.options import options, define
import tornado.ioloop
import tornado.web

# define("port", default=8888, help="Run on the given port.", type=int)
# define("mongod_ip", default="127.0.0.1", help="The Mongod instances IP.", type=str)
# define("mongod_port", default=27017, help="The Mongod instances port.", type=int)
# define("mongod_username", default="", help="The Mongod instance username.", type=str)
# define("mongod_password", default="", help="The Mongod instances password.", type=str)
# define("dask_ip", default="", help="The Dask instances IP. If blank starts a local cluster.", type=str)
# define("dask_port", default=8786, help="The Dask instances port.", type=int)
# # define("fireworks_ip", default="", help="The Fireworks instances IP. If blank starts a local cluster.", type=str)
# # define("fireworks_port", default=None, help="The Fireworks instances port.", type=int)
# define("logfile", default="qcdb_server.log", help="The logfile to write to.", type=str)
# define("queue", default="fireworks", help="The type of queue to use dask or fireworks", type=str)
#
#
#
# queues = ["fireworks", "dask"]
# if options.queue not in queues:
#     raise KeyError("Queue of type %s not understood" % options.queue)
#
# if options.queue == "dask":
#     import distributed
#     dask_dir_geuss = os.getcwd() + '/dask_scratch/'
#     define("dask_dir", default=dask_dir_geuss, help="The Dask workers working director", type=str)
#     dask_working_dir = options.dask_dir
# elif options.queue == "fireworks":
#     import fireworks
#
# tornado.options.options.parse_command_line()
# tornado.options.parse_command_line()
# class DQMServer(object):
#     def __init__(self, logfile_name="qcfractal.log"):

#         self.logger = logging.getLogger(__name__)
#         self.logger.setLevel(logging.INFO)

#         handler = logging.FileHandler(options.logfile)
#         handler.setLevel(logging.INFO)

#         myFormatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
#         handler.setFormatter(myFormatter)

#         self.logger.addHandler(handler)

#         self.logger.info("Logfile set to %s\n" % options.logfile)

#         mongo_username = None
#         mongo_password = None
#         if options.mongod_username:
#             mongo_username = options.mongod_username
#         if options.mongod_password:
#             mongo_password = options.mongod_password
#         # Build mongo socket
#         self.mongod_socket = dqm.mongo_helper.MongoSocket(options.mongod_ip, options.mongod_port, username=mongo_username, password=mongo_password, globalAuth=True)

#         self.logger.info("Mongod Socket Info:")
#         self.logger.info(str(self.mongod_socket) + "\n")

#         loop = tornado.ioloop.IOLoop.current()
#         self.local_cluster = None
#         if options.queue == "dask":
#             # Grab the Dask Scheduler
#             if options.dask_ip == "":
#                 self.local_cluster = distributed.LocalCluster(nanny=None)
#                 self.queue_socket = distributed.Client(self.local_cluster)
#             else:
#                 self.queue_socket = distributed.Client(options.dask_ip + ":" + str(options.dask_port))

#             self.logger.info("Dask Scheduler Info:")
#             self.logger.info(str(self.queue_socket) + "\n")

#             # Make sure the scratch is there
#             if not os.path.exists(dask_working_dir):
#                 os.makedirs(dask_working_dir)

#             # Dask Nanny
#             self.queue_nanny = dqm.handlers.DaskNanny(self.queue_socket, self.mongod_socket, logger=self.logger)

#             scheduler = dqm.handlers.DaskScheduler
#         else:
#             self.queue_socket = fireworks.LaunchPad.auto_load()
#             self.queue_nanny = dqm.handlers.FireworksNanny(self.queue_socket, self.mongod_socket, logger=self.logger)

#             self.logger.info("Fireworks Scheduler Info:")
#             self.logger.info(str(self.queue_socket.host) + ":" + str(self.queue_socket.port) + "\n")

#             scheduler = dqm.handlers.FireworksScheduler

#         tornado_args = {
#             "mongod_socket": self.mongod_socket,
#             "queue_socket": self.queue_socket,
#             "queue_nanny": self.queue_nanny,
#             "logger": self.logger,
#         }

#         # Start up the app
#         app = tornado.web.Application([
#             (r"/information", dqm.handlers.Information, tornado_args),
#             (r"/scheduler", scheduler, tornado_args),
#             (r"/mongod", dqm.handlers.Mongod, tornado_args),
#         ])
#         app.listen(options.port)

#         # Query Dask Nanny on loop
#         tornado.ioloop.PeriodicCallback(self.queue_nanny.update, 2000).start()

#         # This is for testing
#         #loop.add_callback(get, "{data}")
#         #loop.add_callback(post, json_data)
#         #loop.run_sync(lambda: post(data))

#         self.loop = loop
#         self.logger.info("QCDB Client successfully initialized at https://localhost:%d.\n" % options.port)

#     def start(self):

#         self.logger.info("QCDB Client successfully started. Starting IOLoop.\n")

#         # Soft quit at the end of a loop
#         try:
#             self.loop.start()
#         except KeyboardInterrupt:
#             if options.queue == "dask":
#                 self.queue_socket.shutdown()
#             if self.local_cluster:
#                 self.local_cluster.close()
#             self.loop.stop()

#         self.logger.info("QCDB Client stopping gracefully. Stopped IOLoop.\n")

#     def stop(self):


if __name__ == "__main__":

    server = QCDBServer()
    server.start()


def main():

    server = qcfractal.server()
    server.start()


if __name__ == '__main__':
    main()
