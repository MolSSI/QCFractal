"""
A command line interface to the dqm_server.
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

def main():
    
    server = dqm_server.server()
    server.start()


if __name__ == '__main__':
    main()
