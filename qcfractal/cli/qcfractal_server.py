"""
A command line interface to the qcfractal server.
"""

import os
import argparse
import shutil
import sys
import textwrap
import logging
import multiprocessing
import yaml
import time
import signal
import traceback

import qcfractal

from ..config import read_configuration, FractalConfig, FlaskConfig
from ..postgres_harness import PostgresHarness
from ..storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from ..periodics import PeriodicsProcess
from ..app.gunicorn_app import GunicornProcess
from ..process_runner import ProcessRunner
from .cli_utils import install_signal_handlers

class EndProcess(RuntimeError):
    pass

def human_sizeof_byte(num, suffix="B"):
    # SO: https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)

def dump_config(qcf_config: FractalConfig, indent: int = 0) -> str:
    '''
    Returns a string showing a full QCFractal configuration.

    It will be formatted in YAML. It will start and end with a line of '-'

    Parameters
    ----------
    qcf_config: FractalConfig
        Configuration to print
    indent: int
        Indent the entire configuration by this many spaces

    Returns
    -------
    str
        The configuration as a human-readable string
    '''

    s = "-"*80 + '\n'
    cfg_str = yaml.dump(qcf_config.dict())
    s += textwrap.indent(cfg_str, ' '*indent)
    s += '-'*80
    return s


def standard_command_startup(name, config, check=True):
    print(f"QCFractal server {name}.\n")
    print(f"QCFractal server base folder: {config.base_folder}")

    if check:
        print("\n>>> Checking the PostgreSQL connection...")
        psql = PostgresHarness(config, quiet=False, logger=print)
        ensure_postgres_alive(psql)

    return psql


def parse_args() -> argparse.Namespace:
    '''
    Sets up the command line arguments and parses them

    Returns
    -------
    argparse.Namespace
        Argparse namespace containing the information about all the options specified on
        the command line
    '''

    parser = argparse.ArgumentParser(description="A CLI for managing & running a QCFractal server.")
    parser.add_argument("--version", action="version", version=f"{qcfractal.__version__}")

    subparsers = parser.add_subparsers(dest="command")

    #####################################
    # init subcommand
    #####################################
    init = subparsers.add_parser("init", help="Initializes a QCFractal server and database information from a given configuration.")
    init.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    init.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")
    init.add_argument("-v", "--verbose", action="store_true", help="Output more details about the initialization process")

    #####################################
    # start subcommand
    #####################################
    start = subparsers.add_parser("start", help="Starts a QCFractal server instance.")
    start.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    start.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")

    # Allow some config settings to be altered via the command line
    fractal_args = start.add_argument_group("Server Settings")
    fractal_args.add_argument("--port", **FlaskConfig.help_info("port"))
    fractal_args.add_argument("--bind", **FlaskConfig.help_info("bind"))
    fractal_args.add_argument("--num-workers", **FlaskConfig.help_info("num_workers"))
    fractal_args.add_argument("--logfile", **FractalConfig.help_info("loglevel"))
    fractal_args.add_argument("--loglevel", **FractalConfig.help_info("loglevel"))
    fractal_args.add_argument("--cprofile", **FractalConfig.help_info("cprofile"))
    fractal_args.add_argument("--enable-security", **FractalConfig.help_info("enable_security"))

    fractal_args.add_argument(
        "--disable-periodics",
        action="store_true",
        help="[ADVANCED] Disable periodic tasks (service updates and manager cleanup)"
    )

    #####################################
    # upgrade subcommand
    #####################################
    upgrade = subparsers.add_parser("upgrade", help="Upgrade QCFractal database.")
    upgrade.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    upgrade.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")


    #####################################
    # info subcommand
    #####################################
    info = subparsers.add_parser("info", help="Manage users and permissions on a QCFractal server instance.")
    info.add_argument(
        "category", nargs="?", default="config", choices=["config", "alembic"], help="The config category to show."
    )
    info.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    info.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")

    #####################################
    # user subcommand
    #####################################
    user = subparsers.add_parser("user", help="Configure a QCFractal server instance.")
    user.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    user.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")

    # user sub-subcommands
    user_subparsers = user.add_subparsers(dest="user_command")

    user_add = user_subparsers.add_parser("add", help="Add a user to the QCFractal server.")
    user_add.add_argument("username", default=None, type=str, help="The username to add.")
    user_add.add_argument(
        "--password",
        default=None,
        type=str,
        required=False,
        help="The password for the user. If None, a default one will be created and printed.",
    )
    user_add.add_argument(
        "--permissions",
        nargs="+",
        default=None,
        type=str,
        required=True,
        help="Permissions for the user. Allowed values: read, write, queue, compute, admin.",
    )

    user_show = user_subparsers.add_parser("info", help="Show the user's current permissions.")
    user_show.add_argument("username", default=None, type=str, help="The username to show.")

    user_modify = user_subparsers.add_parser("modify", help="Change a user's password or permissions.")
    user_modify.add_argument("username", default=None, type=str, help="The username to modify.")
    user_modify_password = user_modify.add_mutually_exclusive_group()
    user_modify_password.add_argument(
        "--password", type=str, default=None, required=False, help="Change the user's password to the specified value."
    )
    user_modify_password.add_argument(
        "--reset-password",
        action="store_true",
        help="Reset the user's password. A new password will be generated and printed.",
    )
    user_modify.add_argument(
        "--permissions",
        nargs="+",
        default=None,
        type=str,
        required=False,
        help="Change the users's permissions. Allowed values: read, write, compute, queue, admin.",
    )

    user_remove = user_subparsers.add_parser("remove", help="Remove a user.")
    user_remove.add_argument("username", default=None, type=str, help="The username to remove.")

    #####################################
    # backup subcommand
    #####################################
    backup = subparsers.add_parser("backup", help="Creates a postgres backup file of the current database.")
    backup.add_argument(
        "--filename",
        default=None,
        type=str,
        help="The filename to dump the backup to, defaults to 'database_name.bak'.",
    )
    backup.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    backup.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")

    #####################################
    # restore subcommand
    #####################################
    restore = subparsers.add_parser("restore", help="Restores the database from a backup file.")
    restore.add_argument("filename", default=None, type=str, help="The filename to restore from.")
    restore.add_argument("--base-folder", **FractalConfig.help_info("base_directory"))
    restore.add_argument("--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml")


    args = parser.parse_args()
    return args


def server_init(args, config):
    logger = logging.getLogger(__name__)
    logger.info("*** Initializing QCFractal from configuration ***")

    psql = PostgresHarness(config)
    quit()

    # Make sure we do not delete anything.
    if config.config_file_path.exists():
        print()
        if not overwrite_config:
            print(
                "QCFractal configuration file already exists, to overwrite use '--overwrite-config' "
                "or use the `qcfractal-server config` command line to alter settings."
            )
            sys.exit(2)
        else:
            user_required_input = f"REMOVEALLDATA {str(config.database_path)}"
            print("!WARNING! A QCFractal configuration is currently initialized")
            print(
                f"!WARNING! Overwriting will delete all current Fractal data, this includes all data in {str(config.database_path)}."
            )
            print("!WARNING! Please use `qcfractal-server config` to alter configuration settings instead.")
            print()
            print(f"!WARNING! If you are sure you wish to proceed please type '{user_required_input}' below.")

            inp = input("  > ")
            print()
            if inp == user_required_input:
                print("All data will be removed from the current QCFractal instance.")
                psql.shutdown()
                shutil.rmtree(str(config.database_path), ignore_errors=True)
            else:
                print("Input does not match 'REMOVEALLDATA', exiting.")
                sys.exit(1)

    # WARNING! Passwords do not currently work.
    # if config.database.password is None:
    #     print("  Database password is None, generating a new private key.")
    #     config.database.password = secrets.token_urlsafe(16)

    print_config = config.dict()
    print_config["database"]["password"] = "**************"
    print_config = yaml.dump(print_config, default_flow_style=False)
    print("\n>>> Settings found:\n")
    print(print_config)

    print("\n>>> Writing settings...")
    config.config_file_path.write_text(yaml.dump(config.dict(), default_flow_style=False))

    print("\n>>> Setting up PostgreSQL...\n")
    config.database_path.mkdir(exist_ok=True)
    if config.database.own:
        try:
            psql.initialize_postgres()
            psql.create_database()
        except ValueError as e:
            print(str(e))
            sys.exit(1)
    else:
        print(
            "Own was set to False, QCFractal will expect a live PostgreSQL server with the above connection information."
        )

    if config.database.own or clear_database:

        print("\n>>> Initializing database schema...\n")
        try:
            psql.init_database()
        except ValueError as e:
            print(str(e))
            sys.exit(1)

    # create tables and stamp version (if not)
    print("\n>>> Finishing up...")
    print("\n>>> Success! Please run `qcfractal-server start` to boot a FractalServer!")


def server_info(args, qcf_config):
    # Just use raw printing here, rather than going through logging
    if args.category == "config":
        print("Displaying QCFractal configuration below")
        print(dump_config(qcf_config))
    elif args.category == "alembic":
        psql = PostgresHarness(qcf_config.database)
        print(f"Displaying QCFractal Alembic CLI configuration:\n")
        print(" ".join(psql.alembic_commands()))


def server_start(args, config):
    logger = logging.getLogger(__name__)
    logger.info("*** Starting a QCFractal server ***")
    logger.info(f"QCFractal server base directory: {config.base_directory}")

    # Initialize the global logging infrastructure
    if config.logfile is not None:
        logger.info(f"Logging to {config.logfile} at level {config.loglevel}")
        log_handler = logging.FileHandler(config.logfile)
    else:
        logger.info(f"Logging to stdout at level {config.loglevel}")
        log_handler = logging.StreamHandler(sys.stdout)

    # Reset the logger given the full configuration
    logging.basicConfig(level=config.loglevel, handlers=[log_handler], format='[%(asctime)s] (%(processName)-16s) %(levelname)8s: %(name)s: %(message)s', datefmt = '%Y-%m-%d %H:%M:%S %Z', force=True)

    # Logger for the rest of this function
    logger = logging.getLogger(__name__)

    logger.info("Checking the PostgreSQL connection...")
    pg_harness = PostgresHarness(config.database)

    # If we are expected to manage the postgres instance ourselves, start it
    # If not, make sure it is started
    pg_harness.ensure_alive()

    # make sure DB is created
    # If it exists, no changes are made
    pg_harness.create_database()

    # Start up a socket. The main thing is to see if it can connect, and also
    # to check if the database needs to be upgraded
    # We then no longer need the socket (gunicorn and periodics will use their own
    # in their subprocesses)
    socket = SQLAlchemySocket()
    socket.init(config)

    # Start up the gunicorn and periodics
    gunicorn = GunicornProcess(config)
    periodics = PeriodicsProcess(config)
    gunicorn_proc = ProcessRunner('gunicorn', gunicorn)
    periodics_proc = ProcessRunner('periodics', periodics)

    def _cleanup(sig, frame):
        signame = signal.Signals(sig).name
        logger.debug("In cleanup of qcfractal_server")
        raise EndProcess(signame)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    try:
        while True:
            time.sleep(15)
            if not gunicorn_proc.is_alive():
                raise RuntimeError("Gunicorn process died! Check the logs")
            if not periodics_proc.is_alive():
                raise RuntimeError("Periodics process died! Check the logs")
    except EndProcess as e:
        logger.debug("server_start received EndProcess: " + str(e))
    except Exception as e:
        tb = ''.join(traceback.format_exception(None, e, e.__traceback__))
        logger.critical(f"Exception while running QCFractal server:\n{tb}")

    gunicorn_proc.stop()
    periodics_proc.stop()

    # Shutdown the database, but only if we manage it
    if config.database.own:
        pg_harness.shutdown()



def server_upgrade(args, config):
    psql = standard_command_startup("upgrade", config)

    print("\n>>> Upgrading the Database...")

    try:
        psql.upgrade()
    except ValueError as e:
        print(str(e))
        sys.exit(1)


def server_user(args, config):
    standard_command_startup("user function", config)

    storage = storage_socket_factory(config.database_uri(safe=False))

    try:
        if args["user_command"] == "add":
            print("\n>>> Adding new user...")
            success, pw = storage.add_user(args["username"], password=args["password"], permissions=args["permissions"])
            if success:
                print(f"\n>>> New user successfully added, password:\n{pw}")
                if config.fractal.security is None:
                    print(
                        "Warning: security is disabled. To enable security, change the configuration YAML field "
                        "fractal:security to local."
                    )
            else:
                print("\n>>> Failed to add user. Perhaps the username is already taken?")
                sys.exit(1)
        elif args["user_command"] == "info":
            print(f"\n>>> Showing permissions for user '{args['username']}'...")
            permissions = storage.get_user_permissions(args["username"])
            if permissions is None:
                print("Username not found!")
                sys.exit(1)
            else:
                print(permissions)
        elif args["user_command"] == "modify":
            print(f"\n>>> Modifying user '{args['username']}'...")
            success, message = storage.modify_user(
                args["username"], args["password"], args["reset_password"], args["permissions"]
            )
            if success:
                info = "Successfully modified user\n"
                if message is not None:
                    info += "with message: " + message
                print(info)
            else:
                print("Failed to modify user\nwith message:", message)
                sys.exit(1)
        elif args["user_command"] == "remove":
            print(f"\n>>> Removing user '{args['username']}'...")
            if storage.remove_user(args["username"]):
                print("Successfully removed user.")
            else:
                print("Failed to remove user.")
                sys.exit(1)

    except Exception as e:
        print(type(e), str(e))
        sys.exit(1)


def server_backup(args, config):
    psql = standard_command_startup("backup", config)

    print("\n>>> Starting backup, this may take several hours for large databases (100+ GB)...")

    db_size = psql.database_size()
    print(f"Current database size: {db_size['stdout']}")

    filename = args["filename"]
    if filename is None:
        filename = f"{config.database.database_name}.bak"
        print(f"No filename provided, defaulting to filename: {filename}")

    filename = os.path.realpath(filename)

    filename_temporary = filename + ".in_progress"
    psql.backup_database(filename=filename_temporary)

    shutil.move(filename_temporary, filename)

    print("Backup complete!")


def server_restore(args, config):
    psql = standard_command_startup("restore", config)

    print("!WARNING! This will erase old data. Make sure to to run 'qcfractal-server backup' before this option.")

    user_required_input = f"REMOVEALLDATA {str(config.database_path)}"
    print(f"!WARNING! If you are sure you wish to proceed please type '{user_required_input}' below.")

    inp = input("  > ")
    print()
    if inp != user_required_input:
        print(f"Input does not match '{user_required_input}', exiting.")
        sys.exit(1)

    if not os.path.isfile(args["filename"]):
        print(f"Provided filename {args['filename']} does not exist.")
        sys.exit(1)

    restore_size = os.path.getsize(args["filename"])
    human_rsize = human_sizeof_byte(restore_size)

    print("\n>>> Starting restore, this may take several hours for large databases (100+ GB)...")
    print(f"Current restore size: {human_rsize}")

    db_name = config.database.database_name
    db_backup_name = db_name + "_backup"

    print("Renaming old database for backup...")
    cmd = psql.command(f"ALTER DATABASE {db_name} RENAME TO {db_backup_name};", check=False)
    if cmd["retcode"] != 0:
        print(cmd["stderr"])
        raise ValueError("Could not rename the old database, stopping.")

    print("Restoring database...")
    try:
        psql.restore_database(args["filename"])
        psql.command(f"DROP DATABASE {db_backup_name};", check=False)
    except ValueError:
        print("Could not restore the database from file, reverting to the old database.")
        psql.command(f"DROP DATABASE {db_name};", check=False)
        psql.command(f"ALTER DATABASE {db_backup_name} RENAME TO {db_name};")
        sys.exit(1)

    print("Restore complete!")


def main():
    # Parse all the command line arguments
    args = parse_args()

    # If the user wants verbose output (for some commands), then set logging level to be DEBUG
    # Use a stripped down format, since we are just logging to stdout
    verbose = "verbose" in args and args.verbose is True

    # Also set it to debug if loglevel is specified to debug
    verbose = verbose or ("loglevel" in args and args.loglevel.upper() == "DEBUG")

    log_handler = logging.StreamHandler(sys.stdout)

    if verbose:
        logging.basicConfig(level="DEBUG", handlers=[log_handler], format='%(levelname)s: %(name)s: %(message)s')
    else:
        logging.basicConfig(level="INFO", handlers=[log_handler], format='%(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)

    # command = the subcommand used (init, start, etc)
    command = args.command

    # If command is not 'init', we need to build the configuration
    # Priority: 1.) command line
    #           2.) environment variables
    #           3.) config file
    # We do not explicitly handle environment variables.
    # They are handled automatically by pydantic

    # Handle some arguments given on the command line
    # They are only valid if starting a server
    cmd_config = {'flask': {}}

    if command == "start":
        if args.port is not None:
            cmd_config["flask"]["port"] = args.port
        if args.bind is not None:
            cmd_config["flask"]["bind"] = args.bind
        if args.num_workers is not None:
            cmd_config["flask"]["num_workers"] = args.num_workers
        if args.logfile is not None:
            cmd_config["logfile"] = args.logfile
        if args.loglevel is not None:
            cmd_config["loglevel"] = args.loglevel
        if args.cprofile is not None:
            cmd_config["cprofile"] = args.cprofile
        if args.enable_security is not None:
            cmd_config["enable_security"] = args.enable_security

    # base_folder is deprecated. If specified, replace with config=base_folder/qcfractal_config.yaml
    if args.base_folder is not None:
        if args.config is not None:
            raise RuntimeError("Cannot specify both --base-folder and --config at the same time!")

        config_path = os.path.join(args.base_folder, 'qcfractal_config.yaml')
        logger.warning("*"*80)
        logger.warning("Using --base-folder is deprecated. Use --config=/path/to/config.yaml instead.")
        logger.warning(f"For now, I will automatically use --config={config_path}")
        logger.warning("*"*80)

    elif args.config is not None:
        config_path = args.config
    else:
        config_path = os.path.expanduser(os.path.join("~", '.qca', 'qcfractal', 'qcfractal_config.yaml'))
        logger.info(f"Using default configuration path {config_path}")

    # Now read and form the complete configuration
    qcf_config = read_configuration([config_path], cmd_config)

    cfg_str = dump_config(qcf_config, 4)
    logger.debug("Assembled the following configuration:\n" + cfg_str)

    # If desired, enable profiling
    #if config.fractal.cprofile is not None:
    #    print("!" * 80)
    #    print(f"! Enabling profiling via cProfile. Outputting data file to {config.fractal.cprofile}")
    #    print("!" * 80)
    #    import cProfile

    #    pr = cProfile.Profile()
    #    pr.enable()

    if command == "info":
        server_info(args, qcf_config)
    elif command == "init":
        server_init(args, qcf_config)
    elif command == "start":
        server_start(args, qcf_config)
    quit()
    #elif command == "upgrade":
    #    server_upgrade(args, qcf_config)
    #elif command == "user":
    #    server_user(args, qcf_config)
    #elif command == "backup":
    #    server_backup(args, qcf_config)
    #elif command == "restore":
    #    server_restore(args, qcf_config)

    # Everything finished. If profiling is enabled, write out the
    # data file
    #if config.fractal.cprofile is not None:
    #    print(f"! Writing profiling data to {config.fractal.cprofile}")
    #    print("! Read using the Stats class of the pstats package")
    #    pr.disable()
    #    pr.dump_stats(config.fractal.cprofile)


if __name__ == "__main__":
    main()
