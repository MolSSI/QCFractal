"""
A command line interface to the qcfractal server.
"""

from __future__ import annotations

import argparse
import atexit
import logging
import multiprocessing
import os
import shutil
import signal
import sys
import textwrap
import threading
import time
import traceback
from typing import TYPE_CHECKING

import tabulate
import yaml

from qcfractal import __version__
from qcportal.auth import RoleInfo, UserInfo
from .config import read_configuration, write_initial_configuration, FractalConfig, WebAPIConfig
from .db_socket.socket import SQLAlchemySocket
from .flask_app.waitress_app import FractalWaitressApp
from .job_runner import FractalJobRunner
from .postgres_harness import PostgresHarness

if TYPE_CHECKING:
    from logging import Logger


class EndProcess(RuntimeError):
    pass


def pretty_bytes(num):
    # SO: https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%4.2f%sB" % (num, unit)
        num /= 1024.0
    return "%.2f%sB" % (num, "Yi")


def dump_config(qcf_config: FractalConfig, indent: int = 0) -> str:
    """
    Returns a string showing a full QCFractal configuration.

    It will be formatted in YAML. It will start and end with a line of '-'

    Parameters
    ----------
    qcf_config
        Configuration to print
    indent
        Indent the entire configuration by this many spaces

    Returns
    -------
    :
        The configuration as a human-readable string
    """

    s = "-" * 80 + "\n"
    cfg_str = yaml.dump(qcf_config.dict())
    s += textwrap.indent(cfg_str, " " * indent)
    s += "-" * 80
    return s


def start_database(config: FractalConfig, check_revision: bool) -> PostgresHarness:
    """
    Obtain a storage socket to a running postgres server

    If the server is not started and we are expected to manage it, this will also
    start it

    This returns a harness and a storage socket
    """

    logger = logging.getLogger(__name__)
    logger.info("Checking the PostgreSQL connection...")

    pg_harness = PostgresHarness(config.database)
    atexit.register(pg_harness.shutdown)

    if config.database.own and not pg_harness.postgres_initialized():
        raise RuntimeError("PostgreSQL instance has not been initialized?")

    # If we are expected to manage the postgres instance ourselves, start it
    # If not, make sure it is started
    pg_harness.ensure_alive()

    # Checks that the database exists
    if not pg_harness.can_connect():
        raise RuntimeError(f"Database at {config.database.safe_uri} does not exist?")

    # Check that the database is up to date
    if check_revision:
        SQLAlchemySocket.check_db_revision(config.database)

    return pg_harness


def parse_args() -> argparse.Namespace:
    """
    Sets up the command line arguments and parses them

    Returns
    -------
    :
        Argparse namespace containing the information about all the options specified on
        the command line
    """

    # Common help strings
    config_file_help = "Path to a QCFractal configuration file"
    verbose_help = "Output more details about the startup of qcfractal-server commands. Use twice for debug output"

    parser = argparse.ArgumentParser(description="A CLI for managing & running a QCFractal server.")

    parser.add_argument("--version", action="version", version=f"{__version__}")
    parser.add_argument("-v", "--verbose", action="count", default=0, help=verbose_help)

    parser.add_argument("--config", help=config_file_help)

    # Common arguments. These are added to the subcommands
    # They are similar to the global options above, but with SUPPRESS as the default
    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument("-v", "--verbose", action="count", default=argparse.SUPPRESS, help=verbose_help)
    base_parser.add_argument("--config", default=argparse.SUPPRESS, help=config_file_help)

    # Now start the real subcommands
    subparsers = parser.add_subparsers(dest="command")

    #####################################
    # init-config subcommand
    #####################################
    init_config = subparsers.add_parser(
        "init-config",
        help="Creates an initial configuration for a server",
        parents=[base_parser],
    )

    init_config.add_argument("--full", action="store_true", help="Create an example config will all fields")

    #####################################
    # init-db subcommand
    #####################################
    subparsers.add_parser(
        "init-db",
        help="Initializes a QCFractal server and database information from a given configuration",
        parents=[base_parser],
    )

    #####################################
    # start subcommand
    #####################################
    start = subparsers.add_parser("start", help="Starts a QCFractal server instance.", parents=[base_parser])

    # Allow some config settings to be altered via the command line
    start.add_argument("--port", **WebAPIConfig.help_info("port"))
    start.add_argument("--host", **WebAPIConfig.help_info("host"))
    start.add_argument("--logfile", **FractalConfig.help_info("logfile"))
    start.add_argument("--loglevel", **FractalConfig.help_info("loglevel"))
    start.add_argument("--enable-security", **FractalConfig.help_info("enable_security"))

    start.add_argument(
        "--disable-job-runner",
        action="store_true",
        help="[ADVANCED] Disable the internal job runner (service updates and manager cleanup)",
    )

    #####################################
    # start-job-runner subcommand
    #####################################
    start_per = subparsers.add_parser(
        "start-job-runner", help="Starts a QCFractal server job-runner", parents=[base_parser]
    )

    # Allow some config settings to be altered via the command line
    start_per.add_argument("--logfile", **FractalConfig.help_info("logfile"))
    start_per.add_argument("--loglevel", **FractalConfig.help_info("loglevel"))

    #####################################
    # start-api subcommand
    #####################################
    start_api = subparsers.add_parser("start-api", help="Starts a QCFractal server instance.", parents=[base_parser])

    # Allow some config settings to be altered via the command line
    start_api.add_argument("--logfile", **FractalConfig.help_info("logfile"))
    start_api.add_argument("--loglevel", **FractalConfig.help_info("loglevel"))

    #####################################
    # upgrade-db subcommand
    #####################################
    subparsers.add_parser("upgrade-db", help="Upgrade QCFractal database.", parents=[base_parser])

    #####################################
    # upgrade-config subcommand
    #####################################
    subparsers.add_parser("upgrade-config", help="Upgrade a QCFractal configuration file.", parents=[base_parser])

    #####################################
    # info subcommand
    #####################################
    info = subparsers.add_parser(
        "info", help="Manage users and permissions on a QCFractal server instance.", parents=[base_parser]
    )
    info.add_argument(
        "category", nargs="?", default="server", choices=["server", "alembic"], help="The type of info to show"
    )

    #####################################
    # user subcommand
    #####################################
    user = subparsers.add_parser("user", help="Manage users for this instance", parents=[base_parser])

    # user sub-subcommands
    user_subparsers = user.add_subparsers(dest="user_command")

    # user list
    user_subparsers.add_parser("list", help="List information about all users", parents=[base_parser])

    # user info
    user_info = user_subparsers.add_parser("info", help="Show information about a user", parents=[base_parser])
    user_info.add_argument("username", default=None, type=str, help="The username to display information about.")

    # user add
    user_add = user_subparsers.add_parser("add", help="Add a user to the QCFractal server.", parents=[base_parser])
    user_add.add_argument("username", default=None, type=str, help="The username to add.")
    user_add.add_argument(
        "--password",
        type=str,
        required=False,
        help="The password for the user. If not specified, a default one will be created and printed.",
    )
    user_add.add_argument("--role", type=str, required=True, help="The role of this user on the server")

    user_add.add_argument(
        "--fullname", default=None, type=str, help="The real name or description of this user (optional)"
    )
    user_add.add_argument("--organization", default=None, type=str, help="The organization of this user (optional)")
    user_add.add_argument("--email", default=None, type=str, help="Email of the user (optional)")

    # user modify
    user_modify = user_subparsers.add_parser(
        "modify", help="Change a user's password or permissions.", parents=[base_parser]
    )
    user_modify.add_argument("username", default=None, type=str, help="The username to modify.")

    user_modify.add_argument(
        "--fullname", default=None, type=str, help="The real name or description of this user (optional)"
    )
    user_modify.add_argument("--organization", default=None, type=str, help="New organization of the user")
    user_modify.add_argument("--email", default=None, type=str, help="New email of the user")
    user_modify.add_argument("--role", default=None, type=str, help="New role of the user")

    user_modify_enable = user_modify.add_mutually_exclusive_group()
    user_modify_enable.add_argument("--enable", action="store_true", help="Enable this user")
    user_modify_enable.add_argument("--disable", action="store_true", help="Disable this user")

    user_modify_password = user_modify.add_mutually_exclusive_group()
    user_modify_password.add_argument(
        "--password", type=str, default=None, required=False, help="Change the user's password to the specified value."
    )
    user_modify_password.add_argument(
        "--reset-password",
        action="store_true",
        help="Reset the user's password. A new password will be generated and printed.",
    )

    # user delete
    user_delete = user_subparsers.add_parser("delete", help="Delete a user.", parents=[base_parser])
    user_delete.add_argument("username", default=None, type=str, help="The username to delete/remove")
    user_delete.add_argument("--no-prompt", action="store_true", help="Do not prompt for confirmation")

    #####################################
    # role subcommand
    #####################################
    role = subparsers.add_parser("role", help="Manage roles for this instance", parents=[base_parser])

    # user sub-subcommands
    role_subparsers = role.add_subparsers(dest="role_command")

    # role list
    role_subparsers.add_parser("list", help="List all role names", parents=[base_parser])

    # role info
    role_info = role_subparsers.add_parser("info", help="Get information about a role", parents=[base_parser])
    role_info.add_argument("rolename", type=str, help="The role of this user on the server")

    # role reset
    role_subparsers.add_parser("reset", help="Reset all the original roles to their defaults", parents=[base_parser])

    #####################################
    # backup subcommand
    #####################################
    backup = subparsers.add_parser(
        "backup", help="Creates a postgres backup file of the current database.", parents=[base_parser]
    )
    backup.add_argument(
        "filename",
        type=str,
        help="The filename to dump the backup to",
    )

    #####################################
    # restore subcommand
    #####################################
    restore = subparsers.add_parser("restore", help="Restores the database from a backup file.", parents=[base_parser])
    restore.add_argument("filename", default=None, type=str, help="The filename to restore from.")

    args = parser.parse_args()
    return args


def server_init_config(config_path: str, full_config: bool):
    write_initial_configuration(config_path, full_config)

    print("*** Creating initial QCFractal configuration ***")
    print(f"Configuration path: {config_path}")
    print("NOTE: secret keys have been randomly generated. You likely won't need to change those")
    print(" !!! You will likely need/want to change some settings before")
    print("     initializing the database or starting the server !!!")


def server_init_db(config: FractalConfig):
    logger = logging.getLogger(__name__)
    logger.info("*** Initializing QCFractal database from configuration ***")

    pg_harness = PostgresHarness(config.database)
    atexit.register(pg_harness.shutdown)

    # If we own the database, initialize and start it
    if config.database.own and pg_harness.postgres_initialized():
        raise RuntimeError("PostgreSQL has been initialized or already exists")

    if config.database.own:
        pg_harness.initialize_postgres()

    pg_harness.create_database(create_tables=True)

    logger.info("QCFractal PostgreSQL instance is initialized")


def server_info(category: str, config: FractalConfig) -> None:
    # Just use raw printing here, rather than going through logging
    if category == "server":
        pg_harness = PostgresHarness(config.database)
        atexit.register(pg_harness.shutdown)
        pg_harness.ensure_alive()

        print()
        print("-" * 80)
        print("Python executable: ", sys.executable)
        print("QCFractal version: ", __version__)
        print("QCFractal alembic revision: ", pg_harness.get_alembic_version())

        if config.database.own:
            print("pg_ctl path: ", pg_harness._get_tool("pg_ctl"))

        print("PostgreSQL server version: ", pg_harness.get_postgres_version())
        print("-" * 80)
        print()
        print()
        print("Displaying QCFractal configuration below")
        print(dump_config(config))
    elif category == "alembic":
        print(f"Displaying QCFractal Alembic CLI configuration:\n")
        alembic_commands = SQLAlchemySocket.alembic_commands(config.database)
        print(" ".join(alembic_commands))


def setup_logging(config, logger: Logger):
    logger.info(f"QCFractal server base folder: {config.base_folder}")

    # Initialize the global logging infrastructure
    stdout_logging = True
    if config.logfile is not None:
        stdout_logging = False
        logger.info(f"Logging to {config.logfile} at level {config.loglevel}")
        log_handler = logging.FileHandler(config.logfile)
    else:
        logger.info(f"Logging to stdout at level {config.loglevel}")
        log_handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        "[%(asctime)s] (%(processName)-16s) %(levelname)8s: %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S %Z"
    )
    log_handler.setFormatter(formatter)

    # Reset the logger given the full configuration
    logging.getLogger().handlers = [log_handler]
    logging.getLogger().setLevel(config.loglevel)

    return stdout_logging


def server_start(config):
    logger = logging.getLogger(__name__)
    logger.info("*** Starting a QCFractal server ***")

    stdout_logging = setup_logging(config, logger)

    # Logger for the rest of this function
    logger = logging.getLogger(__name__)

    # Ensure that the database is alive, optionally starting it
    start_database(config, check_revision=True)

    # Set up a queue for logging. All child process will send logs
    # to this queue, and a separate thread will handle them
    logging_queue = multiprocessing.Queue()

    def _log_thread(queue):
        while True:
            record = queue.get()

            if record is None:
                # None is used as a sentinel to stop the thread
                break

            logging.getLogger(record.name).handle(record)

    log_thread = threading.Thread(target=_log_thread, args=(logging_queue,))
    log_thread.start()

    # Start up the api and job runner
    api_app = FractalWaitressApp(config, logging_queue=logging_queue)
    api_proc = multiprocessing.Process(target=api_app.run)
    api_proc.start()

    job_runners = [FractalJobRunner(config, logging_queue=logging_queue) for _ in range(config.internal_job_processes)]
    job_runner_procs = [multiprocessing.Process(target=jr.start) for jr in job_runners]
    for p in job_runner_procs:
        p.start()

    def _cleanup(sig, frame):
        signame = signal.Signals(sig).name
        logger.debug("In cleanup of qcfractal_server")
        raise EndProcess(signame)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    exitcode = 0
    try:
        while True:
            time.sleep(15)
            if not api_proc.is_alive():
                raise RuntimeError("API process died! Check the logs")
            if not all([p.is_alive() for p in job_runner_procs]):
                raise RuntimeError("A Job runner died! Check the logs")

    except EndProcess as e:
        if not stdout_logging:
            # Start logging to the screen again
            stdout_handler = logging.StreamHandler(sys.stdout)

            # Keep the old formatting (for consistency)
            stdout_handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
            logging.getLogger().handlers.append(stdout_handler)

        logger.info("QCFractal server received EndProcess: " + str(e))
        logger.info("...stopping server...")

    except Exception as e:
        if not stdout_logging:
            # Start logging to the screen again
            stdout_handler = logging.StreamHandler(sys.stdout)

            # Keep the old formatting (for consistency)
            stdout_handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
            logging.getLogger().handlers.append(stdout_handler)

        tb = "".join(traceback.format_exception(None, e, e.__traceback__))
        logger.critical(f"Exception while running QCFractal server:\n{tb}")
        exitcode = 1

    api_proc.terminate()
    api_proc.join()

    for p in job_runner_procs:
        p.terminate()
        p.join()

    # Stop the logging thread
    logger.debug("Stopping logging thread")
    logging_queue.put(None)
    log_thread.join()

    sys.exit(exitcode)


def server_start_job_runner(config):
    from qcfractal.job_runner import FractalJobRunner

    logger = logging.getLogger(__name__)
    logger.info("*** Starting a QCFractal server job runner ***")

    setup_logging(config, logger)

    # Logger for the rest of this function
    # logger = logging.getLogger(__name__)

    # Ensure that the database is alive. This also handles checking stuff,
    # even if we don't own the db (which we shouldn't)
    start_database(config, check_revision=True)

    # Now just run the job runner directly
    job_runner = FractalJobRunner(config)
    job_runner.start()

    def _cleanup(sig, frame):
        logger.debug("In cleanup of job runner")
        job_runner.stop()

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    exit(0)


def server_start_api(config):
    from qcfractal.flask_app.waitress_app import FractalWaitressApp

    logger = logging.getLogger(__name__)
    logger.info("*** Starting a QCFractal API server ***")

    setup_logging(config, logger)

    # Logger for the rest of this function
    # logger = logging.getLogger(__name__)

    # Ensure that the database is alive. This also handles checking stuff,
    # even if we don't own the db (which we shouldn't)
    start_database(config, check_revision=True)

    # Now just run the api process
    api = FractalWaitressApp(config)
    api.run()


def server_upgrade_db(config):

    # Always set logging level to INFO, otherwise things are a bit quiet
    root_logger = logging.getLogger()
    if root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)

    logger = logging.getLogger(__name__)

    # Don't use start_database - we don't want to create the socket (which
    # will probably fail, because the version check should fail)
    pg_harness = PostgresHarness(config.database)
    atexit.register(pg_harness.shutdown)

    if config.database.own and not pg_harness.postgres_initialized():
        raise RuntimeError("PostgreSQL instance has not been initialized?")

    # If we are expected to manage the postgres instance ourselves, start it
    # If not, make sure it is started
    pg_harness.ensure_alive()

    # Checks that the database exists
    if not pg_harness.can_connect():
        raise RuntimeError(f"Database at {config.database.safe_uri} does not exist for upgrading?")

    logger.info(f"Upgrading the postgres database at {config.database.safe_uri}")

    try:
        SQLAlchemySocket.upgrade_database(config.database)
    except ValueError as e:
        print(str(e))
        sys.exit(1)


def server_upgrade_config(config_path):
    import secrets
    from qcfractal.old_config import OldFractalConfig
    from qcfractal.config import convert_old_configuration

    logger = logging.getLogger(__name__)

    logger.info(f"Reading configuration data from {config_path}")
    with open(config_path, "r") as yf:
        file_data = yaml.safe_load(yf)

    # Is this really an old config?
    if "fractal" not in file_data:
        logger.info(f"Configuration appears to be up-to-date")
        return

    old_qcf_config = OldFractalConfig(**file_data)
    new_qcf_config = convert_old_configuration(old_qcf_config)

    # Move the old file out of the way
    base_backup_path = config_path + ".backup"
    backup_path = base_backup_path

    # Find a suitable backup name. If the path exists,
    # add .1, .2, .3, etc, until it does
    i = 1
    while os.path.exists(backup_path):
        backup_path = base_backup_path + f".{i}"
        i += 1

    logger.info(f"Moving original configuration to {backup_path}")
    shutil.move(config_path, backup_path)

    # We strip out some stuff from the configuration, including things that were
    # set to the default
    new_qcf_config_dict = new_qcf_config.dict(skip_defaults=True)
    new_qcf_config_dict["database"].pop("base_folder")
    new_qcf_config_dict.pop("base_folder")

    # Generate secret keys for the user. They can always change it later
    new_qcf_config_dict["api"]["secret_key"] = secrets.token_hex(24)
    new_qcf_config_dict["api"]["jwt_secret_key"] = secrets.token_hex(24)

    logger.info(f"Writing new configuration data to  {config_path}")
    with open(config_path, "w") as yf:
        yaml.dump(new_qcf_config_dict, yf)

    logger.info("*" * 80)
    logger.info("Your configuration file has been upgraded, but will most likely need some fine tuning.")
    logger.info(f"Please edit {config_path} by hand. See the upgrade documentation for details.")
    logger.info("*" * 80)


def server_user(args: argparse.Namespace, config: FractalConfig):
    user_command = args.user_command

    # Don't check revision here - it will be done in the SQLAlchemySocket constructor
    start_database(config, check_revision=False)
    storage = SQLAlchemySocket(config)

    def print_user_info(u: UserInfo):
        enabled = "True" if u.enabled else "False"
        print("-" * 80)
        print(f"      username: {u.username}")
        print(f"     full name: {u.fullname}")
        print(f"  organization: {u.organization}")
        print(f"         email: {u.email}")
        print(f"          role: {u.role}")
        print(f"       enabled: {enabled}")
        print(f"     auth type: {u.auth_type}")
        print("-" * 80)

    if user_command == "list":
        user_list = storage.users.list()

        table_rows = []

        for u in user_list:
            u_obj = UserInfo(**u)
            table_rows.append((u_obj.username, u_obj.role, u_obj.auth_type, u_obj.enabled, u_obj.fullname))

        print()
        table_str = tabulate.tabulate(table_rows, headers=["username", "role", "auth type", "enabled", "fullname"])
        print(table_str)
        print()

    if user_command == "info":
        u = storage.users.get(args.username)
        print_user_info(UserInfo(**u))

    if user_command == "add":
        fullname = args.fullname if args.fullname is not None else ""
        organization = args.organization if args.organization is not None else ""
        email = args.email if args.email is not None else ""

        user_info = UserInfo(
            username=args.username,
            role=args.role,
            fullname=fullname,
            email=email,
            organization=organization,
            enabled=True,
        )

        pw = storage.users.add(user_info, args.password)

        if args.password is None:
            print(f"\nAutogenerated password for {args.username} is below")
            print("-" * 80)
            print(pw)
            print("-" * 80)

        print(f"Created user {args.username}")

    if user_command == "modify":
        u = storage.users.get(args.username)

        if args.fullname is not None:
            u["fullname"] = args.fullname
        if args.organization is not None:
            u["organization"] = args.organization
        if args.email is not None:
            u["email"] = args.email
        if args.role is not None:
            u["role"] = args.role

        # Enable/disable are separate on the command line
        if args.enable is True:
            u["enabled"] = True
        if args.disable is True:
            u["enabled"] = False

        print(f"Updating information for user {u['username']}")
        storage.users.modify(UserInfo(**u), as_admin=True)

        # Passwords are handled separately
        if args.reset_password is True:
            print(f"Resetting password...")
            pw = storage.users.change_password(u["username"], None)
            print(f"\nNew autogenerated password for {args.username} is below")
            print("-" * 80)
            print(pw)
            print("-" * 80)
        elif args.password is not None:
            print("Setting the password...")
            storage.users.change_password(u["username"], args.password)

    if user_command == "delete":
        u = storage.users.get(args.username)
        print_user_info(UserInfo(**u))

        if args.no_prompt is not True:
            r = input("Really delete this user? (Y/N): ")
            if r.lower() == "y":
                storage.users.delete(args.username)
                print("Deleted!")
            else:
                print("ABORTING!")
        else:
            storage.users.delete(args.username)
            print("Deleted!")


def server_role(args: argparse.Namespace, config: FractalConfig):
    role_command = args.role_command

    # Don't check revision here - it will be done in the SQLAlchemySocket constructor
    start_database(config, check_revision=False)
    storage = SQLAlchemySocket(config)

    def print_role_info(r: RoleInfo):
        print("-" * 80)
        print(f"    role: {r.rolename}")
        print(f"    permissions:")
        for stmt in r.permissions.Statement:
            print("        ", stmt)

    if role_command == "list":
        role_list = storage.roles.list()

        print()
        print("rolename")
        print("--------")
        for r in role_list:
            print(r["rolename"])
        print()

    if role_command == "info":
        r = storage.roles.get(args.rolename)
        print_role_info(RoleInfo(**r))

    if role_command == "reset":
        print("Resetting default roles to their original default permissions.")
        print("Other roles will not be affected.")
        storage.roles.reset_defaults()


def server_backup(args: argparse.Namespace, config: FractalConfig):
    pg_harness = start_database(config, check_revision=True)

    db_size = pg_harness.database_size()
    pretty_size = pretty_bytes(db_size)

    print("\n")
    print(f"Backing up the database at {config.database.safe_uri}")
    print(f"Current database size is {pretty_size}")
    print("\n")

    # Bigger than 250GB?
    if db_size > 250 * 2**30:
        print("\n" + "*" * 80)
        print("This is a pretty big database! This will take a while...")
        print("Consider alternate backup strategies, such as pg_basebackup and WAL archiving")
        print("*" * 80 + "\n")

    filepath = os.path.realpath(args.filename)
    filepath_tmp = filepath + ".in_progress"

    if os.path.exists(filepath):
        raise RuntimeError(f"File {filepath} already exists. Will not overwrite!")
    if os.path.exists(filepath_tmp):
        raise RuntimeError(
            f"File {filepath_tmp} already exists. This is a leftover temporary file, and probably "
            f"needs to be deleted, but it could also mean you have a backup running already"
        )

    print(f"Backing up to {filepath_tmp}...", end=None)

    def _remove_temporary_file():
        if os.path.exists(filepath_tmp):
            print("NOTE: Removing temporary file due to unexpected exit")
            os.remove(filepath_tmp)

    atexit.register(_remove_temporary_file)

    pg_harness.backup_database(filepath_tmp)

    print("Done!")
    print(f"Moving to {filepath}")

    # Check again...
    if os.path.exists(filepath):
        raise RuntimeError(
            f"File {filepath} already exists. It didn't before! {filepath_tmp} contains the backup we "
            f"just made. You are on your own."
        )

    shutil.move(filepath_tmp, filepath)

    print("Backup complete!")
    atexit.unregister(_remove_temporary_file)


def server_restore(args: argparse.Namespace, config: FractalConfig):
    if not os.path.isfile(args.filename):
        raise RuntimeError(f"Backup file {args.filename} does not exist or is not a file!")

    logger = logging.getLogger(__name__)
    logger.info("Checking the PostgreSQL connection...")

    pg_harness = PostgresHarness(config.database)
    atexit.register(pg_harness.shutdown)

    # If we own the db, it might not have been initialized
    # (ie, postgres server hasn't been set up at all)
    if config.database.own and not pg_harness.postgres_initialized():
        pg_harness.initialize_postgres()

    pg_harness.ensure_alive()

    # If we are expected to manage the postgres instance ourselves, start it
    # If not, make sure it is started
    pg_harness.ensure_alive()

    db_exists = pg_harness.can_connect()

    if db_exists:
        print("!WARNING! This will erase old data!")
        print("Run 'qcfractal-server backup' before this option if you want to keep it,")
        print("or copy the database to somewhere else")

        user_required_input = f"REMOVEALLDATA {str(config.database.database_name)}"
        print(f"!WARNING! If you are sure you wish to proceed please type '{user_required_input}' below.")
        inp = input("> ")
        print()

        if inp != user_required_input:
            print(f"Input does not match. Aborting")
            return

        pg_harness.delete_database()

    restore_size = os.path.getsize(args.filename)
    pretty_size = pretty_bytes(restore_size)

    print(f"\nSize of the backup file: {pretty_size}")
    print("Starting restore...", end=None)

    pg_harness.restore_database(args.filename)

    print("done")
    print("\nRestore complete!")


def main():
    # Parse all the command line arguments
    args = parse_args()

    # Set up a log handler. This is used before the logfile is set up
    log_handler = logging.StreamHandler(sys.stdout)

    # If the user wants verbose output (particularly about startup of all the commands), then set logging level
    # to be DEBUG
    # Use a stripped down format, since we are just logging to stdout
    if args.verbose == 0:
        logging.basicConfig(level=logging.WARNING, handlers=[log_handler], format="%(levelname)s: %(message)s")
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO, handlers=[log_handler], format="%(levelname)s: %(name)s: %(message)s")
    elif args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG, handlers=[log_handler], format="%(levelname)s: %(name)s: %(message)s")

    logger = logging.getLogger(__name__)

    # If command is not 'init', we need to build the configuration
    # Priority: 1.) command line
    #           2.) environment variables
    #           3.) config file
    # We do not explicitly handle environment variables.
    # They are handled automatically by pydantic

    # Handle some arguments given on the command line
    # They are only valid if starting a server
    cmd_config = {"api": {}}

    # command = the subcommand used (init, start, etc)
    if args.command == "start":
        if args.port is not None:
            cmd_config["api"]["port"] = args.port
        if args.host is not None:
            cmd_config["api"]["host"] = args.host
        if args.logfile is not None:
            cmd_config["logfile"] = args.logfile
        if args.loglevel is not None:
            cmd_config["loglevel"] = args.loglevel
        if args.enable_security is not None:
            cmd_config["enable_security"] = args.enable_security

    if args.command in ["start-job-runner", "start-api"]:
        if args.logfile is not None:
            cmd_config["logfile"] = args.logfile
        if args.loglevel is not None:
            cmd_config["loglevel"] = args.loglevel

    ###############################################################
    # Shortcuts here for initializing/upgrading the configuration
    # We don't want to read old configs with new code, or the
    #    config doesn't exist yet
    ###############################################################

    if args.command == "init-config":
        if args.config is None:
            raise RuntimeError("Configuration file path (--config) is required for initialization")
        server_init_config(args.config, args.full)
        exit(0)

    if args.command == "upgrade-config":
        if args.config is None:
            raise RuntimeError("Configuration file path (--config) is required for upgrading configuration")
        server_upgrade_config(args.config)
        exit(0)

    # Check for the config path on the command line. The command line
    # always overrides environment variables
    config_paths = []
    if args.config is not None:
        config_paths.append(args.config)
    elif "QCF_CONFIG_PATH" in os.environ:
        config_paths.append(os.getenv("QCF_CONFIG_PATH"))

    config_paths = [os.path.abspath(x) for x in config_paths]

    # Now read and form the complete configuration
    qcf_config = read_configuration(config_paths, cmd_config)

    cfg_str = dump_config(qcf_config, 4)
    logger.debug("Assembled the following configuration:\n" + cfg_str)

    if args.command == "info":
        server_info(args.category, qcf_config)
    elif args.command == "init-db":
        server_init_db(qcf_config)
    elif args.command == "start":
        server_start(qcf_config)
    elif args.command == "start-job-runner":
        server_start_job_runner(qcf_config)
    elif args.command == "start-api":
        server_start_api(qcf_config)
    elif args.command == "upgrade-db":
        server_upgrade_db(qcf_config)
    elif args.command == "user":
        server_user(args, qcf_config)
    elif args.command == "role":
        server_role(args, qcf_config)
    elif args.command == "backup":
        server_backup(args, qcf_config)
    elif args.command == "restore":
        server_restore(args, qcf_config)
