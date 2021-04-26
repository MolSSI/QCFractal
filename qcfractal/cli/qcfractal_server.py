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
from ..interface.models import UserInfo, RoleInfo
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
    """
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
    """

    s = "-" * 80 + "\n"
    cfg_str = yaml.dump(qcf_config.dict())
    s += textwrap.indent(cfg_str, " " * indent)
    s += "-" * 80
    return s


def start_database(args, config, logger):
    """
    Obtain a storage socket to a running postgres server

    If the server is not started and we are expected to manage it, this will also
    start it.

    This returns a harness and a storage socket
    """

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
    return pg_harness, SQLAlchemySocket(config)


def parse_args() -> argparse.Namespace:
    """
    Sets up the command line arguments and parses them

    Returns
    -------
    argparse.Namespace
        Argparse namespace containing the information about all the options specified on
        the command line
    """

    parser = argparse.ArgumentParser(description="A CLI for managing & running a QCFractal server.")
    parser.add_argument("--version", action="version", version=f"{qcfractal.__version__}")
    parser.add_argument(
        "--verbose", action="store_true", help="Output more details about the startup of qcfractal-server commands"
    )

    config_location = parser.add_mutually_exclusive_group()
    config_location.add_argument("--base-folder", **FractalConfig.help_info("base_folder"))
    config_location.add_argument(
        "--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml"
    )

    subparsers = parser.add_subparsers(dest="command")

    #####################################
    # init subcommand
    #####################################
    init = subparsers.add_parser(
        "init", help="Initializes a QCFractal server and database information from a given configuration."
    )
    init.add_argument("--base-folder", **FractalConfig.help_info("base_folder"))
    init.add_argument(
        "--config", help="Path to a QCFractal configuration file. Default is ~/.qca/qcfractal/qcfractal_config.yaml"
    )

    #####################################
    # start subcommand
    #####################################
    start = subparsers.add_parser("start", help="Starts a QCFractal server instance.")

    # Allow some config settings to be altered via the command line
    fractal_args = start.add_argument_group("Server Settings")
    fractal_args.add_argument("--port", **FlaskConfig.help_info("port"))
    fractal_args.add_argument("--host", **FlaskConfig.help_info("host"))
    fractal_args.add_argument("--num-workers", **FlaskConfig.help_info("num_workers"))
    fractal_args.add_argument("--logfile", **FractalConfig.help_info("loglevel"))
    fractal_args.add_argument("--loglevel", **FractalConfig.help_info("loglevel"))
    fractal_args.add_argument("--enable-security", **FractalConfig.help_info("enable_security"))

    fractal_args.add_argument(
        "--disable-periodics",
        action="store_true",
        help="[ADVANCED] Disable periodic tasks (service updates and manager cleanup)",
    )

    #####################################
    # upgrade subcommand
    #####################################
    upgrade = subparsers.add_parser("upgrade", help="Upgrade QCFractal database.")

    #####################################
    # upgrade-config subcommand
    #####################################
    upgrade_config = subparsers.add_parser("upgrade-config", help="Upgrade a QCFractal configuration file.")

    #####################################
    # info subcommand
    #####################################
    info = subparsers.add_parser("info", help="Manage users and permissions on a QCFractal server instance.")
    info.add_argument(
        "category", nargs="?", default="config", choices=["config", "alembic"], help="The config category to show."
    )

    #####################################
    # user subcommand
    #####################################
    user = subparsers.add_parser("user", help="Manage users for this instance")

    # user sub-subcommands
    user_subparsers = user.add_subparsers(dest="user_command")

    # user list
    user_subparsers.add_parser("list", help="List information about all users")

    # user info
    user_info = user_subparsers.add_parser("info", help="Show information about a user")
    user_info.add_argument("username", default=None, type=str, help="The username to display information about.")

    # user add
    user_add = user_subparsers.add_parser("add", help="Add a user to the QCFractal server.")
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
    user_modify = user_subparsers.add_parser("modify", help="Change a user's password or permissions.")
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
    user_delete = user_subparsers.add_parser("delete", help="Delete a user.")
    user_delete.add_argument("username", default=None, type=str, help="The username to delete/remove")
    user_delete.add_argument("--no-prompt", action="store_true", help="Do not prompt for confirmation")

    #####################################
    # role subcommand
    #####################################
    role = subparsers.add_parser("role", help="Manage roles for this instance")

    # user sub-subcommands
    role_subparsers = role.add_subparsers(dest="role_command")

    # role list
    role_subparsers.add_parser("list", help="List all role names")

    # role info
    role_subparsers.add_parser("info", help="Get information about a role")

    # role reset
    role_subparsers.add_parser("reset", help="Reset all the original roles to their defaults")

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

    #####################################
    # restore subcommand
    #####################################
    restore = subparsers.add_parser("restore", help="Restores the database from a backup file.")
    restore.add_argument("filename", default=None, type=str, help="The filename to restore from.")

    args = parser.parse_args()
    return args


def server_init(args, config):
    logger = logging.getLogger(__name__)
    logger.info("*** Initializing QCFractal from configuration ***")

    psql = PostgresHarness(config.database)

    # If we own the database, start it up
    if config.database.own:
        psql.start()

    # Does the database already exist? If so, don't do anything
    if psql.is_alive():
        raise RuntimeError("Database already exists, so you don't need to run init")

    psql.create_database()

    # Adds tables, etc
    # TODO: (right??)
    socket = SQLAlchemySocket(config)


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
    logger.info(f"QCFractal server base directory: {config.base_folder}")

    # Initialize the global logging infrastructure
    if config.logfile is not None:
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

    # Logger for the rest of this function
    logger = logging.getLogger(__name__)

    # Ensure that the database is alive, optionally starting it
    pg_harness, _ = start_database(args, config, logger)

    # Start up the gunicorn and periodics
    gunicorn = GunicornProcess(config)
    periodics = PeriodicsProcess(config)
    gunicorn_proc = ProcessRunner("gunicorn", gunicorn)
    periodics_proc = ProcessRunner("periodics", periodics)

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
            if not gunicorn_proc.is_alive():
                raise RuntimeError("Gunicorn process died! Check the logs")
            if not periodics_proc.is_alive():
                raise RuntimeError("Periodics process died! Check the logs")
    except EndProcess as e:
        logger.debug("server_start received EndProcess: " + str(e))
    except Exception as e:
        tb = "".join(traceback.format_exception(None, e, e.__traceback__))
        logger.critical(f"Exception while running QCFractal server:\n{tb}")
        exitcode = 1

    gunicorn_proc.stop()
    periodics_proc.stop()

    # Shutdown the database, but only if we manage it
    if config.database.own:
        pg_harness.shutdown()

    sys.exit(exitcode)


def server_upgrade(args, config):
    logger = logging.getLogger(__name__)

    psql = PostgresHarness(config.database)
    logger.info(f"Upgrading the postgres database at {config.database.safe_uri}")

    try:
        psql.upgrade()
    except ValueError as e:
        print(str(e))
        sys.exit(1)


def server_upgrade_config(args, config_path):
    import secrets
    from ..old_config import OldFractalConfig
    from ..config import convert_old_configuration

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
    new_qcf_config_dict["flask"]["secret_key"] = secrets.token_hex(24)
    new_qcf_config_dict["flask"]["jwt_secret_key"] = secrets.token_hex(24)

    logger.info(f"Writing new configuration data to  {config_path}")
    with open(config_path, "w") as yf:
        yaml.dump(new_qcf_config_dict, yf)

    logger.info("*" * 80)
    logger.info("Your configuration file has been upgraded, but will most likely need some fine tuning.")
    logger.info(f"Please edit {config_path} by hand. See the upgrade documentation for details.")
    logger.info("*" * 80)


def server_user(args, config):
    logger = logging.getLogger(__name__)
    user_command = args.user_command
    pg_harness, storage = start_database(args, config, logger)

    def print_user_info(u: UserInfo):
        enabled = "True" if u.enabled else "False"
        print("-" * 80)
        print(f"      username: {u.username}")
        print(f"     full name: {u.fullname}")
        print(f"  organization: {u.organization}")
        print(f"         email: {u.email}")
        print(f"          role: {u.role}")
        print(f"       enabled: {enabled}")
        print("-" * 80)

    if user_command == "list":
        user_list = storage.user.list()

        print("{:20}  {:16}  {:7}  {}".format("username", "role", "enabled", "fullname"))
        print("{:20}  {:16}  {:7}  {}".format("--------", "----", "-------", "--------"))
        for u in user_list:
            enabled = "True" if u.enabled else "False"
            print(f"{u.username:20}  {u.role:16}  {enabled:>7}  {u.fullname}")

    if user_command == "info":
        u = storage.user.get(args.username)
        print_user_info(u)

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

        pw = storage.user.add(user_info, args.password)

        if args.password is None:
            print("Autogenerated password for user is below")
            print("-" * 80)
            print(pw)
            print("-" * 80)

        print(f"Created user {args.username}")

    if user_command == "modify":
        u = storage.user.get(args.username)

        update = {}
        if args.fullname is not None:
            update["fullname"] = args.fullname
        if args.organization is not None:
            update["organization"] = args.organization
        if args.email is not None:
            update["email"] = args.email
        if args.role is not None:
            update["role"] = args.role

        # Enable/disable are separate on the command line
        if args.enable is True:
            update["enabled"] = True
        if args.disable is True:
            update["enabled"] = False

        u2 = u.copy(update=update)
        print(f"Updating information for user {u2.username}")
        storage.user.modify(u2, as_admin=True)

        # Passwords are handled separately
        if args.reset_password is True:
            print(f"Resetting password...")
            pw = storage.user.reset_password(u.username)
            print("New password is below")
            print("-" * 80)
            print(pw)
            print("-" * 80)
        elif args.password is not None:
            print("Setting the password...")
            storage.user.change_password(u.username, args.password)

    if user_command == "delete":
        u = storage.user.get(args.username)
        print_user_info(u)

        if args.no_prompt is not True:
            r = input("Really delete this user? (Y/N): ")
            if r.lower() == "y":
                storage.user.delete(u.username)
                print("Deleted!")
            else:
                print("ABORTING!")
        else:
            storage.user.delete(u.username)
            print("Deleted!")

    # Shutdown the database, but only if we manage it
    if config.database.own:
        pg_harness.shutdown()


def server_role(args, config):
    logger = logging.getLogger(__name__)
    role_command = args.role_command
    pg_harness, storage = start_database(args, config, logger)

    def print_role_info(r: RoleInfo):
        print("-" * 80)
        print(f"    role: {r.rolename}")
        print(f"    permissions:")
        for stmt in r.permissions["Statement"]:
            print("        ", stmt)

    if role_command == "list":
        role_list = storage.role.list()

        print("rolename")
        print("--------")
        for r in role_list:
            print(f"{r.rolename}")

    if role_command == "info":
        r = storage.role.get(args.rolename)
        print_role_info(r)

    if role_command == "reset":
        print("Resetting default roles to their original default permissions.")
        print("Other roles will not be affected.")
        storage.role.reset_defaults()

    # Shutdown the database, but only if we manage it
    if config.database.own:
        pg_harness.shutdown()


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

    # Set up a a log handler. This is used before the logfile is set up
    log_handler = logging.StreamHandler(sys.stdout)

    # If the user wants verbose output (particularly about startup of all the commands), then set logging level to be DEBUG
    # Use a stripped down format, since we are just logging to stdout
    if args.verbose:
        logging.basicConfig(level="DEBUG", handlers=[log_handler], format="%(levelname)s: %(name)s: %(message)s")
    else:
        logging.basicConfig(level="INFO", handlers=[log_handler], format="%(levelname)s: %(message)s")

    logger = logging.getLogger(__name__)

    # If command is not 'init', we need to build the configuration
    # Priority: 1.) command line
    #           2.) environment variables
    #           3.) config file
    # We do not explicitly handle environment variables.
    # They are handled automatically by pydantic

    # Handle some arguments given on the command line
    # They are only valid if starting a server
    cmd_config = {"flask": {}}

    # command = the subcommand used (init, start, etc)
    if args.command == "start":
        if args.port is not None:
            cmd_config["flask"]["port"] = args.port
        if args.host is not None:
            cmd_config["flask"]["host"] = args.host
        if args.num_workers is not None:
            cmd_config["flask"]["num_workers"] = args.num_workers
        if args.logfile is not None:
            cmd_config["logfile"] = args.logfile
        if args.loglevel is not None:
            cmd_config["loglevel"] = args.loglevel
        if args.enable_security is not None:
            cmd_config["enable_security"] = args.enable_security

    # If base_folder is specified, replace with config=base_folder/qcfractal_config.yaml
    if args.base_folder is not None:
        # Mutual exclusion is handled by argparse
        assert args.config is None
        config_path = os.path.join(args.base_folder, "qcfractal_config.yaml")

    elif args.config is not None:
        config_path = args.config
    else:
        config_path = os.path.expanduser(os.path.join("~", ".qca", "qcfractal", "qcfractal_config.yaml"))
        logger.info(f"Using default configuration path {config_path}")

    # Shortcut here for upgrading the configuration
    # This prevent s actually reading the configuration with the newest code
    if args.command == "upgrade-config":
        server_upgrade_config(args, config_path)
        exit(0)

    # Now read and form the complete configuration
    qcf_config = read_configuration([config_path], cmd_config)

    cfg_str = dump_config(qcf_config, 4)
    logger.debug("Assembled the following configuration:\n" + cfg_str)

    if args.command == "info":
        server_info(args, qcf_config)
    elif args.command == "init":
        server_init(args, qcf_config)
    elif args.command == "start":
        server_start(args, qcf_config)
    elif args.command == "upgrade":
        server_upgrade(args, qcf_config)
    elif args.command == "user":
        server_user(args, qcf_config)
    elif args.command == "role":
        server_role(args, qcf_config)
    # elif args.command == "backup":
    #    server_backup(args, qcf_config)
    # elif args.command == "restore":
    #    server_restore(args, qcf_config)
