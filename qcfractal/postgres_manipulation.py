import os
import subprocess
import shutil


def _psql_return(data):
    """

    Finds the data line as show below:
    >>> _psql_return('''
     port
    ------
     5432
     2345
    (1 row)
    ''')
    ['5432', '2345']

    """
    return [x.strip() for x in data.splitlines()[2:-1]]


def _run(commands, quiet=True, logger=print):
    proc = subprocess.run(commands, stdout=subprocess.PIPE)
    rcode = proc.returncode
    stdout = proc.stdout.decode()
    if not quiet:
        logger(stdout)

    return (rcode, stdout)


def shutdown_postgres(config, quiet=True, logger=print):
    ret = _run([
        shutil.which("pg_ctl"),
        "-D", str(config.database_path),
        "stop"],
        logger=logger, quiet=quiet) # yapf: disable
    return ret


def initialize_postgres(config, quiet=True, logger=print):

    if not quiet:
        print("Initializing the database:")

    # Initialize the database
    init_code, init_stdout = _run([shutil.which("initdb"), "-D", config.database_path], logger=logger, quiet=quiet)
    if "Success." not in init_stdout:
        raise ValueError(init_stdout)

    # Change any configurations
    psql_conf_file = (config.database_path / "postgresql.conf")
    psql_conf = psql_conf_file.read_text()
    if config.database.port != 5432:
        assert "#port = 5432" in psql_conf
        psql_conf = psql_conf.replace("#port = 5432", f"port = {config.database.port}")

    psql_conf_file.write_text(psql_conf)

    # Startup the server
    start_code, start_stdout = _run([
        shutil.which("pg_ctl"),
        "-D", str(config.database_path),
        "-l", str(config.base_path / config.database.logfile),
        "start"],
        logger=logger, quiet=quiet) # yapf: disable
    if "server started" not in start_stdout:
        raise ValueError(start_stdout)

    # Create teh user and database
    if not quiet:
        logger(f"Building user information.")
    ret = _run([shutil.which("createdb"), "-p", str(config.database.port)])

    def run_psql(cmd):
        psql_cmd = [shutil.which("psql"), "-p", str(config.database.port), "-c"]
        return _run(psql_cmd + [cmd], logger=logger, quiet=quiet)

    if not quiet:
        logger(f"Creating database name '{config.database.default_database}'.")
    ret = run_psql(f'create database {config.database.default_database};')

# createuser [-p 5433] --superuser postgres
# psql [-p 5433] -c "create database qcarchivedb;" -U postgres
# psql [-p 5433] -c "create user qcarchive with password 'mypass';" -U postgres
# psql [-p 5433] -c "grant all privileges on database qcarchivedb to qcarchive;" -U postgres