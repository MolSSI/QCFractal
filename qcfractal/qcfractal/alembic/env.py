from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

from qcfractal.db_socket import BaseORM
from qcfractal.components import register_all  # noqa

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
# Only do this if not being run programmatically
if config.get_main_option("skip_logging", "False") == "False":
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
# target_metadata = None
target_metadata = BaseORM.metadata
compare_type = True

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.
transaction_per_migration = True

# Overwrite the ini-file sqlalchemy.url path
# This allows you to pass in the uri on the command line
uri = context.get_x_argument(as_dictionary=True).get("uri", None)
if uri is not None:
    # Pin the driver if not specified. SQLAlchemy 2.1 changed the default driver to psycopg (v3)
    url = make_url(uri)
    if url.drivername == "postgresql":
        uri = url.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)

    # Escape '%' since alembic config is a ConfigParser, and the url may contain percent-encoded characters
    config.set_main_option("sqlalchemy.url", uri.replace("%", "%%"))


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=compare_type,
        transaction_per_migration=transaction_per_migration,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section), prefix="sqlalchemy.", poolclass=pool.NullPool, future=True
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=compare_type,
            transaction_per_migration=transaction_per_migration,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
