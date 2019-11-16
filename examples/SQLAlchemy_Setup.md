## PostgreSQL installation

Install `postgresql` package along with the `-contrib` to get the extra extensions.

`sudo apt-get install postgresql postgresql-contrib`

### Configuring PostgreSQL

PostgreSQL uses roles are users which correspond to the OS users. The default uperadmin 
user created with installation is `postgres`. To login to the PostgreSQL prompt:

`sudo -u postgres psql`

Next, you can add password to the `postgres` user:
`alter user postgres with  password 'myAdminPass';`


Or just `\password postgres`...

### Create a DB and an associated user for your application

First, create the DB and the user and give that user full access to that DB.
Always use that user when accessing the DB, not the admin user.

```
sudo -u postgres psql
postgres=# create database test_qcarchivedb;
postgres=# create user qcarchive with password 'mypass';
postgres=# grant all privileges on database test_qcarchivedb to qcarchive;
postgres=# \q
```

Now, you can connect to the DB with the new user from command line using:

```
psql qcarchive -d qcarchivedb -h localhost  -W 
```

Then type in the password.

### Useful command:

\l : shows DBs
\dt : list tables
\du : list users and roles
\dn : list schema of current DB
\q : quit
sudo service postgresql restart


## Install PostgreSQL using conda:

Create conda environment, and install postgresql:

```
conda install -c anaconda postgresql 
```

Then in the shell run:

```
psql --version
which psql
# create a data folder anywhere you like
initdb -D ~/anaconda3/envs/testsql/psql_data
# change the port in the postgresql.conf file if you have nother running on 5432
# start the server
pg_ctl -D ~/anaconda3/envs/testsql/psql_data start
createuser [-p 5433] --superuser postgres
psql [-p 5433] -c "create database qcarchivedb;" -U postgres
psql [-p 5433] -c "create user qcarchive with password 'mypass';" -U postgres
psql [-p 5433] -c "grant all privileges on database qcarchivedb to qcarchive;" -U postgres
```


## Install DB view using DataGrip (best)

Go to (https://www.jetbrains.com/datagrip/) and install using OS links.
You can create an academic account to use all commercial products of JetBrains.



## Installing and setting up pgAdmin4 (deprecated)

You can also install the graphical interface, [PgAdmin4](https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v4.2/pip/pgadmin4-4.2-py2.py3-none-any.whl)


For Linux distributions, create an env and install pgAdmin4 it using pip wheel.


```bash
conda create -n pgadmin python=3.6
conda activate pgadmin
wget https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v4.12/pip/pgadmin4-4.12-py2.py3-none-any.whl
pip install pgadmin4-4.12-py2.py3-none-any.whl
```

Now, configure pgAdmin in the installation directory (in the conda env):

```bash
which python
vi ~/anaconda/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/config_local.py
```

Then past the configuration

```python
import os
SERVER_MODE = False
DATA_DIR = os.path.realpath(os.path.expanduser('~/.pgadmin/'))
LOG_FILE = os.path.join(DATA_DIR, 'pgadmin4.log')
SQLITE_PATH = os.path.join(DATA_DIR, 'pgadmin4.db')
SESSION_DB_PATH = os.path.join(DATA_DIR, 'sessions')
STORAGE_DIR = os.path.join(DATA_DIR, 'storage')
```

Then run the setup file

`python ~/anaconda/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/setup.py`

Finally, you can run phAdmin by running the server using:

`python ~/anaconda/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/pgAdmin4.py`

and opening the browser [http://localhost:5050](http://localhost:5050)

#### Create an alias to run with one command

Create a file 
```
vi ~/pgamin4.sh
chmod +x ~/pgadmin4.sh 
```
 
 with the following:

```
#!/usr/bin/env bash
 
conda activate pgadmin
python ~/anaconda/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/pgAdmin4.py
```

Then add an alias to the bashrc `vi ~/.bashrc` the following

`alias pgadmin4=~/pgadmin4.sh`

Reload bashrc `source ~/.bashrc`


## Connecting SQLAlchemy to PostgreSQL

First, you need to install sqlalchemy and one of python's postgres 
drivers. The most common one is `psycog2`

`pip install sqlalchemy psycopg2-binary sqlalchemy_utils`


## Migration commands:

Check current version:

`alembic current`

`alembic history --verbose'`

Create migration script:

`alembic revision -m "Add a column"`

Create migration script with autogenerate code (must be revised and checked):

`alembic revision --autogenerate -m "Add a column"`

Upgrade to the latest version (head):

`alembic upgrade head`

Upgrade to a specific version:

`alembic upgrade ae1027a6acf`

Upgrade by 2 versions:

`alembic upgrade +2`

Downgrade by 1 version:

`alembic downgrade -1`

Downgrade back to the beginning (base):

`alembic downgrade base`

 