## PostgreSQL installation

Install `postgresql` package along with the `-contrib` to get the extra extensions.

`apt-get install postgresql postgresql-contrib`

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
postgres=# create database qcarchivedb;
postgres=# create user qcarchive with password 'mypass';
postgres=# grant all privileges on database qcarchivedb to qcarchive;
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



## Installing and setting up pgAdmin4

You can also install the graphical interface, [PgAdmin4](https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v4.2/pip/pgadmin4-4.2-py2.py3-none-any.whl)


For Linux distributions, create an env and install pgAdmin4 it using pip wheel.


```bash
conda create -n pgadmin python=3.6
source activate pgadmin
wget https://ftp.postgresql.org/pub/pgadmin/pgadmin4/v4.2/pip/pgadmin4-4.2-py2.py3-none-any.whl
pip install pgadmin4-4.2-py2.py3-none-any.whl
```

Now, configure pgAdmin in the installation directory (in the conda env):

```bash
which python
vi ~/anaconda3/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/config_local.py
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

`python ~/anaconda3/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/setup.py`

Finally, you can run phAdmin by running the server using:

`python ~/anaconda3/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/pgAdmin4.py`

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
 
source activate pgadmin
python ~/anaconda3/envs/pgadmin/lib/python3.6/site-packages/pgadmin4/pgAdmin4.py
```

Then add an alias to the bashrc `vi ~/.bashrc` the following

`alias pgadmin4=~/pgadmin4.sh`

Reload bashrc `source ~./bashrc`


## Connecting SQLAlchemy to PostgreSQL

First, you need to install sqlalchemy and one of python's postgres 
drivers. The most commone one is `psycog2`

`pip install sqlalchemy psycopg2-binary sqlalchemy_utils`