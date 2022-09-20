#!/usr/bin/env python3
import db
import os
import sys
from logger import logger
from config_props import get_config_properties

global props
global db_config

# Methods to provision a database for a db's internal state.
# Provisioning is required if it has not yet occurred for the configured server.
#
# MySQL server is supported.

def can_connect(db_type, host, port, database, user, password):
    """
    Test to see whether we can use the endpoint user and password to execute a simple query
    against the endpoint database/schema. If this succeeds, provisioning will be deemed already
    complete.

    :param db_type: "mysql"
    :param host: name or IP address of the db server
    :param port: port on which to connect to server
    :param database: name of database to access
    :param user: user login to use for connection
    :param password: password for user
    :return: True if a connection could be created using these parameters, else False
    """
    try:
        with db.get_connection(db_type, host, port, database, user, password) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 'hi there'")
            cur.fetchone()
            # connection worked
        return True
    except:
        # connection failed
        return False

def db_execute(conn, sql):
    """
    Execute a non-query SQL statement on the given connection.
    :param conn: connection to use
    :param sql: SQL statement to execute
    """
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()

def provision_mysql(host, database, user, password, endpoint_name, component):
    """
    Provision a mysql database and login user if needed.

    :param host: host name or IP addr of the db server
    :param database: database to create
    :param user: username to create
    :param password: password for username
    :param endpoint_name
    :param component
    """
    # get port configured for the endpoint
    port = db.get_db_config("port", "mysql", endpoint_name, [component])
    # check if the credentials already work, and skip provisioning if so
    if can_connect('mysql', host, port, database, user, password):
        logger.info(
            f"MySQL database {database} already provisioned for user {user} on server {host}")
        return
    # obtain root credentials if we need to do provisioning
    try:
        with db.get_root_connection('mysql', endpoint_name, [component], host=host) as conn:
            db_execute(conn, f"CREATE DATABASE `{database}`")
            db_execute(conn, f"CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY "
                             f"'{password}'")
            db_execute(conn, f"GRANT ALL ON `{database}`.* TO '{user}'@'%'")
            db_execute(conn, f"FLUSH PRIVILEGES")
        logger.info(f"Provisioned MySQL database {database} for user {user} on server {host}")
    except:
        tb = sys.exc_info()
        logger.error(tb)
        raise RuntimeError(f"Failed to perform provisioning for MySQL host={host} user={user}") \
            .with_traceback(tb)

def main():
    """
    Perform provisioning for the database indicated in the db.ini file
    """
    # we're interested in the "database" section of the config file
    database = db_config['database'] or {}
    # environment variables get precedence over anything in the ini file
    config = lambda key : os.environ.get('DATABASE_' + key.upper(), database.get(key))
    try:
        if config('type') == 'mysql':
            print("Provisioning MySQL")
            provision_mysql(config('host'), config('name'), config('user'), db.get_from_auth(
                'mysql', 'rootPassword', config('component')), config('endpoint_name'), \
                                                          config('component'))
        else:
            logger.error(sys.exc_info())
            raise RuntimeError(f"Cannot provision database of type {config('type')}")
    except:
        # Fail overall script execution if it didn't work
        logger.error(sys.exc_info())
        logger.exception(f"Failed to provision database")
        sys.exit(1)

if __name__ == '__main__':
    # command-line invocation - establish globals and then call main()
    # flattened CR properties
    props = get_config_properties()
    # db.ini values
    db_config = db.get_db_config_from_file()
    main()
