#!/usr/bin/env python3
import psycopg2
import psycopg2.extensions
import db
from configparser import ConfigParser
import os
import sys
import logging
from logger import logger
from config_props import get_config_properties

global props
global grafana_config


# Methods to provision a database for Grafana's internal state.
# Provisioning is required if it has not yet occurred for the configured server.
#
# Both Postgres and MySQL servers are supported.

def get_grafana_config():
    """
    Read the grafana.ini file as a primary source for db properties.
    The GF_PATHS_CONFIG environment variable is used to locate the config file (same as used by
    the Grafana server itself).

    :return: parsed config file
    """
    config = ConfigParser(interpolation=None)
    config.read(os.environ['GF_PATHS_CONFIG'])
    return config


def can_connect(db_type, host, port, database, user, password):
    """
    Test to see whether we can use the endpoint user and password to execute a simple query
    against the endpoint database/schema. If this succeeds, provisioning will be deemed already
    complete.

    :param db_type: "postgres" or "mysql"
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


def provision_postgres(host, database, user, password):
    """
    Provision a postgres database and login user if needed.

    :param host: host name or IP addr of the db server
    :param database: database to create
    :param user: username to create
    :param password: password for username
    """
    # get port and schema name for postgres
    port = db.get_db_config("port", "postgres", "dbs.grafana", 'grafana') \
           or db.get_db_config("port", "postgres", "dbs.grafana", ['extractor', 'grafana'])
    schema = db.get_db_config("schemaName", "postgres", "dbs.grafana", 'grafana') \
             or db.get_db_config("schemaName", "postgres", "dbs.grafana", ['extractor', 'grafana'])
    # check if credentials already work, and if so skip provisioning
    if can_connect('postgres', host, port, database, user, password):
        logger.info(
            f"PostgreSQL database {database} already provisioned for user {user} on server {host}")
        return
    try:
        # get a root connection to the database
        with db.get_root_connection('postgres', 'dbs.grafana', ['grafana', 'extractor']) as conn:
            # perform initial provisioning operations when connected to `postgres` database
            # note that some of the statements executed here are not permitted inside a transaction
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            db_execute(conn, f"CREATE USER \"{user}\" PASSWORD '{password}'")
            db_execute(conn, f"CREATE DATABASE \"{database}\" OWNER \"{user}\"")

        # peform remaining operations when connected to the target database
        with db.get_endpoint_connection('postgres', 'dbs.grafana',
                                        ['grafana', 'extractor']) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            db_execute(conn, f"CREATE SCHEMA \"{schema}\" AUTHORIZATION \"{user}\"")
            db_execute(conn, f"ALTER USER \"{user}\" SET search_path TO \"{schema}\"")
            logger.info(f"Provisioned Postgres database {database} for user {user} on server {host}")
    except:
        tb = sys.exc_info()[2]
        raise RuntimeError(f"Failed to perform provisioning for Postgres host={host} user={user}") \
            .with_traceback(tb)


def provision_mysql(host, database, user, password):
    """
    Provision a mysql database and login user if needed.

    :param host: host name or IP addr of the db server
    :param database: database to create
    :param user: username to create
    :param password: password for username
    """
    # get port configured for the endpoint
    port = db.get_db_config("port", "mysql", "dbs.grafana", ['grafana'])
    # check if the credentials already work, and skip provisioning if so
    if can_connect('mysql', host, port, database, user, password):
        logger.info(
            f"MySQL database {database} already provisioned for user {user} on server {host}")
        return
    # obtain root credetials if we need to do provisionng
    try:
        with db.get_root_connection('mysql', 'dbs.grafana', ['grafana'], host=host) as conn:
            db_execute(conn, f"CREATE DATABASE `{database}`")
            db_execute(conn, f"CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'")
            db_execute(conn, f"GRANT ALL ON `{database}`.* TO '{user}'@'%'")
        logger.info(f"Provisioned MySQL database {database} for user {user} on server {host}")
    except:
        tb = sys.exc_info()[2]
        raise RuntimeError(f"Failed to perform provisioning for MySQL host={host} user={user}") \
            .with_traceback(tb)


def main():
    """
    Perform provisioning for the database indicated in the grafana.ini file
    """
    # we're interested in the "database" section of the config file
    db = grafana_config['database'] or {}
    # environment variables get precedence over anything in the ini file
    config = lambda key : os.environ.get('GF_DATABASE_' + key.upper(), db.get(key))
    try:
        if config('type') == 'postgres':
            provision_postgres(config('host'), config('name'), config('user'), config('password'))
        elif config('type') == 'mysql':
            provision_mysql(config('host'), config('name'), config('user'), config('password'))
        else:
            raise RuntimeError(f"Cannot provision database of type {config('type')}")
    except:
        # Fail overall script execution if it didn't work
        logger.exception(f"Failed to provision database")
        sys.exit(1)


if __name__ == '__main__':
    # command-line invocation - establish globals and then call main()
    # flattened CR properties
    props = get_config_properties()
    # grafana.ini values
    grafana_config = get_grafana_config()
    main()
