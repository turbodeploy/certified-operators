from contextlib import contextmanager
import psycopg2
import psycopg2.extensions
import mysql.connector
from config_props import get_config_properties
import requests
from logger import logger


# utility methods to work with postgres and mysql databases


def get_from_auth(db_type, property_name, components=None):
    """
    Obtain a property value from auth component. Supported properties are rootUserName and
    rootPassword.

    :param db_type: "postgres" or "mysql"
    :param property_name: name of desired property
    :param components: component whose properties should override global, if any
    :return: property value from auth component
    """
    method = None
    # compute the name of the auth method that will deliver the desired property
    if property_name == 'rootUserName':
        if db_type == 'postgres':
            method = 'getPostgresDBRootUsername'
        elif db_type == 'mysql':
            method = 'getSqlDBRootUsername'
    elif property_name == 'rootPassword':
        method = 'getSqlDBRootPassword'
    if not method:
        raise RuntimeError(
            f"Cannot request property {property_name} for db type {db_type} from auth")
    # use CR props to retrieve coordinates for auth service requests
    props = get_config_properties(components=components)
    host = props.get('authHost')
    port = props.get('serverHttpPort')
    # construct and execute the request
    headers = {'Accept': 'text/plain, application/json, application/*+json, */*'}
    r = requests.get(f"http://{host}:{port}/securestorage/{method}", headers=headers)
    if r.status_code == 200:
        return r.text
    else:
        raise RuntimeError(f"Auth request method {method} failed: {r.text}")


def get_db_default(property_name, db_type, components=None):
    """
    Obtain the default value for the given property for the given database type. These properties
    may be overridden by grafana.ini or CR values, but values returned by this method will be used
    otherwise.

    Some property defaults are fixed, others are obtained from auth component.

    :param property_name: property name
    :param db_type: "postgres" or "mysql"
    :param components: components that can override global props, if any
    :return: property value
    """
    if db_type == 'postgres':
        # postgres default port is 5432, and grafana doesn't care what schema is used, as long
        # as it's the first on the search-path for the grafana login.
        return {'port': '5432',
                'schemaName': 'grafana_writer',
                'databaseName': components[-1] if components else None,
                'rootUserName': get_from_auth(db_type, 'rootUserName', components=components),
                'rootPassword': get_from_auth(db_type, 'rootPassword', components=components),
                'password': get_from_auth(db_type, 'rootPassword', components=components)
                }.get(property_name)
    elif db_type == 'mysql':
        # mysql default port is 3306, and mysql does not have a concept of schema that is distinct
        # from a database
        return {'port': '3306',
                'schemaName': None,
                'databaseName': components[-1] if components else None,
                'rootUserName': get_from_auth(db_type, 'rootUserName', components=components),
                'rootPassword': get_from_auth(db_type, 'rootPassword', components=components),
                'password': get_from_auth(db_type, 'rootPassword', components=components)
                }.get(property_name)
    else:
        raise RuntimeError(f"Invalid db_type: {db_type}")


def get_db_config(property_name, db_type, endpoint_name, components=None):
    """
    Get a DB configuration property for provisioning, by considering the CR properties and defaults
    that would be used by the Java DbEndpointResolver class when completing an endpoint. This
    should not be used for properties that are available in the grafana.ini file, since the latter
    will be used directly by the Grafana server when accessing its internal data.

    N.B. There is nothing that ensures that this class will continue to resolve properties in the
    same way as the Java DbEndpointResolver class. Relevant changes to the latter need to be
    reflected here. Similarly, changes to endpoint names should be reflected in callers of this
    method.

    :param property_name: name of desired property
    :param db_type: "postgres" or "mysql"
    :param endpoint_name: name of DbEndpoint
    :param components: names of component whose properties override global, if any
    :return: property value
    """
    value = None
    db_type_aliases = {'postgres': ['postgres'], 'mysql': ['mysql', 'mariadb']}.get(db_type)
    if not db_type_aliases:
        raise RuntimeError(f"Unknown db_type: {db_type}")
    # try to obtain value from lowest to highest priority config options, except that
    # defaults are retrieved only if necessary at the end, to avoid unneeded calls to auth
    # first come the `xxxDefault` properties, including both mysql and mariadb aliases for mysql
    props = get_config_properties(components)
    for dbType in db_type_aliases:
        value = props.get(f'dbs.{dbType}Default.{property_name}', None)
    prefix = ""
    # next try successively longer prefixes of the endpoint name
    prefix_parts = endpoint_name.split('.')
    for part in prefix_parts:
        prefix = prefix + '.' + part
        # skip first prefix char, which will always be an extraneous period
        value = props.get(prefix[1:] + '.' + property_name, value)
    # finally, get defaults if nothing above produced a value
    return value if value else get_db_default(property_name, db_type_aliases[0],
                                              components=components)


@contextmanager
def get_connection(db_type, host, port, database, user, password):
    """
    Get a connection to a database. The connection is decorated with a context manager that will
    close the connection on exit, so it should typically be used in a `with` statement.

    :param db_type: "postgres" or "mysql"
    :param host: name or IP address of the db server
    :param port: port on which to connect to server
    :param database: name of database to access
    :param user: user login to use for connection
    :param password: password for user
    :return: connection
    """
    connect_func = {'postgres': psycopg2.connect, 'mysql': mysql.connector.connect}.get(db_type)
    if connect_func:
        conn = connect_func(host=host, port=port, database=database, user=user, password=password)
        try:
            yield conn
        finally:
            conn.close()
    else:
        raise RuntimeError(f"Unknown db_type: {db_type}")


def get_endpoint_connection(db_type, endpoint_name, components=None, host=None, port=None,
                            database=None, user=None, password=None):
    """
    Get a database connection using config properties resolved for the given Java `DbEndpoint`
    name. The returned value is a context manager that will close the connection on exit, so
    this should generally be used in a `with` statement.

    :param db_type: "postgres" or "mysql"
    :param endpoint_name: name of endpoint whose config should be used
    :param components: components whose props should override global, if any
    :param host: host name, or null to obtain from configs
    :param port: port number, or null to obtain from configs
    :param database: database name, or null to obtain from configs
    :param user: login user, or null to obtain from configs
    :param password: password, or null to obtain from configs
    :return: connection, wrapped as an auto-closing context manager
    """
    host = host or get_db_config('host', db_type, endpoint_name, components=components)
    port = port or get_db_config('port', db_type, endpoint_name, components=components)
    # explicitly compare to None since '' is a the correct value sent in for a root mysql connection
    database = database if database is not None \
        else get_db_config('databaseName', db_type, endpoint_name, components=components)
    user = user or get_db_config('userName', db_type, endpoint_name, components=components)
    password = password or get_db_config('password', db_type, endpoint_name, components=components)
    return get_connection(db_type, host, port, database, user, password)


def get_root_connection(db_type, endpoint_name, components=None, host=None):
    """
    Obtain credentials to obtain a root-level connection to the database.
    Three values are retrieved: a schema name, a username, and a password

    :param db_type: "postgres" or "mysql"
    :param endpoint_name: name of a Java DbEndpoint whose configuration should be used
    :param components: names of components, in decreasing priority order, whose props should
        override globals
    :param host: host of database server or None to obtain from configs
    :return: (schema_name, user, password) triple
    """
    root_user = get_db_config('rootUserName', db_type, endpoint_name, components=components)
    root_password = get_db_config('rootPassword', db_type, endpoint_name, components=components)
    database = {'mysql': '', 'postgres': 'postgres'}.get(db_type)
    host = host or get_db_config('host', db_type, endpoint_name, components=components)
    return get_endpoint_connection(db_type, endpoint_name, components=components,
                                   host=host, database=database, user=root_user,
                                   password=root_password)
