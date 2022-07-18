#!/usr/bin/env python3

import crypto
import os
import random
import string
import sys
import urllib.error

from consul_kv import Connection
from db import get_from_auth
from logger import logger
from provision_db import get_db_config

global db_config

# Take consul IP and Port and the master key and decrypt and return the hydra
# secret from consul if it's there.  If it's not there generate it, write it to consule (encrypted),
# and return it.
def get_secret(consul_ip, consul_port, master_key):
    conn = Connection(endpoint=f'http://{consul_ip}:{consul_port}/v1/')
    try:
        hydra_secret = conn.get('hydra-1/secret')
        secret = crypto.decrypt(hydra_secret['hydra-1/secret'], master_key)
        return secret
    # There is no secret in Consul
    except urllib.error.HTTPError:
        new_secret = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(32))
        put_secret(new_secret, consul_ip, consul_port, master_key)
        return new_secret

def put_secret(plaintext_secret, consul_ip, consul_port, master_key):
    conn = Connection(endpoint=f'http://{consul_ip}:{consul_port}/v1/')
    try:
        encrypted_secret = crypto.encrypt(plaintext_secret, master_key)
        conn.put('hydra-1/secret', encrypted_secret)
    except:
        tb = sys.exc_info()
        logger.error(tb)
        raise RuntimeError(f"Failed to store system secret in Consul address={consul_ip} port={consul_port}") \
            .with_traceback(tb)

def main():
    if len(sys.argv) != 2:
        logger.error(f'Wrong number of args to hydra_set_secrets_to_env: {sys.argv}')
        sys.exit(1)

    if sys.argv[1] == 'secret':
        consul_host = os.environ['CONSUL_SERVICE_HOST']
        consul_port = os.environ['CONSUL_SERVICE_PORT']
        primary_master_key = crypto.get_master_secret('/etc/turbonomic/master_keys/primary_key_256.out')
        try:
            system_secret = get_secret(consul_host, consul_port, primary_master_key)
            print(system_secret)
            return 0
        # if the primary key fails to decrypt the secret, try the fallback
        except ValueError:
            fallback_master_key = crypto.get_master_secret('/etc/turbonomic/master_keys/fallback_key_256.out')
            system_secret = get_secret(consul_host, consul_port, fallback_master_key)
            # persist the secret in consul encrypted with the new key
            put_secret(system_secret, consul_host, consul_port, primary_master_key)
            print(system_secret)
            return 0
    elif sys.argv[1] == 'dsn':
        # Now set DSN
        # we're interested in the "database" section of the config file
        database = db_config['database'] or {}
        # environment variables get precedence over anything in the ini file
        config = lambda key : os.environ.get('DATABASE_' + key.upper(), database.get(key))
        db_type = config('type')
        dsn = db_type + '://' + config('user') + ':' \
                            + get_from_auth(db_type, 'rootPassword', ['component']) \
                            + "@tcp(" + config('host') + ':' + config('port') + ')/' + config('user')
        print(dsn)
        return 0
    else:
        logger.error(f'Unrecognized argument: {sys.argv[1]}')

if __name__ == '__main__':
    # db.ini values
    db_config = get_db_config()
    main()
