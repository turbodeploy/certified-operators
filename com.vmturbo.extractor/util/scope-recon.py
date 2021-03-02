import logging
import psycopg2

from datetime import datetime

class Scope:

    def __init__(self, type_map, cursor):
        self.current = {}
        self.current_time = None
        self.prior = {}
        self.prior_time = None
        self.type_map = type_map
        self.cursor = cursor
        self.starts = {}
        self.MAX_TIME = datetime.fromisoformat('9999-12-31T00:00:00+00:00')
        self.GROUP_TYPES = set(['GROUP', 'RESOURCE_GROUP', 'COMPUTE_CLUSTER',
                                'K8S_CLUSTER', 'STORAGE_CLUSTER', 'BILLING_FAMILY'])

    def open_time(self, t):
        self.prior_time = self.current_time
        self.prior = self.current
        self.current_time = t
        self.current = {}
        logger.info(f"Captured time: {self.current_time} = {t}")
        
    def add_scope(self, seed, scoped):
        for oid in scoped:
            if seed not in self.current:
                self.current[seed] = []
            self.current[seed].append(oid)

    def close_time(self):
        for seed in self.current.keys():
            for oid in self.current[seed]:
                if seed in self.prior and oid in self.prior[seed]:
                    # continuing scope
                    pass
                else:
                    # oid enters seed's scope - remember start time
                    logger.info(f"Adding to starts: ({seed}, {oid}): {self.current_time}")
                    self.starts[(seed, oid)] = self.current_time
                        
        for seed in self.prior.keys():
            for oid in self.prior[seed]:
                if seed in self.current and oid in self.current[seed]:
                    # continuing scope
                    pass
                else:
                    # oid exits seed's scope
                    self.write_record(seed, oid, self.starts[(seed, oid)], prior_time)
                    logger.info(f"Removing from starts: ({seed}, {oid}): {self.starts[(seed, oid)]}")
                    del self.starts[(seed, oid)]

    def finish(self):
        self.close_time()
        for seed in self.current.keys():
            for oid in self.current[seed]:
                self.write_record(seed, oid, self.starts[(seed, oid)], self.MAX_TIME)
        self.starts.clear()
        self.prior.clear()
        self.current.clear()
        self.prior_time = None
        self.current_time = None

    def write_record(self, seed, oid, start, finish):
        if self.is_group(seed):
            # we only add groups when appearing in another entity's scope, since that
            # is the only place they appeared initially
            return
        stmt = """
           INSERT INTO rebuilt_scope (seed_oid, scoped_oid, scoped_type, start, finish)
           VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(stmt, (seed, oid, self.type_map[oid], start, finish))
        # add symmetric relationship for groups scopes
        if self.is_group(oid):
            self.cursor.execute(stmt, (oid, seed, self.type_map[seed], start, finish))

    def is_group(self, oid):
        type = self.type_map[oid]
        return type in self.GROUP_TYPES
        

def load_entity_types(conn):
    type_map = {}
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT oid, type FROM entity')
    for oid, type in cursor.fetchall():
        type_map[oid] = type
    return type_map

def load_scopes(conn):
    scope_map = {}
    cursor = conn.cursor()
    cursor.execute('SELECT hash, scope FROM entity')
    for (hash, scope) in cursor.fetchall():
        scope_map[hash] = scope
    return scope_map

def main():
    global logger
    logger = logging.getLogger("scope-recon")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    logger.setLevel('INFO')
    # No idea why, but if I don't do the following, nothing gets logged at INFO level using logger
    logging.info(None)
    conn = psycopg2.connect(
        user="extractor", password="vmturbo", host="localhost", port="5432",
        database="extractor")
    type_map = load_entity_types(conn)
    logger.info(f'type_map loaded with {len(type_map)} entries')
    hash_scopes = load_scopes(conn)
    logger.info(f'legacy scopes loaded with {len(hash_scopes)} entries')
    cursor = conn.cursor()
    cursor.execute("SELECT time, entity_oid, entity_hash FROM scope_recon WHERE TIME < '2020-12-10 16:40:00' ORDER BY time")
    last_time = None
    scope = Scope(type_map, conn.cursor())
    for (time, oid, hash) in cursor:
        if (time != last_time):
            if last_time:
                scope.close_time()
            scope.open_time(time)
            last_time = time
            logger.info(f'Loading data for time {time}')
        scope.add_scope(oid, hash_scopes[hash])
    scope.finish()
    conn.commit()
            

if (__name__ == '__main__'):
    main()
