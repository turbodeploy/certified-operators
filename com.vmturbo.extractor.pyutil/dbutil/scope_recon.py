import logging
from datetime import datetime
from multiprocessing import Process, Queue
from time import perf_counter

import psycopg2
import xxhash


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
        self.GROUP_TYPES = {'GROUP', 'RESOURCE_GROUP', 'COMPUTE_CLUSTER', 'K8S_CLUSTER', 'STORAGE_CLUSTER',
                            'BILLING_FAMILY'}

    def open_time(self, t):
        self.prior_time = self.current_time
        self.prior = self.current
        self.current_time = t
        self.current = {}

    def add_scope(self, seed, scoped):
        for oid in scoped:
            if seed not in self.current:
                self.current[seed] = set()
            self.current[seed].add(oid)

    def close_time(self):
        for seed in self.current.keys():
            for oid in self.current[seed]:
                if seed in self.prior and oid in self.prior[seed]:
                    # continuing scope
                    pass
                else:
                    # oid enters seed's scope - remember start time
                    self.starts[(seed, oid)] = self.current_time
        for seed in self.prior.keys():
            for oid in self.prior[seed]:
                if seed in self.current and oid in self.current[seed]:
                    # continuing scope
                    pass
                else:
                    # oid exits seed's scope
                    self.write_record(seed, oid, self.starts[(seed, oid)], self.prior_time)
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
        _type = self.type_map[oid]
        return _type in self.GROUP_TYPES


class ScopeBuilder:
    def __init__(self, type_map, hash_scopes, conn_provider, data_queue):
        self.conn = conn_provider()
        self.type_map = type_map
        self.hash_scopes = hash_scopes
        self.data_queue = data_queue

    def run(self):
        scope = Scope(self.type_map, self.conn.cursor())
        last_time = None
        while True:
            (time, oid, _hash) = self.data_queue.get()
            if not time:
                break
            if time != last_time:
                if last_time:
                    scope.close_time()
                scope.open_time(time)
                last_time = time
            if _hash in self.hash_scopes:
                scope.add_scope(oid, self.hash_scopes[_hash])
            else:
                logger.warning(f"Missing scope for hash {_hash}")
        scope.finish()
        self.conn.commit()


def load_entity_types(conn):
    type_map = {}
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT oid, type FROM entity')
    for oid, _type in cursor.fetchall():
        type_map[oid] = _type
    return type_map


def load_scopes(conn):
    scope_map = {}
    cursor = conn.cursor()
    cursor.execute('SELECT hash, scope FROM entity')
    for (_hash, scope) in cursor.fetchall():
        scope_map[_hash] = scope
    return scope_map


def runner(type_map, hash_scopes, conn_provider, data_queue):
    builder = ScopeBuilder(type_map, hash_scopes, conn_provider, data_queue)
    builder.run()


def main():
    global logger
    logger = logging.getLogger("scope-recon")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    logger.setLevel('INFO')
    # No idea why, but if I don't do the following, nothing gets logged at INFO level using logger
    logging.info(None)
    conn_provider = lambda: psycopg2.connect(
        user="extractor", password="vmturbo", host="localhost", port="5432",
        database="extractor")
    conn = conn_provider()
    type_map = load_entity_types(conn)
    logger.info(f'type_map loaded with {len(type_map)} entries')
    hash_scopes = load_scopes(conn)
    logger.info(f'legacy scopes loaded with {len(hash_scopes)} entries')
    cursor = conn.cursor()
    cursor.execute("SELECT time, entity_oid, entity_hash FROM scope_recon ORDER BY time")
    bucket_count = 1
    queues = [Queue(maxsize=1000) for _ in range(bucket_count)]
    workers = [Process(target=runner, args=(i, type_map, hash_scopes, conn_provider, queues[i])) for i in
               range(bucket_count)]
    for worker in workers:
        worker.start()
    last_time = None
    work = [0 for _ in range(bucket_count)]
    start = None
    for (time, oid, _hash) in cursor:
        if time != last_time:
            if start:
                logger.info(f"Elapsed: {perf_counter() - start}; distribution: {work}")
            logger.info(f"Processing records from time {time}")
            work = [0 for _ in range(bucket_count)]
            last_time = time
            start = perf_counter()
        x = xxhash.xxh64()
        x.update(str(oid))
        bucket = x.intdigest() % bucket_count
        work[bucket] += 1
        queues[bucket].put((time, oid, _hash))
    for i in range(bucket_count):
        queues[i].put((None, None, None))
    start = perf_counter()
    for i in range(bucket_count):
        workers[i].join()
    logger.info(f"Final data write took {perf_counter() - start} seconds")


if __name__ == '__main__':
    main()
