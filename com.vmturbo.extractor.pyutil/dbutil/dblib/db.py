import humanize
import psycopg2
from psycopg2.extras import DictCursor

from .hypertable import HypertableConfig


class Database:
    """Provide DB services."""

    def __init__(self, args, logger):
        self.args = args
        self.logger = logger

    def query(self, sql):
        """Perform a DB query and return results.

        This streams results as they become available and does not fetch all rows into memory first.

        :param sql:Query SQL
        :return:result rows as an iterator of DictRow objects
        """
        cur = None
        self.logger.debug(f"DB query: {sql}")
        try:
            cur = self.__get_cursor()
            cur.execute(sql)
        except Exception as err:
            if cur:
                cur.connection.rollback()
            raise Exception(f"Query failed: {sql}", err)
        else:
            return cur

    def execute(self, sql):
        """Perform a DB operation that does not return result rows.

        :param sql:SQL for operation
        :return:row count from cursor after operation completes
        """
        cur = None
        self.logger.debug(f"DB exec: {sql}")
        try:
            cur = self.__get_cursor()
            cur.execute(sql)
            result = cur.rowcount
        except Exception as err:
            if cur:
                cur.connection.rollback()
            raise Exception(f"DB operation failed: {sql}", err)
        else:
            cur.connection.commit()
            return result

    def get_tables(self, schema):
        rows = self.query(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}'")
        return [row['tablename'] for row in rows]

    def table_size_info(self, schema, table):
        if HypertableConfig.is_hypertable(schema, table, self):
            return HypertableConfig.hypertable_size_info(schema, table, self)
        else:
            sname = f"{schema}.{table}"
            tbl_size, idx_size, total_size = next(self.query(
                f"SELECT pg_relation_size('{sname}'), pg_indexes_size('{sname}'), "
                f"  pg_total_relation_size('{sname}')"))
            detail = f"Table: {humanize.naturalsize(tbl_size)}; " \
                     f"Indexes: {humanize.naturalsize(idx_size)}; " \
                     f"Other: {humanize.naturalsize(total_size-tbl_size-idx_size)}; " \
                     f"Total: {humanize.naturalsize(total_size)}"
            return total_size, detail

    def __get_cursor(self):
        args = self.args
        conn = psycopg2.connect(host=args.db_host, port=args.db_port, user=args.db_user,
                                password=args.db_password, database=args.db_database,
                                cursor_factory=DictCursor)
        return conn.cursor(cursor_factory=DictCursor)

