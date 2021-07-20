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
        """Get the nams of all the tables in the given schema.

        :param schema: schema to scan
        :return: list of tables
        """
        rows = self.query(f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}'")
        return [row['tablename'] for row in rows]

    def get_indexes(self, schema, table):
        """Get a list of Index instances for all the indexes defined for the given  table.
        :param schema: schema name
        :param table:  table name
        :return: list of indexes
        """
        sql = f"SELECT * from pg_index WHERE indrelid = '{schema}.{table}'::regclass"
        return [Index(row, self) for row in self.query(sql)]

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
                     f"Total: {humanize.naturalsize(total_size)}; " \
                     f"Actual: {total_size}b"
            return total_size, detail

    def __get_cursor(self):
        args = self.args
        conn = psycopg2.connect(host=args.db_host, port=args.db_port, user=args.db_user,
                                password=args.db_password, database=args.db_database,
                                cursor_factory=DictCursor)
        return conn.cursor(cursor_factory=DictCursor)


class Index:
    """Class to represent and operate on a table index."""

    def __init__(self, info, db):
        """Create a new instance.
        :param info: the row from pg_index for this index
        :param db: database access
        """
        self.is_pk = info['indisprimary']
        # get the definition SQL, as well as the qualified names of the table and index
        sql = f"SELECT pg_get_indexdef({info['indexrelid']}) AS defn, " \
              f"  {info['indrelid']}::regclass AS tbl, " \
              f"  (parse_ident({info['indexrelid']}::regclass::text)) AS name"
        self.defn, self.table, self.name = \
            map(db.query(sql).fetchone().get, ['defn', 'tbl', 'name'])
        self.db = db

    def drop(self):
        """Drop this index from its table."""
        sql = f"ALTER TABLE {self.table} DROP CONSTRAINT {self.name[1]}" if self.is_pk \
            else f"DROP INDEX {'.'.join(self.name)}"
        self.db.execute(sql)


    def create(self):
        """(Re)create this index on its table."""
        self.db.execute(self.defn)
        # and restore the primary key if this index was a PK index
        if self.is_pk:
            self.db.execute(f"ALTER TABLE {self.table} ADD PRIMARY KEY USING INDEX {self.name[1]}")
