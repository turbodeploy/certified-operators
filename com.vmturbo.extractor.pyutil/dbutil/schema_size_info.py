import argparse
import logging

import humanize

from dblib import Database


class ArgParser:
    """Command line argument parser."""

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Extractor Data Extender',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # database connection
        parser.add_argument(
            '--db-host', '-H', help='database host', default='localhost')

        parser.add_argument(
            '--db-port', '-p', help='database port', default='5432')
        parser.add_argument(
            '--db-user', '-U', help='database user name', default='extractor')
        parser.add_argument(
            '--db-password', '-P', help='database password', default='vmturbo')
        parser.add_argument(
            '--db-database', '-D', help='database to connect to', default='extractor')
        parser.add_argument(
            '--db-schema', '-S', help='database schema for sample data', default='extractor')
        # miscellaneous
        parser.add_argument(
            '--log-level', '-l', help='log level',
            choices=[logging.getLevelName(level)
                     for level in logging._levelToName.keys()], default='INFO')
        self.parser = parser

    def parse(self):
        return self.parser.parse_args()


def log_sizes(schema, db, logger):
    """Log sizes of all tables in the given schemas, including compression-related info for
     hypertables.
     """
    logger.info(f"Table size info for schema '{schema}'")
    total = 0
    for table in sorted(db.get_tables(schema)):
        size, detail = db.table_size_info(schema, table)
        total += size
        logger.info(f"- {table}[{humanize.naturalsize(size)}]{': '+detail if detail else ''}")
    logger.info(f"- SCHEMA TOTAL: {humanize.naturalsize(total)}")


def get_args():
    """Parse command line args and perform any required post-processing."""
    parser = ArgParser()
    return parser.parse()


def main():
    """Main program - collect command line args, and extend tables as indicated."""
    global logger
    logger = logging.getLogger("extend-data")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    args = get_args()
    logger.setLevel(args.log_level)
    if logger.isEnabledFor(logging.DEBUG):
        for name in sorted(vars(args).keys()):
            logger.debug(f"Command line arg {name}: {vars(args)[name]}")
    db = Database(args, logger)
    log_sizes(args.db_schema, db, logger)


if __name__ == '__main__':
    main()
