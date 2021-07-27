import argparse
import csv
import logging
import sys

import humanize

from dblib import Database

global logger
csv_format = False


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
        parser.add_argument(
            '--format', '-F', help='size data output format (csv)', default='log')
        # miscellaneous
        parser.add_argument(
            '--log-level', '-l', help='log level',
            choices=[logging.getLevelName(level)
                     for level in logging._levelToName.keys()], default='ERROR')
        self.parser = parser

    def parse(self):
        return self.parser.parse_args()


def log_sizes(schema, db):
    """Log sizes of all tables in the given schemas, including compression-related info for
     hypertables.
     """
    csv_writer = None
    if csv_format:
        headers = ['Table', 'Compressed(b)', 'Total(b)']
        csv_writer = csv.DictWriter(sys.stdout, fieldnames=headers)
        csv_writer.writeheader()
    else:
        print(f"Table size info for schema '{schema}'.")
    total = 0
    for table in sorted(db.get_tables(schema)):
        size, detail = db.table_size_info(schema, table)
        total += size
        if not csv_format:
            print(f"{table}[{humanize.naturalsize(size)}]{': ' + detail if detail else ''}")
            continue

        # We need csv display format.
        if not detail or table.endswith('_old'):
            continue
        # Strip out actual (total) and compressed bytes value from:
        # '...; Uncompressed: 3.0 GB; Compressed: 249891b; Actual: 34398981b'
        actual = 0
        compressed = ''
        for part in detail.split(';'):
            part = part.strip()
            if part.startswith('Compressed: '):
                compressed = part[12:].strip('b')
            elif part.startswith('Actual: '):
                actual = int(part[8:].strip('b'))
        csv_data = {
            'Table': table,
            'Compressed(b)': compressed,
            'Total(b)': actual
        }
        csv_writer.writerow(csv_data)
    if not csv_format:
        print(f"SCHEMA TOTAL: {humanize.naturalsize(total)}")


def get_args():
    """Parse command line args and perform any required post-processing."""
    parser = ArgParser()
    return parser.parse()


def main():
    """Main program - collect command line args, and extend tables as indicated."""
    global logger
    logger = logging.getLogger("schema_size_info")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    args = get_args()
    global csv_format
    csv_format = args.format == 'csv'
    logger.setLevel(args.log_level)
    if logger.isEnabledFor(logging.DEBUG):
        for name in sorted(vars(args).keys()):
            logger.debug(f"Command line arg {name}: {vars(args)[name]}")
    db = Database(args, logger)
    log_sizes(args.db_schema, db)


if __name__ == '__main__':
    main()
