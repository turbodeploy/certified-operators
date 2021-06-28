import argparse
import ctypes
import json
import logging
import os
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from enum import Enum
from fnmatch import fnmatch
from math import log10, floor
from pathlib import Path

import dataset
import psycopg2
import requests
import ruamel.yaml
import sqlalchemy
import xxhash
from ruamel.yaml import YAML

class Folder: pass

class Dashboard: pass

class Variable: pass

class Panel: pass

class Query: pass

class DB: pass

class Grafana:
    now = datetime.now(timezone.utc)

    def __init__(self, url, api_key):
        self.url = url
        self.headers = {'Authorization': 'Bearer ' + api_key}

        r = self.get('/api/folders')
        self.folders = {folder['title']: folder for folder in r.json()}

    def __require_folder_id(self, title):
        if (self.folders[title]):
            return self.folders[title]['id']
        else:
            raise Exception('Unable to find folder ' + title)

    def get_folder(self, title):
        self.__require_folder_id(title)
        return Folder(self.folders[title], self)

    def load_folder(self, args):
        # note that we don't actually instantiate a folder or its dashboards from grafana
        # in this case, so the logic is all here, except for a few static utility methods
        # in other classes
        id = self.__require_folder_id(args.folder)
        p = Path(args.directory)
        if (not p.is_dir()):
            raise Exception(f'Path "{p}" is not a directory')

        filext = f'.{args.disk_format.lower()}'
        for child in p.iterdir():
            if (child.name == "folder" + filext
                    or child.name == "permissions" + filext
                    or Path(child.name).suffix != filext):
                continue
            if (args.disk_format == 'YAML'):
                db_json = Grafana.__fix_value_tagged_scalars(YAML().load(child.read_text()))
            elif (args.disk_format == 'JSON'):
                db_json = json.loads(child.read_text())
            else:
                raise Exception(f'Cannot load from unrecognized disk format {args.disk_format}')

            if Dashboard.should_include(db_json['uid'], db_json['title'], args):
                prepped_json = Dashboard.prep_for_load(db_json, args)
                post_data = {
                    'dashboard': prepped_json,
                    'overwrite': args.overwrite,
                    'folderId': id,
                    'message': f'Uploaded dashboard at {Grafana.now}'
                }
                logger.info(f"Posting dashboard {db_json['title']} [{db_json['uid']}]")
                self.post("/api/dashboards/db", post_data)

    def save_folder(self, args):
        f = self.get_folder(args.folder)
        p = Path(args.directory)
        filext = f'.{args.disk_format.lower()}'
        if (not p.is_dir()):
            raise Exception(f'Path "{p}" is not a directory')
        for d in self.dashboards:
            dashboard = json.loads(d.raw)
            # Remove id and version
            dashboard.pop("id", None)
            dashboard.pop("version", None)
            #            dashboard.pop("time", None)
            dashboard["editable"] = False
            for v in dashboard["templating"]["list"]:
                # Remove variable selection values
                v.pop("current", None)
                # Hide datasource variables
                if (v["type"] == "datasource"):
                    v["hide"] = 2
            if (self.should_exclude(dashboard, args)):
                continue
            file_name = Path(dashboard['uid']).name
            file_path = p / (file_name + filext)

            if (not args.overwrite and file_path.exists()):
                file_path.rename(f'{file_path}.{int(time.time())}.bak')

            logger.info(f'Saving dashboard {dashboard["title"]} [{dashboard["uid"]}]')
            if (args.disk_format == 'YAML'):
                # turn multi-line strings into string literals before dumping yaml
                ruamel.yaml.scalarstring.walk_tree(dashboard)  # Makes multiline strings be output
                YAML().dump(dashboard, file_path.open('w'))
            elif (args.disk_format == 'JSON'):
                json.dump(dashboard, file_path.open('w'))
            else:
                raise Exception(f'Cannot save to unrecognized disk format {args.disk_format}')

    def test_folder(self, args):
        f = self.get_folder(args.folder)
        test_db = DB(dataset.connect(args.database, schema=args.schema or None))
        perf_db = DB(dataset.connect(args.perfdb or args.database, schema=args.schema or None))
        f.test(args, test_db, perf_db)

    def post(self, path, body):
        r = requests.post(self.url + path, headers=self.headers, json=body, verify=False)
        if (r.status_code != requests.codes.ok):
            r.raise_for_status()
        return r

    def get(self, path):
        r = requests.get(self.url + path, headers=self.headers, verify=False)
        if (r.status_code != requests.codes.ok):
            r.raise_for_status()
        return r

    @staticmethod
    def __fix_value_tagged_scalars(yaml):
        """
        This post-processes the output of the ruamel.yaml parser, fixing a case
        where the parser output is not correct according to YAML 1.2 specification,
        but appears to be related to a YAML 1.1 syntax having to do "Value Key
        Language-Independent Type for YAML Version 1.1". See https://yaml.org/type/value.html

        The upshot is that an unquoted equal-sign (e.g. as an array element, '- =') is
        represneted in a way that causes the JSON dumper to fail. Here we recursively walk
        the parsed object and replace any such values with a plain string.
        """
        if (isinstance(yaml, ruamel.yaml.comments.TaggedScalar)):
            tag = yaml.tag
            if (yaml.tag != None and yaml.tag.value == 'tag:yaml.org,2002:value'):
                return yaml.value
        elif (isinstance(yaml, list)):
            for i in range(len(yaml)):
                yaml[i] = Grafana.__fix_value_tagged_scalars(yaml[i])
        elif (isinstance(yaml, dict)):
            for key in yaml:
                yaml[key] = Grafana.__fix_value_tagged_scalars(yaml[key])
        return yaml

    @staticmethod
    def get_times(args, interval):
        to_time = datetime.fromisoformat(args.end_time)
        from_time = to_time - Grafana.__get_time_delta(args, interval)
        return (from_time, to_time)

    @staticmethod
    def __get_time_delta(args, interval):
        match = re.fullmatch(r'(\d+)([smhd])', interval)
        if match:
            n = int(match.group(1))
            u = match.group(2)
            return timedelta(
                seconds=u == 's' and n,
                minutes=u == 'm' and n,
                hours=u == 'h' and n,
                days=u == 'd' and n)
        else:
            raise Exception(f'Invalid test time interval: {interval}')

    __time_filter_pat = re.compile(r'\$__timeFilter\(([^)]*)\)')

    @staticmethod
    def interpolate(s, from_time, to_time, bindings):
        if not s:
            return None
        time_filter_sub = fr"\1 BETWEEN '{from_time.isoformat()}' AND '{to_time.isoformat()}'"
        result = Grafana.__time_filter_pat.sub(time_filter_sub, s)
        result = result.replace('$__timeFrom()', f"'{from_time.isoformat()}'")
        result = result.replace('$__timeTo()', f"'{to_time.isoformat()}'")
        for key, values in bindings.items():
            key_pat = fr'\[\[{re.escape(key)}\]\]'  # [[key]]
            key_pat += fr'|\${re.escape(key)}'  # $key
            key_pat += fr'|\$\{{{re.escape(key)}(:([a-z]+))?\}}'  # ${key} or ${key:fmt}
            repl = lambda match: Grafana.__value_replacement(values, match.group(2))
            result = re.sub(key_pat, repl, result)
        return result

    @staticmethod
    def __value_replacement(values, format):
        # currently only raw, csv and (default) Sqlstring
        if format == 'raw':
            joined = ",".join(values)
            # probably need to stop using `raw` because grafana puts braces around multiple values
            return '{' + joined + '}' if len(values) > 1 else joined
        elif format == 'csv':
            return ",".join(values)
        else:  # format == 'SqlSring' or unspecivied
            quoted_values = ["'" + x.replace("'", "''") + "'" for x in values]
            return ",".join(quoted_values)

    @staticmethod
    def match_globs(x, globs):
        """ Check whether the given string matches any of the specified glob patterns.
        :param x: value to be tested
        :param globs: comma-separated list of globs to match
        :return: whether value matches at least one of the provided globs
        """
        for glob in str.split(globs, ','):
            if fnmatch(x, glob):
                return True
        return False


class Folder:
    def __init__(self, folder_json, grafana):
        self.title = folder_json['title']
        self.id = folder_json['id']
        self.grafana = grafana

        self.dashboards = []
        for db_meta in self.grafana.get(f"/api/search?folderIds={self.id}&type=dash-db").json():
            logger.debug(f"search response: {db_meta['uid']}")
            db_json = self.grafana.get(f"/api/dashboards/uid/{db_meta['uid']}").json()
            self.dashboards.append(Dashboard(db_json['dashboard'], self))

    def test(self, args, test_db, perf_db):
        started = not args.start_with
        for interval in args.intervals.split(','):
            for db in self.dashboards:
                if (db.included(args) and (started or Grafana.match_globs(db.uid, args.start_with))):
                    started = True
                    db.test(args, interval, test_db, perf_db)


class Dashboard:
    def __init__(self, db_json, folder):
        self.json = db_json
        self.folder = folder

        self.title = db_json['title']
        self.uid = db_json['uid']
        self.variables = [Variable(var_json, self) for var_json in db_json['templating']['list'] \
                          if var_json['type'] == 'query']
        self.has_all = [v for v in self.variables if v.include_all]
        self.panels = [Panel(panel_json, self) for panel_json in db_json['panels']]

    def test(self, args, interval, test_db, perf_db):
        (from_time, to_time) = Grafana.get_times(args, interval)
        logger.info(f"Testing dashboard '{self.title}'"
                    + f" interval {interval} ({from_time}..{to_time})")
        for all in ([False, True] if self.has_all else [False]):
            bindings = self.get_var_bindings(args, interval, 1, all, test_db)
            for var in self.variables:
                logger.debug(f"  Testing variable {var.name}")
                logger.debug(f"  - Var bindings: {bindings}")
                var.test(args, interval, all, test_db, perf_db, bindings)
            for panel in self.panels:
                logger.info(f"  Testing panel '{panel.title}'{'[ALL]' if all else ''}")
                logger.debug(f"  - Var bindings: {bindings}")
                panel.test(args, interval, all, test_db, perf_db, bindings)

    def get_var_bindings(self, args, interval, n, all, test_db):
        bindings = {}
        for var in self.variables:
            bindings[var.name] = var.get_values(args, interval, n, all, test_db, bindings)
        return bindings

    def included(self, args):
        return Dashboard.should_include(self.uid, self.title, args)

    @staticmethod
    def should_include(uid, title, args):
        uid = uid if args.match_on == 'uid' or args.match_on == 'both' else None
        title = title if args.match_on == 'title' or args.match_on == 'both' else None
        included = (not args.include_globs) \
                   or (uid and Grafana.match_globs(uid, args.include_globs)) \
                   or (title and Grafana.match_globs(title, args.include_globs))
        excluded = args.exclude_globs \
                   and ((uid and Grafana.match_globs(uid, args.exclude_globs)) \
                        or (title and Grafana.match_globs(title, args.exclude_globs)))
        return included and not excluded

    @staticmethod
    def prep_for_load(db_json, args):
        for v in db_json['templating']['list']:
            # Unhide the datasource variables
            if (v['type'] == 'datasource'):
                v['hide'] = 0
        # Make dashboard editable
        db_json['editable'] = True
        return db_json


class Variable:
    # todo handle custom varialbe type
    def __init__(self, var_json, dashboard):
        self.json = var_json
        self.dashboard = dashboard

        self.name = var_json['name']
        self.query = var_json['query'] if var_json['type'] == 'query' else None
        self.include_all = var_json['includeAll']
        self.all_value = var_json['allValue'] if var_json['includeAll'] else None

    def get_values(self, args, interval, n, all, test_db, bindings):
        if not self.query:
            return []
        (from_time, to_time) = Grafana.get_times(args, interval)
        if all:
            if self.all_value:
                return self.all_value
            else:
                n = 0 if self.include_all else 1
        sql = Grafana.interpolate(self.query, from_time, to_time, bindings)
        return Variable.get_n_values(sql, n, test_db)

    @staticmethod
    def get_n_values(sql, n, test_db):
        if sql:
            r = test_db.query(sql)  # todo
            values = list(OrderedDict.fromkeys([Variable.__get_value(row) for row in r]))
            return values if n == 0 else values[0:n]

    @staticmethod
    def __get_value(row):
        if '__value' in row:
            return str(row['__value'])
        elif len(row) == 1:
            return str(list(row.values())[0])
        else:
            raise Exception(f'Invalid result row from variable query: {row}')

    def test(self, args, interval, all, test_db, perf_db, bindings):
        (from_time, to_time) = Grafana.get_times(args, interval)
        sql = Grafana.interpolate(self.query, from_time, to_time, bindings)
        metadata = {'time': Grafana.now, 'trial': int(args.trial), 'label': args.label,
                    'folder': args.folder, 'dashboard': self.dashboard.title,
                    'dashboard_uid': self.dashboard.uid, 'query_type': 'variable',
                    'name': self.name, 'variable': str(bindings), 'all_variable': all,
                    'interval': interval, 'start_time': from_time, 'end_time': to_time}
        record_data = test_db.measure([sql], metadata, args)
        keys = ['trial', 'label', 'dashboard_uid', 'query_type', 'name', 'all_variable',
                'interval'] if args.upsert else None
        perf_db.record(record_data, keys)


class Panel:
    def __init__(self, panel_json, dashboard):
        self.json = panel_json
        self.dashboard = dashboard
        self.queries = []

        self.title = panel_json['title']
        if 'targets' in panel_json and panel_json['targets']:
            self.queries = [target['rawSql'] for target in panel_json['targets'] if
                            'rawSql' in target]

    def test(self, args, interval, all, test_db, perf_db, bindings):
        (from_time, to_time) = Grafana.get_times(args, interval)
        queries = [Grafana.interpolate(query, from_time, to_time, bindings) for query in
                   self.queries]
        metadata = {'time': Grafana.now, 'trial': int(args.trial), 'label': args.label,
                    'folder': args.folder, 'dashboard': self.dashboard.title,
                    'dashboard_uid': self.dashboard.uid, 'query_type': 'panel',
                    'name': self.title, 'variable': str(bindings), 'all_variable': all,
                    'interval': interval, 'start_time': from_time, 'end_time': to_time}
        record_data = test_db.measure(queries, metadata, args, log_times=True)
        keys = ['trial', 'label', 'dashboard_uid', 'query_type', 'name', 'all_variable',
                'interval'] if args.upsert else None
        perf_db.record(record_data, keys)


class DB:
    def __init__(self, datasource):
        self.datasource = datasource
        # todo make table name an option
        self.result_table = datasource['grafana']

    def query(self, sql):
        return self.datasource.query(sql)

    __exec_time_pat = re.compile('[0-9.]+')

    def measure(self, queries, metadata, args, log_times=False):
        expect_plan = True
        expect_results = False
        prefix = ''
        if args.panel_mode == 'EXPLAIN':
            prefix = 'EXPLAIN'
        elif args.panel_mode == 'ANALYZE':
            prefix = 'EXPLAIN ANALYZE'
        elif args.panel_mode == 'VERBOSE':
            prefix = 'EXPLAIN ANALYZE VERBOSE'
        else:
            expect_plan = False
            expect_results = True

        timings = []
        records = []
        for sql in queries:
            logger.debug(f"Executing SQL: {sql}")
            try:
                start_time = time.time()
                result_set = self.datasource.query(f'{prefix} {sql}')
                execution_time = (time.time() - start_time)*1000
                record = {'query': sql}
                if expect_plan:
                    analysis = [row['QUERY PLAN'] for row in result_set]
                    execution_time = float(DB.__exec_time_pat.search(analysis[-1]).group())
                    record['analysis'] = '\n'.join(analysis)
                if expect_results:
                    record.update(self.__collect_results(result_set, args.max_rows))
                record['execution_time'] = execution_time
                timings.append(execution_time / 1000)
                record.update(metadata)
                records.append(record)
            except psycopg2.errors.SyntaxError as err:
                logger.error(f'Error executing qeury')
                raise err
            except sqlalchemy.exc.OperationalError as err:
                logger.error(err)
        if log_times:
            logger.info(f'    Query time(s): {timings}')
        return records

    def record(self, record_data, keys):
        try:
            for rec in record_data:
                if keys:
                    self.result_table.upsert(rec, keys)
                else:
                    self.result_table.insert(rec)
        except sqlalchemy.exc.OperationalError as err:
            logger.error(err)

    def __collect_results(self, result_set, max_rows):
        n = 0
        hash = 0
        row_data = []
        for row in result_set:
            if n == 0:
                row_data.append(','.join(row.keys()))
            row_str = ','.join([str(v) for v in self.fix_floats(row.values(),12)])
            # we compute the hash in a fashion that is independent of row order
            x = xxhash.xxh64()
            x.update(row_str)
            hash ^= ctypes.c_long(x.intdigest()).value
            if n < max_rows:
                row_data.append(row_str)
            n += 1
        return {'row_count': n, 'row_data': row_data, 'row_data_hash': hash}

    def fix_floats(self, values, n):
        return [self.fix_float(x, n) if type(x) == float else x for x in values]

    def fix_float(self, x, n):
        # round x to n significant digits, so rounding errors don't create false-negatives
        # in result hash comparisons
        return 0.0 if x == 0.0 else round(x, (n - 1) - int(floor(log10(abs(x)))))

def run_load(args):
    g = Grafana(args.url, args.api_key).load_folder(args)


def run_save(args):
    g = Grafana(args.url, args.api_key).save_folder(args)


def run_lint(args):
    g = Grafana(url=args.url, folder_name=args.folder, api_key=args.api_key)
    output = False
    for line in g.lint(args):
        if (not output):
            logger.warning("Issues detected:")
        logger.warning(f'  {line}')
        output = True
    if (output):
        sys.exit(1)
    else:
        logger.info("No issues detected")
    sys.exit(0)


def run_test(args):
    Grafana(args.url, args.api_key).test_folder(args)

    # t = GrafanaTester(grafana=g, database=db, perfdb=perfdb)
    # end_time = datetime.fromisoformat(args.end_time)
    # for interval in args.intervals.split(','):
    #     t.test(end_time - Grafana.__get_time_delta(interval), end_time, interval, args)


class Command(Enum):
    """Program top-level commands, with their implementation methods"""
    load = [run_load]
    save = [run_save]
    lint = [run_lint]
    test = [run_test]

    def __call__(self, *args):
        """Calling an enum member means calling its implementation method"""
        self.value[0](*args)


class Arg(Enum):
    """
    A way of packaging up argparse argument descriptions for reuse.
    Each member includes a list of positional args, and a dictionary of keyword args.
    """
    url = (['-u', '--url'],
           {'dest': 'url', 'help': 'Grafana base URL (e.g. http://host:3000); env: GRAFANA_URL',
            'default': os.environ.get('GRAFANA_URL'),
            'required': 'GRAFANA_URL' not in os.environ})
    api_key = (['-k', '--apikey'],
               {'dest': 'api_key',
                'help': 'API key for Grafana authentication; env: GRAFANA_KEY',
                'default': os.environ.get('GRAFANA_KEY'),
                'required': 'GRAFANA_KEY' not in os.environ})
    folder = (['-f', '--folder'],
              {'dest': 'folder', 'help': 'name of Grafana folder; env: GRAFANA_FOLDER',
               'default': os.environ.get('GRAFANA_FOLDER'),
               'required': 'GRAFANA_FOLDER' not in os.environ})
    disk_directory = (['-d', '--directory', '--disk-directory; env: GRAnaFANA_DIR'],
                      {'dest': 'directory', 'help': 'on-disk dashboard directory',
                       'default': os.environ.get('GRAFANA_DIR'),
                       'required': 'GRAFANA_DIR' not in os.environ})
    disk_format = (['--disk-format'],
                   {'dest': 'disk_format', 'help': 'on-disk dashboard file format',
                    'default': 'YAML',
                    'choices': ['YAML', 'JSON']})
    database = (['--db', '--database'],
                {'dest': 'database',
                 'help': 'database URL, e.g. postgresql://host:5432; env: GRAFANA_DB',
                 'default': os.environ.get('GRAFANA_DB'),
                 'required': 'GRAFANA_DB' not in os.environ})
    schema = (['--schema'],
              {'dest': 'schema', 'help': 'database schema; env: GRAFANA_SCHEMA',
               'default': os.environ.get('GRAFANA_SCHEMA'),
               'required': 'GRAFANA_SCHEMA' not in os.environ})
    overwrite = (['-o', '--overwrite'],
                 {'dest': 'overwrite', 'action': 'store_true',
                  'help': 'overwrite existing dashboards'})
    clear = (['-c', '--clear'],
             {'dest': 'clear', 'action': 'store_true', 'help': 'remove existing dashboards'})
    include = (['--include'],
               {'dest': 'include_globs',
                'help': 'glob patterns to match for included dashboards'})
    exclude = (['--exclude'],
               {'dest': 'exclude_globs',
                'help': 'glob patterns to match for exclude dashboards'})
    match_on = (['--match-on'],
                {'dest': 'match_on', 'help': 'how to match dashboards for inclusion/exclusion',
                 'choices': ['title', 'uid', 'both'], 'default': 'uid'})
    start_with = (['--start-with'],
                  {'dest': 'start_with',
                   'help': 'dashboard uid pattern for first dashboard to test'})
    test_intervals = (['--intervals'],
                      {
                          'help': 'test interval durations e.g. 30m,1h,4h,1d; env GRAFANA_TEST_INTERVALS',
                          'default': os.environ.get('GRAFANA_TEST_INTERVALS',
                                                    '1h,2h,4h,8h,24h'),
                          'dest': 'intervals'
                      })
    test_end_time = (['--end-time'],
                     {
                         'help': 'test interval end time e.g. 2020-01-01T03:41:53+00:00; env GRAFANA_TEST_END',
                         'default': os.environ.get(
                             'GRAFANA_TEST_END', datetime.now(timezone.utc).isoformat()),
                         'dest': 'end_time',
                     })
    test_panels = (['--test-panels'],
                   {'dest': 'panels_globs',
                    'help': 'glob patterns to match specific panels by name',
                    'default': '*'})
    test_panel_mode = (['--test-panel-mode'],
                       {'dest': 'panel_mode', 'help': 'how to execute SQL for panel queries',
                        'choices': ['RUN', 'EXPLAIN', 'ANALYZE', 'VERBOSE'],
                        'default': 'ANALYZE'})
    test_var_value_count = (['--test-var-value-count'],
                            {'dest': 'var_value_count', 'type': int, 'default': 1,
                             'help': 'how many of the available vars to include for "many" vars'})
    test_label = (['--label'],
                  {'dest': 'label', 'help': 'label column value for performance records',
                   'default': '-none-'})
    test_trial = (['--trial'],
                  {'dest': 'trial',
                   'help': 'trial id to link multiple test executions for reporting; env: GRAFANA_TEST_TRIAL',
                   'type': int, 'default': os.environ.get('GRAFANA_TEST_TRIAL'),
                   'required': 'GRAFANA_TEST_TRIAL' not in os.environ})
    test_perfdb = (['--perfdb'],
                   {'dest': 'perfdb', 'help': 'DB URL for where to write test results',
                    'default': os.environ.get('GRAFANA_PERFDB')})
    test_upsert = (['--upsert'],
                   {'dest': 'upsert', 'action': 'store_true',
                    'help': 'Use upserts so existing perf records are updated'})
    test_max_rows = (['--max-rows'],
                      {'dest': 'max_rows', 'help': 'max result rows to capture in RUN mode',
                       'type': int, 'default': 100000})
    log_level = (['-l', '-log-level'],
                 {'dest': 'loglevel', 'help': 'logging level', 'default': 'INFO',
                  'choices': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']})

    def add_to_parser(self, parser):
        """Add this arg specification to a parser"""
        parser.add_argument(*self.value[0], **self.value[1])


def main():
    os.environ['PGOPTIONS'] = '-c statement_timeout=3600000'
    parser = argparse.ArgumentParser(
        description='Grafana Utilities', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(dest='command')
    load_parser = subparsers.add_parser(
        Command.load.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.disk_directory, Arg.disk_format,
                Arg.overwrite, Arg.clear, Arg.include, Arg.exclude, Arg.match_on,
                Arg.log_level]:
        arg.add_to_parser(load_parser)

    save_parser = subparsers.add_parser(
        Command.save.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.disk_directory, Arg.disk_format,
                Arg.overwrite, Arg.clear, Arg.include, Arg.exclude, Arg.match_on,
                Arg.log_level]:
        arg.add_to_parser(save_parser)

    lint_parser = subparsers.add_parser(
        Command.lint.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.include, Arg.exclude, Arg.match_on,
                Arg.log_level]:
        arg.add_to_parser(lint_parser)

    test_parser = subparsers.add_parser(
        Command.test.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.database, Arg.schema, Arg.test_perfdb,
                Arg.include, Arg.exclude, Arg.match_on, Arg.test_intervals, Arg.test_end_time,
                Arg.log_level, Arg.test_panels, Arg.test_panel_mode, Arg.test_var_value_count,
                Arg.test_label, Arg.test_trial, Arg.test_upsert, Arg.test_max_rows, Arg.start_with]:
        arg.add_to_parser(test_parser)

    args = parser.parse_args()

    global logger
    logger = logging.getLogger("grafana")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    logger.setLevel(args.loglevel or 'INFO')
    # No idea why, but if I don't do the following, nothing gets logged at INFO level using logger
    logging.info(None)
    Command[args.command](args)


if (__name__ == '__main__'):
    main()

# TODO: Create folder for load if it doesn't already exist
# TODO: Implement "clear" behavior for load and save
# TODO: Package this up properly, e.g. for library version dependencies, etc.
# TODO: Add some comments where helpful
# TODO: Fix save and lint operations
