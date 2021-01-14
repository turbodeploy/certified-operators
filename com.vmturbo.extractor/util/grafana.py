from pathlib import Path
from enum import Enum
import argparse
import os
import sys

import json
import ruamel.yaml
from ruamel.yaml import YAML
import requests
import psycopg2
import pprint
import re
import dataset
from datetime import datetime
from datetime import timezone
from datetime import timedelta
import logging
import time
import sqlalchemy
from fnmatch import fnmatch

dashboards = []
now = datetime.now(timezone.utc)


class Dashboard:
    def __init__(self, title, raw):
        self.title = title
        self.variables = []
        self.panels = []
        self.raw = raw

    def add_variable(self, variable):
        self.variables.append(variable)

    def add_panel(self, panel):
        self.panels.append(panel)


class Variable:
    def __init__(self, dashboard, name, query, raw, refresh, all_value=None):
        self.name = name
        self.query = Query(query)
        self.all_value = all_value
        self.raw = raw
        self.refresh = refresh
        dashboard.add_variable(self)


class Panel:
    def __init__(self, dashboard, title, queries, raw):
        self.title = title
        self.queries = []
        for q in queries:
            self.queries.append(Query(q))
        self.raw = raw
        dashboard.add_panel(self)


class Query:
    time_filter = re.compile('\$__timeFilter\(([^)]*)\)')

    def __init__(self, query):
        self.query = query

    def sub(self, cursor, start_time, end_time, variables={}):
        temp = self.time_filter.sub(r"\1 between '" + start_time.isoformat() + "' and '" +
                                    end_time.isoformat() + "'", self.query)
        temp = temp.replace('$__timeFrom()', "'" + start_time.isoformat() + "'")
        temp = temp.replace('$__timeTo()', "'" + end_time.isoformat() + "'")
        for key, value in variables.items():
            temp = temp.replace('[[' + str(key) + ']]', str(value))
            temp = temp.replace('$' + str(key), str(value))
            temp = temp.replace('${' + str(key) + '}', str(value))
        return temp

    def explain(self, database, start_time, end_time, variables={}):
        return self.execute(database, start_time, end_time, 'EXPLAIN', variables)

    def explain_analyze(self, database, start_time, end_time, variables={}):
        return self.execute(database, start_time, end_time, 'ANALYZE', variables)

    def execute(self, database, start_time, end_time, mode, variables={}):
        sql = self.sub(database, start_time, end_time, variables)
        if mode == 'EXPLAIN':
            sql = f'EXPLAIN {sql}'
        elif mode == 'ANALYZE':
            sql = f'EXPLAIN ANALYZE {sql}'
        elif mode == 'VERBOSE':
            sql = f'EXPLAIN ANALYZE VERBOSE {sql}'
        logger.debug(f'Executing DB query: {sql}')
        return database.query(sql), sql

    def get_n_values(self, database, start_time, end_time, n=1, variables={}):
        temp = self.sub(database, start_time, end_time, variables)
        #        temp = temp + ' offset 10 limit 1'
        temp = f'{temp} limit {n}'
        return database.query(temp), temp


class Grafana:
    def __init__(self, url, api_key, folder_name):
        self.url = url
        self.api_key = api_key
        self.folder_name = folder_name
        self.headers = {'Authorization': 'Bearer ' + api_key}
        self.dashboards = []

        r = self.get('/api/folders')
        self.raw = r.text
        self.id = None
        for folder in r.json():
            if (folder['title'] == self.folder_name):
                self.id = folder['id']
                break

        if (None == self.id):
            raise Exception('Unable to find folder ' + folder_name)

        r = self.get('/api/search?folderIds=' + str(self.id))

        result = []
        for d in r.json():
            uid = d['uid']
            r2 = self.get('/api/dashboards/uid/' + str(uid))
            current = r2.json()["dashboard"]
            dashboard = Dashboard(title=current["title"], raw=json.dumps(current, indent=2))
            self.dashboards.append(dashboard)

            variable_list = current["templating"]["list"]
            for variable in variable_list:
                if variable["type"] == "query":
                    Variable(dashboard=dashboard, name=variable["name"], query=variable["query"],
                             refresh=variable["refresh"],
                             all_value=(variable["allValue"] if variable["includeAll"] else None),
                             raw=json.dumps(variable, indent=2))

            for panel in current["panels"]:
                queries = []
                for q in panel.get("targets", []):
                    if "rawSql" in q:
                        queries.append(q["rawSql"])
                Panel(dashboard=dashboard, title=panel["title"], queries=queries,
                      raw=json.dumps(panel, indent=2))

    timestamp_filter = re.compile('TIMESTAMP(?!TZ)', re.IGNORECASE)

    def lint(self):
        results = []
        for d in self.dashboards:
            logger.info(f'Checking dashboard {d.title}')
            for v in d.variables:

                if (v.refresh != 2):
                    results.append(
                        'Dashboard "{0}", Variable "{1}" is not set to refresh on time change'.format(
                            d.title, v.name))
                variable = json.loads(v.raw)
                if (variable['type'] == 'query' and variable['datasource'][0] != '$'):
                    results.append(
                        'Dashboard "{0}", Variable "{1}" datasource is not variable: "{2}"'.format(
                            d.title, v.name, variable['datasource']))

            for p in d.panels:
                panel = json.loads(p.raw)
                # Datasource should be variable based
                if ('datasource' in panel and
                        (panel["datasource"] == None or
                         (panel["datasource"][0] != '$' and panel[
                             "datasource"] != '-- Dashboard --'))):
                    results.append(
                        'Dashboard "{0}", Panel "{1}", Datasource "{2}" does not have variable datasource'.format(
                            d.title, p.title, panel["datasource"]))
                for q in p.queries:
                    if (None != self.timestamp_filter.search(q.query)):
                        results.append(
                            'Dashboard "{0}", Panel "{1}" uses TIMESTAMP instead of TIMESTAMPTZ'.format(
                                d.title, p.title))
        return results

    def save(self, args):
        p = Path(args.directory)
        filext = f'.{args.disk_format.lower()}'
        if (not p.is_dir()):
            raise Exception('Path "{0}" is not a directory'.format(args.directory))
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

    def load(self, args):
        p = Path(args.directory)
        filext = f'.{args.disk_format.lower()}'
        if (not p.is_dir()):
            raise Exception('Path "{0}" is not a directory'.format(path))

        for child in p.iterdir():
            if (child.name == "folder" + filext
                    or child.name == "permissions" + filext
                    or Path(child.name).suffix != filext):
                continue
            if (args.disk_format == 'YAML'):
                dashboard = self.__fixValueTaggedScalars(YAML().load(child.read_text()))
            elif (args.disk_format == 'JSON'):
                dashboard = json.loads(child.read_text())
            else:
                raise Exception(f'Cannot load from unrecognized disk format {args.disk_format}')

            if (self.should_exclude(dashboard, args)):
                continue

            for v in dashboard["templating"]["list"]:
                # Unhide the datasource variables
                if (v["type"] == "datasource"):
                    v["hide"] = 0
            # Make dashboard editable
            dashboard["editable"] = True
            post_data = {}
            post_data["dashboard"] = dashboard
            post_data["overwrite"] = args.overwrite
            post_data["folderId"] = self.id
            post_data["message"] = 'Uploaded dashboard at {0}'.format(now)

            logger.info(f'Posting dashboard {dashboard["title"]} [{dashboard["uid"]}]')
            self.post("/api/dashboards/db", post_data)

    def should_exclude(self, dashboard, args):
        uid = (args.match_on == 'uid' or args.match_on == 'both') and dashboard['uid']
        title = (args.match_on == 'title' or args.match_on == 'both') and dashboard['title']
        included = (not args.include_globs) \
                   or (uid and match_globs(uid, args.include_globs)) \
                   or (title and match_globs(title, args.include_globs))
        excluded = args.exclude_globs\
                   and ((uid and match_globs(uid, args.exclude_globs)) \
                        or (title and match_globs(title, args.exclude_globs)))
        return excluded or not included

    def post(self, path, body):
        r = requests.post(self.url + path, headers=self.headers, json=body)

        if (r.status_code != requests.codes.ok):
            logger.error(f'HTTP request failed: {r.json()["message"]}')
            r.raise_for_status()
        return r

    def get(self, path):
        r = requests.get(self.url + path, headers=self.headers)
        if (r.status_code != requests.codes.ok):
            logger.error(f'HTTP request failed: {r.json()["message"]}')
            r.raise_for_status()
        return r

    def __fixValueTaggedScalars(self, yaml):
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
                yaml[i] = self.__fixValueTaggedScalars(yaml[i])
        elif (isinstance(yaml, dict)):
            for key in yaml:
                yaml[key] = self.__fixValueTaggedScalars(yaml[key])
        return yaml


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


class GrafanaTester:
    execution_time_expr = re.compile('[0-9.]+')

    def __init__(self, grafana, database):
        self.grafana = grafana
        self.database = database
        self.result_table = self.database["grafana"]

    def test(self, start_time, end_time, interval, args):
        for d in self.grafana.dashboards:
            dashboard = json.loads(d.raw)
            if (self.grafana.should_exclude(dashboard, args)):
                continue
            logger.info(f'Testing dashboard {d.title} interval {start_time}..{end_time}')
            for v in d.variables:
                try:
                    logger.debug(f'  Processing variable {v.name} ')
                    query, text = v.query.explain_analyze(
                        database=self.database, start_time=start_time,
                        end_time=end_time)

                    result_text = ""

                    for r in query:
                        result_text += r['QUERY PLAN'] + "\n"
                        last_result = r['QUERY PLAN']

                    execution_time = float(
                        self.execution_time_expr.search(last_result).group())

                    result_row = dict(time=now,
                                      label=args.label,
                                      trial=int(args.trial),
                                      folder_uid=args.folder,
                                      dashboard=d.title,
                                      dashboard_uid=dashboard['uid'],
                                      query_type="variable",
                                      name=v.name,
                                      interval=interval,
                                      start_time=start_time,
                                      end_time=end_time,
                                      execution_time=execution_time,
                                      variable='{}',
                                      query=text,
                                      analysis=result_text)
                    self.result_table.insert(result_row)
                except psycopg2.errors.SyntaxError as err:
                    logger.error(f'Error getting variable {v.name}')
                    raise err
                except sqlalchemy.exc.OperationalError as err:
                    logger.error(err)

            variables = {}
            for v in d.variables:
                try:
                    query, text = v.query.get_n_values(database=self.database,
                                                       n=args.var_value_count,
                                                       start_time=start_time, end_time=end_time)
                    found = False
                    raw = json.loads(v.raw)
                    multi = raw['multi']
                    includeAll = raw['includeAll']
                    values = []
                    for r in query:
                        value = str(r['__value'])
                        if (multi or includeAll):
                            values.append(f"'{value}'")
                        else:
                            values.append(value)
                        found = True
                    if (found):
                        variables[v.name] = ','.join(values)
                        logger.debug(f'  Values for variable {v.name}: {variables[v.name]}')
                    else:
                        logger.debug(f'  No values found for variable {v.name}')
                except psycopg2.errors.SyntaxError as err:
                    logger.error('Error getting variable {v.name}')
                    raise err
                except sqlalchemy.exc.OperationalError as err:
                    logger.error(err)

            if ((len(d.variables) == 0) or
                    (len(variables) != 0)):
                for p in d.panels:
                    if len(p.queries) == 0:
                        continue
                    if not match_globs(p.title, args.panels_globs):
                        continue
                    logger.info(f'  Testing panel {p.title} with var values {variables}')
                    # statement = """select id from grafana where id >= 1599
                    # and dashboard = {0}
                    # and query_type='panel'
                    # and name= {1}
                    # and not all_variable""".format(d.title, p.title)

                    timings = []
                    for q in p.queries:
                        try:
                            query, text = q.execute(
                                database=self.database, start_time=start_time, end_time=end_time,
                                variables=variables, mode=args.panel_mode)

                            result_text = ""
                            for r in query:
                                result_text += r['QUERY PLAN'] + "\n"
                                last_result = r['QUERY PLAN']

                            if args.panel_mode == 'ANALYZE' or args.panel_mode == 'VERBOSE':
                                execution_time = float(
                                    self.execution_time_expr.search(last_result).group())

                            result_row = dict(time=now,
                                              label=args.label,
                                              trial=int(args.trial),
                                              folder_uid=args.folder,
                                              dashboard=d.title,
                                              dashboard_uid=dashboard['uid'],
                                              query_type="panel",
                                              name=p.title,
                                              interval=interval,
                                              start_time=start_time,
                                              end_time=end_time,
                                              execution_time=execution_time,
                                              variable=json.dumps(variables),
                                              all_variable=False,
                                              query=text,
                                              analysis=result_text)
                            self.result_table.insert(result_row)
                            timings.append(execution_time / 1000)
                        except sqlalchemy.exc.ProgrammingError as err:
                            logger.error(f'Error executing dashboard {d.title} panel {p.title}')
                            raise err
                        except sqlalchemy.exc.OperationalError as err:
                            logger.error(err)
                    if timings:
                        logger.info(f'    Query time(s): {timings}')
            all_variable = False
            variables = {}
            for v in d.variables:
                if (v.all_value != None):
                    variables[v.name] = v.all_value
                    all_variable = True

            if (not all_variable):
                continue

            for p in d.panels:
                if len(p.queries) == 0:
                    continue
                if not match_globs(p.title, args.panels_globs):
                    continue
                logger.info(f'  Testing panel {p.title} with all value')
                timings = []
                for q in p.queries:
                    try:
                        query, text = q.execute(
                            database=self.database, start_time=start_time, end_time=end_time,
                            mode=args.panel_mode, variables=variables)

                        result_text = ""
                        for r in query:
                            result_text += r['QUERY PLAN'] + "\n"
                            last_result = r['QUERY PLAN']

                        if args.panel_mode == 'ANALYZE' or args.panel_mode == 'VERBOSE':
                            execution_time = float(
                                self.execution_time_expr.search(last_result).group())

                        result_row = dict(time=now,
                                          label=args.label,
                                          trial=int(args.trial),
                                          folder_uid=args.folder,
                                          dashboard=d.title,
                                          dashboard_uid=dashboard['uid'],
                                          query_type="panel",
                                          name=p.title,
                                          interval=interval,
                                          start_time=start_time,
                                          end_time=end_time,
                                          execution_time=execution_time,
                                          variable=json.dumps(variables),
                                          all_variable=True,
                                          query=text,
                                          analysis=result_text)
                        self.result_table.insert(result_row)
                        timings.append(execution_time / 1000)
                    except psycopg2.errors.SyntaxError as err:
                        logger.error(f'Error getting variable {v.name}')
                        raise err
                    except sqlalchemy.exc.OperationalError as err:
                        logger.error(err)
                if timings:
                    logger.info(f'    Query time(s): {timings}')


def run_load(args):
    g = Grafana(url=args.url, folder_name=args.folder, api_key=args.api_key)
    g.load(args)


def run_save(args):
    g = Grafana(url=args.url, folder_name=args.folder, api_key=args.api_key)
    g.save(args)


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
    os.environ['PGOPTIONS'] = '-c statement_timeout=3600000'
    db = dataset.connect(args.database, schema=(None if not args.schema else args.schema))
    g = Grafana(url=args.url, folder_name=args.folder, api_key=args.api_key)
    t = GrafanaTester(grafana=g, database=db)
    end_time = datetime.fromisoformat(args.end_time)
    for interval in args.intervals.split(','):
        t.test(end_time - get_time_delta(interval), end_time, interval, args)


def get_time_delta(duration):
    match = re.fullmatch(r'(\d+)([smhd])', duration)
    if match:
        n = int(match.group(1))
        u = match.group(2)
        return timedelta(
            seconds=u == 's' and n,
            minutes=u == 'm' and n,
            hours=u == 'h' and n,
            days=u == 'd' and n)
    else:
        raise Exception(f'Invalid test time duration: {duration}')


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
            'default': os.environ.get('GRAFANA_URL'), 'required': 'GRAFANA_URL' not in os.environ})
    api_key = (['-k', '--apikey'],
               {'dest': 'api_key', 'help': 'API key for Grafana authentication; env: GRAFANA_KEY',
                'default': os.environ.get('GRAFANA_KEY'),
                'required': 'GRAFANA_KEY' not in os.environ})
    folder = (['-f', '--folder'],
              {'dest': 'folder', 'help': 'name of Grafana folder; env: GRAFANA_FOLDER',
               'default': os.environ.get('GRAFANA_FOLDER'),
               'required': 'GRAFANA_FOLDER' not in os.environ})
    disk_directory = (['-d', '--directory', '--disk-directory; env: GRAFANA_DIR'],
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
               {'dest': 'include_globs', 'help': 'glob patterns to match for included dashboards'})
    exclude = (['--exclude'],
               {'dest': 'exclude_globs', 'help': 'glob patterns to match for exclude dashboards'})
    match_on = (['--match-on'],
                {'dest': 'match_on', 'help': 'how to match dashboards for inclusion/exclusion',
                 'choices': ['title', 'uid', 'both'], 'default': 'uid'})
    test_intervals = (['--intervals'],
                      {
                          'help': 'test interval durations e.g. 30m,1h,4h,1d; env GRAFANA _TEST_INTERVALS',
                          'default': os.environ.get('GRAFANA_TEST_INTERVALS', '1h,2h,4h,8h,24h'),
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
                        'choices': ['RUN', 'EXPLAIN', 'ANALYZE', 'VERBOSE'], 'default': 'ANALYZE'})
    test_var_value_count = (['--test-var-value-count'],
                            {'dest': 'var_value_count', 'type': int, 'default': 1,
                             'help': 'how many of the available vars to include for "many" vars'})
    test_label = (['--label'],
                  {'dest': 'label', 'help': 'label column value for performance records',
                   'default': '-none-'})
    test_trial = (['--trial'],
                  {'dest': 'trial',
                   'help': 'trial id to link multiple test executions for reporting',
                   'type': int, 'default': os.environ.get('GRAFANA_TEST_TRIAL'),
                   'required': 'GRAFANA_TEST_TRIAL' not in os.environ})
    log_level = (['-l', '-log-level'],
                 {'dest': 'loglevel', 'help': 'logging level', 'default': 'INFO',
                  'choices': ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']})

    def add_to_parser(self, parser):
        """Add this arg specification to a parser"""
        parser.add_argument(*self.value[0], **self.value[1])


def main():
    parser = argparse.ArgumentParser(
        description='Grafana Utilities', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    subparsers = parser.add_subparsers(dest='command', required=True)
    load_parser = subparsers.add_parser(
        Command.load.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.disk_directory, Arg.disk_format,
                Arg.overwrite, Arg.clear, Arg.include, Arg.exclude, Arg.match_on, Arg.log_level]:
        arg.add_to_parser(load_parser)

    save_parser = subparsers.add_parser(
        Command.save.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.disk_directory, Arg.disk_format,
                Arg.overwrite, Arg.clear, Arg.include, Arg.exclude, Arg.match_on, Arg.log_level]:
        arg.add_to_parser(save_parser)

    lint_parser = subparsers.add_parser(
        Command.lint.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.include, Arg.exclude, Arg.match_on,
                Arg.log_level]:
        arg.add_to_parser(lint_parser)

    test_parser = subparsers.add_parser(
        Command.test.name, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    for arg in [Arg.url, Arg.api_key, Arg.folder, Arg.database, Arg.schema, Arg.include,
                Arg.exclude, Arg.match_on, Arg.test_intervals, Arg.test_end_time, Arg.log_level,
                Arg.test_panels, Arg.test_panel_mode, Arg.test_var_value_count,
                Arg.test_label, Arg.test_trial]:
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
