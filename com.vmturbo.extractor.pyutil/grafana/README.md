# Grafana Dashboard Utility

The `grafana.py` python script in this directory supports command-line capabilities for working with
grafana dashboards, including:

* Loading dashboard definitions from a local directory into a grafana dashboard folder
* Saving dashboard defintions from a grafana folder into a local directory
* Linting dashboard definitions from a grafana folder (i.e. checking for common errors in dashboard
  definitions)
* Running performance tests for dashboard queries in a given grafana folder. Test results are
  recorded in the same DB server as was used to execute the queries, in a form intended to be
  visualized in grafana.

## API Keys

The script uses a Grafana API key to authenticate to Grafana for API operations. You will need to
create an active key in Grafana prior to using the tool.

The API key can be specified on the script command line, or it can be set in an environment
variable, `GRAFANA_KEY`. Many (but not all) of the script options work in this fashion. Environment
variable names are listed with the corresponding options in the help output for the individual
commands For example:

```
grafana.py  load -h
usage: grafana.py load [-h] [-u URL] [-k API_KEY] [-f FOLDER] [-d DIRECTORY] [--disk-format {YAML,JSON}] [-o] [-c] [--include INCLUDE_GLOB] [--exclude EXCLUDE_GLOB]
                       [--match-on {title,uid,both}]

optional arguments:
  ...
  -k API_KEY, --apikey API_KEY
                        API key for Grafana authentication; env: GRAFANA_KEY
   ...
```

## Loading Dashboards

For full option details, say `python3 grafana.py load -h`.

Dashboards can be loaded from a single directory on your local computer to a single folder in your
Grafana dashboards collection. The folder must already exist in Grafana. (We would like to have an
auto-create option.)
The local directory and the Grafana folder do not have to have the same name.

Local dashboard files can appear in either YAML or JSON format, but you cannot process a mixed
collection - at least not in a single load operation. The on-disk format can be specified
with `--disk-format YAML` or
`--disk-format JSON`, defaulting to `YAML`. Folder and permissions files within a local dashboard
directory are currently ignored. The `YAML` default is strongly recommended, because YAML is capable
of storing multi-line strings in a multi-line format (block literals). Besides making the files
generally more readable, this leads to a profound improvement in the experience of reviewing SQL
query changes in Review Board.

If the `--overwrite` option is used, uploaded dashboards will replace existing dashboards with the
same `uid`, if they exist. Otherwise, if any loaded dashboard already exists, the loading of that
dashboard will fail, and the overall load operation will stop at that point. Grafana's error message
in this case will be the somewhat dubious message,
"The dashboard has been changed by someone else."

A `--clear` option is listed in the command help, but it is not implemented. The intention is that
it will cause all dashboards to be removed from the target folder prior to commencement of the
operation.

The `--include`, `--exclude` and `--match-on` options can be used to selectively load only some of
the dashboards present in the source directory. The `--include` and `--exclude` options supply "file
glob" patterns that are matched against dashboard UIDs or titles (or both), depending on
the `--match-on` option. For both `--include` and `--exclude`, the argument can be a single glob
pattern or a comma-separated list of glob patterns (with no intervening spaces that are not intended
to be part of a pattern). There is not presently a way to quote commas so that they are treated as
part of a glob pattern.

A dashboard is loaded if:

* There is no include and no exclude pattern.
* There is no include pattern, and the dashboard does not match the exclude pattern.
* The dashboard matches the include pattern, and there is no exclude pattern.
* The dashboard matches the include pattern and does not match the exclude pattern.

## Saving Dashboards

For full option details, say `python3 grafana.py save -h`.

This operation is the reverse of the load operation; dashboards appearing in a named Grafana folder
are saved as individual files in a designated local file directory.

Dashboards are saved in either `.yaml` or `.json` files, with file names that are identical to final
component of the dashboard `uid`, as if it were a file path. For example, if the `uid` is "foo," the
saved file name will be named `foo.yaml`. If the `uid` is "foo/bar," the saved file name will
be `bar.yaml`. This makes it possible to prefix all the dashboard `uid` values in a folder with a
common unique prefix, to prevent uid collisions in Grafana and still have readable on-disk file
names. For example, if the dashboard `uid` is "asdfsd_=3224)*/my_cool_dashboard", the saved file
name will be `my_cool_dashboard.yaml`.

Most of the available options operate the same as they do with the `load`
operation, however there are some differences:

* `--overwrite`, if present, causes any existing dashboard files to be renamed to time-stamped
  backup (`.bak`) files before the new files are saved. Existing files in the target directory that
  are NOT named the same as files to be written by the save operation remain unchanged.
  **N.B.**: In particular, if your current dashboard files are not named after the dashboard `uid`
  values, those existing files will appear side-by-side with the newly written `uid`-named files
  after the save operation completes. You are strongly advised to name all your dashboard files
  using the `uid`
  convention, to avoid confusion.
* `--clear` is not implemented, but its intent is to remove all dashboard files prior to starting
  the operation. A "dashboard file" is any file that:
    * Has the file extension corresponding to the `--disk-format` option
    * Is not named `folder` or `permissions`

**N.B.**: YAML includes a syntax for comments, but JSON does not. Grafana's API conveys dashboards
strictly using JSON, so on-disk dashboard files created by the save operation will never contain
comments, even if they are saved in YAML format. This means that round-tripping an on-disk dashboard
definition (loading then saving) will strip the definition of its comments (along with the potential
for other superficial changes). Comments in dashboard `.yaml` files are discouraged for this reason,
unless/until we can devise some means of preserving them.

## Linting Dashboards

For full option details, say `python3 grafana.py lint -h`.

The lint operation performs certain checks on dashboards appearing in a named Grafana folder. The
operation cannot be directed to check on-disk dashboard files; you must load the dashboards into a
grafana instance, and then lint them there.

The lint operation currently performs these checks:

* Every query variable (`type: query`) must be set to refresh on time-range change (`refresh: 2`),
  and must be configured with a datasource specified by a variable (`datasource: $xxx`).
* If a panel has a data source (`datasource` property), its value should be either a variable
  reference (`$xxx`) or the fixed value `-- Dashboard --`
  (indicating, I believe, a reference to another dashboard to re-use its queries).

Feel free to extend this list, so we're better protected against deploying mis-configured
dashboards.

## Testing Dashboards

For full option details, say `python3 grafana.py test -h`

The test operation extracts database queries from one or more dashboards and executes those queries
in a way that closely mimics execution by grafana. Performance information gathered from these
executions is then recorded in the database for subsequent analysis. A few grafana dashboards are
available for perusing the performance data, including performance plots of one or more queries, as
well as A/B comparisons to assess the benefits of schema changes or query rewrites, as well as
performance under various usage scenarios.

### Basic Operation

The test operation proceeds as follows:

* Dashboards specified for the test are retrieved from a live grafana instance.
* The dashboards are scraped for their database queries. This includes queries that provide data for
  panels as well as queries attached to query-backed variables.
* Available values for query-backed variables are retrieved from the databases using their
  associated queries.
* Designated queries are executed on the database, after performing substitutions for variables and
  for time range associated functions.
* Exeuction times are gathered for the executed queries and recorded in the database, with metadata
  associated with the executions.

### Selecting Queries To Test

Queries to be executed are scraped from variable and panel definitions within selected dashboards.
As with other operations, `--include` and `--exclude` may be used to limit the operation to a subset
of available dashboards.

Specific targets may be targeted using the `--test-panels` option, specifying a comma-separated list
of glob patterns, similar to `--include` and `--exclude`. In this case, the patterns are used to
filter panels within the selected dashboards by testing whether a panel's title matches any of the
provided patterns.

Note that there is not currently a way to filter variable queries, which means that all variable
queries appearing in selected dashboards are always tested.

### Specifining Time Ranges

Each query is tested using a sequence of time ranges. Two options control these time ranges:

* `--end-time` is used to specify the common ending time for all the time ranges.
* `--intervals`specifies the durations for all the time ranges. The start times for the ranges are
  computed by subtracting intervals from the common end time.

The intervals are specified as a comma-separated list of interval specification, each of which is an
integer followed by one of the letters `s`, `m`, `h`, or `d`, for seconds, minutes, hours, and days,
respectively.

The default for `--intervals` is `1h,2h,4h,8h,24h`.

### Specifying Other Variable Substitutions

There are two ways values are created for substitution into queries that include variable
references. Both `$var` and `[[var]]` syntaxes are supported, though the `[[var]]` syntax is
deprecated by grafana.

* Individal Value(s): One or more values that would be available for selection in grafana are
  substituted. For multi-value variables, the number of values selected is determined by
  the `--test-var-value-count` parameter, which defaults to 1.

  The selected value(s) are obtained from the front of the list of available values, so in order to
  make such selections repeatable in these tests it may make sense to include an `ORDER BY` clause
  in corresponding variable queries in the dashboard definition. Any sort order specified in the
  variable definition appearing in the dashboard is ignored.
* All Values: If the variable has a configured "all value," a tested query is always executed with
  that value instead of explicit values available for the variable.

If a query depends on multiple variables, it is still executed at most twice per interval - once
with explicit variable values, and once with "all" values for variables that define them. There is
no way to execute a test with mixtures of all and explicit values for different variables.

### Controlling Query Execution

The `--test-panel-mode` determines the manner of query execution:

* `RUN`: Queries are executed as-is. It is not clear that this is helpful in any way, for reasons
  described below.
* `EXPLAIN`: Queries are prefixed with `EXPLAIN`, so they are analyzed by the database, and
  execution plans are produced, but the queries are not actually executed.
* `ANALYZE`: Queries are prefixed with `EXPLAIN ANALYZE`, so they are analyzed by the database and
  also executed to produce results. Exeuction plans and exeuction time is captured and recorded.
* `VERBOSE`: Queries are prefixed with `EXPLAIN ANALYZE VERBOSE`. This is just like `ANALYZE`, but
  the query plan includes additional information that can be helpful when assessing plans.

Note that, currently, execution times are scraped from analysis output that appears with `ANALYZE`
and `VERBOSE` output. Use of other options will therefore result in performance data written to the
database that lacks execution times.

### Organizing Tests

Two options can be used to organize tests for subsequent analysis:

* `--trial` specifies a trial number that can be used to collect multiple test executions under the
  umbrella of an overall collection that are intended to be analyzed together. A single trial
  probably occurs over a period of time where various tests are run, and eventually the trial ends.
* `--label` provides a common label for some of the tests performed in a given trial. This is
  helpful when conducing A/B tests, comparing performance of queries under various schema options
  (e.g. indexing changes) and/or query implementations. In such a scenario, one label would be used
  for A tests, and another for B tests.

### Viewing Test Results

The `com.vmturbo.extractor/util/dashboards` directory includes a handful of dashboards that can be
used to view results of performance tests:

* `performance_overview` can be used to view performance of queries in a selected trial/label
  combination. Each tested query gives rise to one or two time series (two if there are any
  variables with all values), with the "time" values for the series corresponding to the time range
  intervals used in the executions.

  Grafana does not understnad "intervals" as time values, so in reality, each interval is converted
  to a time value based on a built-in start time of `2019-12-31 00:00:00` (the actual date is not
  important, but the choice of midnight is). Each exeuction's "time" value is computed by adding the
  interval to that fixed start time. As a result, exeuction times are plotted with an X-axis that
  more or less displays intervals in an intuitive fashion. This isn't perfect, but it's the best
  we've come up with given grafana's very limited x-axis control.

  Besides the panel that shows the performance data graphically, this dashboard also includes a
  table panel that lists detailed data for all selected exeuctions.

* `a_b_perf_comparision` can be used in A/B testing scenarios. This has two graph panels that are
  essentially identical to the one in `performance_overview`. These two panels show the A and B
  data, respectively. The dashboard includes two "label" drop-downs, which can be used to designate
  different labels for the A and B panels.

  As with `performance_overview`, the dashboard also provides a table panel that displays details
  for all the exeuctions. A and B exections are combined in this panel, and the default sort order
  is arranged so that A rows immediately precede corresponding B rows.

* `query_details` can be used to view queries and plans. A single panel takes the form of a
  one-column table whose rows are the individual lines of text appearing in the query and its
  `EXPLAIN` output. This is not ideal, but it's the best we could come up with for a presentation in
  grafana. The major drawback is line indentation is not visible, making queries and especially
  plans very difficult to understand..

  To get view this information in more useful forms, two options are:

    * In the panel drop-down menu, choose **Inspect** -> **Data**, and then click the **Download
      CSV** button to capture the text in a file that can be viewed in spreadsheet. Indentation of
      individual lines is preserved in this approach.
    * Select one or more lines from the grafana panel itself, and paste into the application of your
      choice. Line indentation is not preserved in this approach.
