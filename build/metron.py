#!/usr/bin/env python

import argparse
import contextlib
import datetime
import fileinput
import getpass
import logging
import logging.handlers
import os
import platform
import random
import re
import socket
import subprocess
import json
import tarfile
import logging
import logging.handlers
import platform
import fileinput
import urllib2
import zipfile
import shutil
import httplib
import re

SERVICES = [
    'influxdb',  # The influxdb service. Metrics are stored to influx.
    'ml-datastore'  # The machine learning datastore component.
    # Receives topology broadcasts and writes them to influx.
]
DOCKER_COMMAND = 'docker'
DOCKER_COMPOSE_COMMAND = "docker-compose"

VERSION_FILE = '/etc/docker/turbonomic_info.txt'
ENV_FILE_PATH = '/etc/docker/.env'  # Only exists in production
VERSION_LINE_PREFIX = 'Version: XL '
DUMP_DIR = '/var/lib/docker/volumes/docker_influxdb-dump/_data'
PROMETHEUS_METRICS_DIR = '/tmp/metron_scraped_metrics'
PROMETHEUS_METRICS_ZIP_FILE = 'prometheus-metrics.zip'
PROMETHEUS_METRICS_ZIP_PATH = '/tmp/{}'.format(PROMETHEUS_METRICS_ZIP_FILE)
PROMETHEUS_METRICS_SERVICE_SKIP = ['rsyslog']  # don't collect prometheus metrics from these services.
DIAGS_URL = 'https://upload.vmturbo.com/appliance/cgi-bin/vmtupload.cgi'
LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'
CURLRC_FILE_PATH = os.path.expanduser("~") + '/.curlrc'
EXAMPLE_URL = "http://www.example.com"  # To test proxy server connectivity
PROXY_CONNECT_TIMEOUT_SECS = 10

# Command to upload Metron data; for use in crontab in auto-upload mode
# Note: The absolute path of this command comes from the main function and will be added when the
# cron job is set up.
#
UPLOAD_CMD = 'metron.py export --clean --upload --customername='


def bring_up_service(service_name):
    """
    Bring up a Metron service.

    :param service_name: The name of the service to start.
    """
    if is_service_alive(service_name):
        logging.info("Service {} is already running. Skipping startup...".format(service_name))
    else:
        logging.info("Starting service {}".format(service_name))
        docker_compose(['up', '-d', service_name])


def bring_down_service(service_name):
    """
    Bring down a Metron service.

    :param service_name: The name of the service to stop.
    """
    if is_service_alive(service_name):
        logging.info("Stopping service {}".format(service_name))
        docker_compose(['stop', service_name])
    else:
        logging.info("Service {} is already stopped".format(service_name))


def is_service_alive(service_name):
    """
    Check if a particular service is running in docker.

    :param service_name: The name of the service to check.
    :return: True if the service is alive, False otherwise
    """
    docker_ps_output = subprocess.check_output([DOCKER_COMMAND, 'ps'])

    # Surround with underscores so that only strings such as
    # "build_ml-datastore_1" should match.
    return "_{}_".format(service_name) in docker_ps_output


def docker_compose(cmd):
    """
    Shell out to docker-compose.

    :param cmd: The command to ask docker-compose to execute.
    :return: The return code of the docker-compose command.
    """
    call_shell_cmd([DOCKER_COMPOSE_COMMAND] + cmd)


def service_ip_addresses():
    """
    Composes a dictionary of service names to their associated ip addresses.

    :return: A  dictionary of service names to their associated ip addresses.
    """
    docker_ps_output = subprocess.check_output([DOCKER_COMMAND, 'ps'])
    lines = docker_ps_output.strip().split("\n")

    # Lines are in the format (first field is always container id, last is always the name):
    # CONTAINER ID    IMAGE        COMMAND           CREATED       STATUS        PORTS     NAMES
    lines.pop(0)  # Remove the header line
    name_to_id = {}
    ip_addresses = {}

    for line in lines:
        fields = re.split(" {2,}", line)
        # name is in the format build_topology-processor_1. Pull out the 'topology-processor' bit.
        name = fields[-1].split('_')[1]
        if name not in PROMETHEUS_METRICS_SERVICE_SKIP:
            name_to_id[name] = fields[0]

    # Parse the ip address for the service from the docker-inspect output.
    for name, docker_id in name_to_id.items():
        docker_inspect_output = subprocess.check_output([DOCKER_COMMAND, 'inspect', docker_id])
        inspect_json = json.loads(docker_inspect_output)
        networks = inspect_json[0]['NetworkSettings']['Networks']
        network_key = next(key for key in networks.keys() if key.endswith('_default'))
        ip_addresses[name] = networks[network_key]['IPAddress']

    return ip_addresses


def call_shell_cmd(cmd, shell=False):
    """
    Execute a command in the external OS environement.

    If the 'shell' operand is True, then the command will run in a shell rather than a simple
    fork/exec.

    The current working directory for the command will be the same as the diretory in which this
    script resides.

    Catches KeyboardInterrupt and returns.
    """
    try:
        retcode = subprocess.call(cmd, shell=shell, cwd=cwd)
        if retcode != 0:
            raise OSError("error return %s from command '%s'" % (retcode, cmd))
    except KeyboardInterrupt:
        logging.info("\n\nok")


def remove_export_cron():
    """
    Remove any export cron job - the remove command is adapted from:
    https://askubuntu.com/questions/408611/how-to-remove-or-delete-single-cron-job-using-linux-command
    """
    cmd = 'crontab -l | grep -v "{}" | crontab -'.format(UPLOAD_CMD)
    call_shell_cmd(cmd, shell=True)


def add_export_cron(interval, customer_name, scrape_prometheus):
    """
    Add an export cron job - the add command is adapted from:
    https://stackoverflow.com/questions/4880290/how-do-i-create-a-crontab-through-a-script

    :param interval: Export/upload interval
    :param customer_name: Name of the customer which would be part of the export/upload filename
    """
    hour = random.randint(0, 5)  # 12AM to 4AM only
    min = random.randint(0, 60)
    logging.info("Creating a cron job to auto-upload Metron data every {} day(s) at {}:{}. "
                 "Scrape prometheus={}".format(
        interval, hour, min, scrape_prometheus))
    #
    # Remove any old one before adding
    #
    remove_export_cron()
    prometheus_arg = ' -p' if scrape_prometheus else ''
    cmd = '(crontab -l 2>/dev/null; echo "{} {} */{} * * {}/{}{}{}") | crontab -'.format(
        min, hour, interval, cwd, UPLOAD_CMD, customer_name, prometheus_arg)
    call_shell_cmd(cmd, shell=True)


def metron_control(args):
    """
    Control Metron components.

    :param args: The command-line specified arguments.
    """
    if args.down:
        logging.info("Bringing down Metron...")

        # Set the METRON_ENABLED flag to false so no Metron components will start up anymore.
        os.environ['METRON_ENABLED'] = 'false'

        map(bring_down_service, SERVICES[::-1])  # Bring down services in reverse order they were started
        persist_metron_state('false')

        remove_export_cron()

    elif args.up:
        if args.autoupload and args.customername is None:
            logging.error("Customer name has to be specified with auto-upload option.")
            exit(1)

        logging.info("Bringing up Metron...")

        # Set the METRON_ENABLED flag to true so no Metron components are able to start up.
        os.environ['METRON_ENABLED'] = 'true'

        map(bring_up_service, SERVICES)
        persist_metron_state('true')

        if args.autoupload:
            add_export_cron(args.uploadinterval, args.customername, args.scrape_prometheus)


def persist_metron_state(enabled_state):
    """
    Persist enabling/disabling metron in a persistent fashion by changing the value in the .env file.

    :param enabled_state: 'true' or 'false' to enable/disable
    """
    if os.path.isfile(ENV_FILE_PATH):
        logging.info("Saving change 'METRON_ENABLED={}' to '{}'".format(enabled_state, ENV_FILE_PATH))
        # redirects STDOUT to the file ENV_FILE_PATH
        for line in fileinput.input(ENV_FILE_PATH, inplace=1):
            print re.sub("^METRON_ENABLED=.*", "METRON_ENABLED={}".format(enabled_state), line),
    else:
        logging.info("Skipping save of 'METRON_ENABLED={}' to '{}'".format(enabled_state, ENV_FILE_PATH))


def scrape_metrics_for_service(service_name, service_ip):
    try:
        url = 'http://{}:8080/metrics'.format(service_ip)
        response = urllib2.urlopen(url)
        metrics = response.read()
        response.close()

        with open("{}/{}-metrics.txt".format(PROMETHEUS_METRICS_DIR, service_name), "w") as text_file:
            text_file.write(metrics)

        return service_name
    except urllib2.URLError:
        pass
    except httplib.BadStatusLine:
        logging.exception("BadStatusLine when retrieving metrics for {} at '{}':".format(service_name, url))
    return None


def scrape_component_prometheus_metrics():
    """
    Scrape individual component prometheus metrics and write them into files in the METRICS_DIR.
    """
    try:
        if not os.path.exists(PROMETHEUS_METRICS_DIR):
            logging.info("Attempting to make prometheus metrics directory {}".format(PROMETHEUS_METRICS_DIR))
            os.mkdir(PROMETHEUS_METRICS_DIR)

        service_ips = service_ip_addresses()
        logging.info("Scraping metrics for services: {}".format(service_ips.keys()))
        services_with_metrics = map(scrape_metrics_for_service, service_ips.keys(), service_ips.values())
        services_with_metrics = filter(None, services_with_metrics)

        with zipfile.ZipFile(PROMETHEUS_METRICS_ZIP_PATH, 'w') as zip:
            for metrics_file in os.listdir(PROMETHEUS_METRICS_DIR):
                filename = "{}/{}".format(PROMETHEUS_METRICS_DIR, metrics_file)
                zip.write(filename, arcname='/prometheus-metrics/{}'.format(metrics_file))
        logging.info("Created prometheus metric zip file {} containing metrics for the components {}"
                     .format(PROMETHEUS_METRICS_ZIP_PATH, services_with_metrics))
    except OSError:
        logging.exception("Error scraping prometheus metrics.")
    except IndexError:
        logging.exception("Error scraping prometheus metrics.")
    except Exception:
        logging.exception("Unexpected error while scraping prometheus metrics.")


def metron_export(args):
    """
    Attempt to export data from Metron.

    :param args: The command-line specified arguments.
    """
    if args.filename:
        export_filename = args.filename
    else:
        customer_name = args.customername
        domain_name = socket.gethostname()
        host_ip = socket.gethostbyname(domain_name)
        version = get_version()
        datetime_now = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        export_filename = 'metron_data-' + customer_name + '-' + host_ip + '-' + datetime_now + '_' + version + '.tar'

    export_file_fullpath = "/tmp/" + export_filename
    logging.info("Attempting to export Metron data")
    logging.info("...from directory '{}'".format(DUMP_DIR))
    logging.info("...to a tar file '{}'\n".format(export_file_fullpath))

    if os.path.isdir(DUMP_DIR) is False:
        logging.error("Expected Metron data directory '{}' does not exist; exiting without "
                      "export".format(DUMP_DIR))
        logging.error("...This script is expected to run on the XL VM (the docker host)")
        logging.error("...If you are running elsewhere for testing purpose, you can create the "
                      "data directory in your environment")
        logging.error("......but keep in mind that influxdb will not actually back up data there.")
        logging.error("...If you are on the XL VM, please check if the data directory is mounted "
                      "properly from the influxdb container:")
        logging.error("......data directory on the docker host: {}".format(DUMP_DIR))
        logging.error("......data directory on the influxdb container: /home/influxdb/influxdb-dump")
        exit(1)

    # Scrape prometheus metrics into a zip file in the /tmp directory.
    if args.scrape_prometheus:
        scrape_component_prometheus_metrics()

    try:
        # Generate a tar file for everything in the given dump directory.  Adopted from:
        # https://stackoverflow.com/questions/2032403/how-to-create-full-compressed-tar-file-using-python
        #
        with tarfile.open(export_file_fullpath, "w") as tar:
            tar.add(DUMP_DIR, arcname=os.path.sep + 'metron_data')
            tar.add(PROMETHEUS_METRICS_ZIP_PATH, arcname=os.path.sep + PROMETHEUS_METRICS_ZIP_FILE)
            logging.info("Metron data has been exported to {}".format(export_file_fullpath))
            fnames = tar.getnames()
            tar.close()

        # Remove the metrics directory that may have been created earlier. Ignore any errors that may occur
        # because this cleanup is just hygiene and an error is not that harmful.
        shutil.rmtree(PROMETHEUS_METRICS_DIR, ignore_errors=False)
        if os.path.exists(PROMETHEUS_METRICS_ZIP_PATH):
            os.remove(PROMETHEUS_METRICS_ZIP_PATH)

        if args.upload:
            logging.info("Starting to upload the metron data {}...".format(export_file_fullpath))
            # The -f flag below is important here to ensure curl returns a failure upon any http error
            call_shell_cmd(["curl", "-fF", "ufile=@" + export_file_fullpath, DIAGS_URL])
            logging.info("Metron data has been uploaded; deleting the uploaded tar file...")
            os.remove(export_file_fullpath)
            logging.info("The exported tar file has been deleted")

        if args.clean:
            logging.info("Deleting the original data dump files...")
            for fname in fnames:
                #
                # This list includes the dot directory, which is not to be deleted and will
                # be filtered out in the following if statement.
                #
                if fname:
                    os.remove(os.path.join(DUMP_DIR, fname))

            logging.info("Original data dump files have been deleted.")

    except OSError as err:
        logging.error("Error in exporting the Metron data dump: {}".format(err))


def get_version():
    """
    Parse the version file and return the extracted version number.

    :return: The extracted version number or "unknown.version" if not found
    """
    if (os.path.isfile(VERSION_FILE)):
        # parse the file and grab the version number
        with open(VERSION_FILE, 'r') as f:
            for line in f:
                if line.startswith(VERSION_LINE_PREFIX):
                    return line[len(VERSION_LINE_PREFIX):-1]

    logging.info("Version file '{}' not found or version line prefix '{}' not found in the "
                 "file".format(VERSION_FILE, VERSION_LINE_PREFIX))
    logging.info("...using 'unknown.version' as version number\n")
    return "unknown.version"


def metron_proxy(args):
    """
    Add or remove proxy configuration in the .curlrc file.

    :param args: The command-line specified arguments.
    """
    host_port = None
    user_pass = None
    if args.add:
        if (args.host is None) or (args.port is None):
            logging.error("Both host and port are required when configuring proxy")
            exit(1)

        if (args.protocol is not 'http') and (args.protocol is not 'https'):
            logging.error("Unsupported protocol {} to access proxy server".format(args.protocol))
            exit(1)

        host_port = "{}:{}".format(args.host, args.port)
        if args.user:
            # Let user enter the password if username is specified
            password = getpass.getpass('Password:')
            user_pass = "{}:{}".format(args.user, password)

        if not is_good_proxy(args.protocol, host_port, user_pass):
            logging.error("Proxy configuration aborted; no changes have been made to {}".format(
                CURLRC_FILE_PATH))
            exit(1)

    # Delete any previous proxy configuration in .curlrc
    #
    # The implementation below is adopted from the 2nd solution here:
    # https://stackoverflow.com/questions/4710067/using-python-for-deleting-a-specific-line-in-a-file
    #
    with open(CURLRC_FILE_PATH, 'r+') as f:
        lines = f.readlines()
        f.seek(0)
        for line in lines:
            if not line.startswith('proxy'):
                f.write(line)
        f.truncate()

        if args.add:
            proxy_line = "proxy = {}\n".format(host_port)
            f.write(proxy_line)
            if user_pass:
                proxy_user_line = "proxy-user = {}\n".format(user_pass)
                f.write(proxy_user_line)
            logging.info("Proxy has been configured in {}".format(CURLRC_FILE_PATH))
        else:
            logging.info("Proxy configuration in {} has been removed.".format(CURLRC_FILE_PATH))


def is_good_proxy(protocol, host_port, user_pass=None):
    """
    Check if the specified proxy is good by trying to reach out to www.example.com.

    Adopted from: https://stackoverflow.com/questions/765305/proxy-check-in-python
    and https://github.com/ApsOps/proxy-checker/blob/master/proxy_check.py

    :param protocol: The protocol to access the proxy server, i.e. http or https
    :param host_port: The host/port of the proxy server in the form of <host>:<port>
    :param user_pass: Username and password of the proxy server in the form of <username>:<password>
                      None if not specified
    :return True if successfully accessing www.example.com via the specified proxy; False otherwise
    """
    try:
        # Construct the full proxy URL
        #
        proxy_url = "{}://{}".format(protocol, host_port) if user_pass is None\
            else "{}://{}@{}".format(protocol, user_pass, host_port)

        proxy_handler = urllib2.ProxyHandler({protocol: proxy_url})
        opener = urllib2.build_opener(proxy_handler)
        urllib2.install_opener(opener)
        req = urllib2.Request(EXAMPLE_URL)
        #
        # The with statement and the contextlib.closing() library ensure closing of the established
        # connection; see
        # https://stackoverflow.com/questions/1522636/should-i-call-close-after-urllib-urlopen
        #
        with contextlib.closing(urllib2.urlopen(req, timeout=PROXY_CONNECT_TIMEOUT_SECS)):
            pass
    except urllib2.URLError, e:
        logging.error("Bad proxy {}; cannot access {}: {}".format(proxy_url, EXAMPLE_URL, e))
        return False

    logging.info("Proxy access has been verified")
    return True


if __name__ == "__main__":
    # Configure logging to a file if running outside a development environment. Note that this is a poor
    # proxy but is simple.
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    if platform.system() != 'Darwin':  # 'Darwin' is what MacOS shows up as in python.
        # Log both to a file and to stdout
        logging.getLogger().addHandler(logging.handlers.SysLogHandler(address='/dev/log'))

    script_path = os.path.realpath(__file__)
    cwd = os.path.dirname(os.path.abspath(script_path))

    p = argparse.ArgumentParser(description='Interact with Metron-related (machine learning) components.')

    # Mutually exclusive group for bringing Metron up, down, and exporting data.
    subparser = p.add_subparsers(help='Control Metron components or export Metron data.')
    control_parser = subparser.add_parser('control', help='Bring metron-related components up or down.')
    group = control_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-d', '--down', action='store_true', help="If down is specified, will shut down Metron.")
    group.add_argument('-u', '--up', action='store_true', help="If up is specified, will bring up Metron.")
    control_parser.add_argument('-a', '--autoupload', action='store_true',
                                help="Enable to auto-upload Metron data")
    control_parser.add_argument('-i', '--uploadinterval', type=int, choices=range(1, 366),
                                metavar='[1-365]', default=7,
                                help="Specify the upload interval in days [1-365]; default=7 (1 week)")
    control_parser.add_argument('-n', '--customername',
                                help='Customer name as part of the constructed upload file name')
    control_parser.add_argument('-p', '--scrape-prometheus', action='store_true', default=False,
                                help='Boolean flag to indicate whether to scrape prometheus')
    control_parser.set_defaults(func=metron_control)

    export_parser = subparser.add_parser('export', help='Export Metron metric data.')
    export_name_parser_group = export_parser.add_mutually_exclusive_group(required=True)
    export_name_parser_group.add_argument('-f', '--filename',
                                          help='Filename to export data to. If no file name is '
                                               'specified, exported filename will be constructed')
    export_name_parser_group.add_argument('-n', '--customername',
                                          help='Customer name as part of the constructed export '
                                               'file name')
    export_parser.add_argument('-u', '--upload', action='store_true',
                               help='Boolean flag to indicate we would upload Metron data to turbo')
    export_parser.add_argument('-c', '--clean', action='store_true',
                               help='Boolean flag to indicate we would delete the data dump files')
    export_parser.add_argument('-p', '--scrape-prometheus', action='store_true', default=False,
                               help='Boolean flag to indicate whether to scrape prometheus')
    export_parser.set_defaults(func=metron_export)

    proxy_parser = subparser.add_parser('proxy',
                                        help='Add or remove proxy configuration in .curlrc file.')
    proxy_group = proxy_parser.add_mutually_exclusive_group(required=True)
    proxy_group.add_argument('-r', '--remove', action='store_true',
                             help="Remove any proxy configuration.")
    proxy_group.add_argument('-a', '--add', action='store_true',
                             help="Add a new or replace the existing proxy configuration.")
    proxy_parser.add_argument('--protocol', default="http", help="Proxy protocol; default=http")
    proxy_parser.add_argument('--host', help="Proxy host")
    proxy_parser.add_argument('--port', default="8080", help="Proxy port; default=8080")
    proxy_parser.add_argument('--user', help="Proxy user; enter password at prompt")
    proxy_parser.set_defaults(func=metron_proxy)

    args = p.parse_args()
    args.func(args)
