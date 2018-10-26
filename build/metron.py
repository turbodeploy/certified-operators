#!/usr/bin/env python

import argparse
import datetime
import os
import random
import socket
import subprocess
import tarfile
import logging
import platform

SERVICES = [
    'influxdb',         # The influxdb service. Metrics are stored to influx.
    'ml-datastore'      # The machine learning datastore component.
                        # Receives topology broadcasts and writes them to influx.
]
DOCKER_COMMAND = 'docker'
DOCKER_COMPOSE_COMMAND = "docker-compose"

VERSION_FILE='/etc/docker/turbonomic_info.txt'
VERSION_LINE_PREFIX='Version: XL '
DUMP_DIR='/var/lib/docker/volumes/docker_influxdb-dump/_data'
DIAGS_URL='https://upload.vmturbo.com/appliance/cgi-bin/vmtupload.cgi'
LOG_FORMAT='%(asctime)s %(levelname)s: %(message)s'

# Command to upload Metron data; for use in crontab in auto-upload mode
# Note: The absolute path of this command comes from the main function and will be added when the
# cron job is set up.
#
UPLOAD_CMD='metron.py export --clean --upload --customername='


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


def add_export_cron(interval, customer_name):
    """
    Add an export cron job - the add command is adapted from:
    https://stackoverflow.com/questions/4880290/how-do-i-create-a-crontab-through-a-script

    :param interval: Export/upload interval
    :param customer_name: Name of the customer which would be part of the export/upload filename
    """
    hour = random.randint(0, 5) # 12AM to 4AM only
    min = random.randint(0, 60)
    logging.info("Creating a cron job to auto-upload Metron data every {} day(s) at {}:{}".format(
        interval, hour, min))
    #
    # Remove any old one before adding
    #
    remove_export_cron()
    cmd = '(crontab -l 2>/dev/null; echo "{} {} */{} * * {}/{}{}") | crontab -'.format(
        min, hour, interval, cwd, UPLOAD_CMD, customer_name)
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

        remove_export_cron()

    elif args.up:
        if args.autoupload and args.customername is None:
            logging.error("Customer name has to be specified with auto-upload option.")
            exit(1)

        logging.info("Bringing up Metron...")

        # Set the METRON_ENABLED flag to true so no Metron components are able to start up.
        os.environ['METRON_ENABLED'] = 'true'

        map(bring_up_service, SERVICES)

        if args.autoupload:
            add_export_cron(args.uploadinterval, args.customername)


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
        logging.error("Expected Metron data directory '{}' does not exist; exiting without export".format(
            DUMP_DIR))
        logging.error("...This script is expected to run on the XL VM (the docker host)")
        logging.error("...If you are running elsewhere for testing purpose, you can create the data "
              "directory in your environment")
        logging.error("......but keep in mind that influxdb will not actually back up data there.")
        logging.error("...If you are on the XL VM, please check if the data directory is mounted properly "
              "from the influxdb container:")
        logging.error("......data directory on the docker host: {}".format(DUMP_DIR))
        logging.error("......data directory on the influxdb container: /home/influxdb/influxdb-dump")
        exit(1)

    try:
        # Generate a tar file for everything in the given dump directory.  Adopted from:
        # https://stackoverflow.com/questions/2032403/how-to-create-full-compressed-tar-file-using-python
        #
        with tarfile.open(export_file_fullpath, "w") as tar:
            tar.add(DUMP_DIR, arcname=os.path.sep)
            logging.info("Metron data has been exported to {}".format(export_file_fullpath))
            fnames = tar.getnames()
            tar.close()

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

    logging.info("Version file '{}' not found or version line prefix '{}' not found in the file".format(
        VERSION_FILE, VERSION_LINE_PREFIX))
    logging.info("...using 'unknown.version' as version number\n")
    return "unknown.version"


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
    control_parser.add_argument('-i', '--uploadinterval', type=int, choices=range(1,366),
                                metavar='[1-365]', default=1,
                                help="Specify the upload interval in days [1-365]; default=1")
    control_parser.add_argument('-n', '--customername',
                                help='Customer name as part of the constructed upload file name')
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
    export_parser.set_defaults(func=metron_export)

    args = p.parse_args()
    args.func(args)
