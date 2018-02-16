#!/usr/bin/python

import argparse
import os
import string
import subprocess

import sys

"""
Command Line function for managing a Turbonomic Platform.

It is assumed that this file lives in the Turbonomic Install directory which includes
- docker-compose.yml
- turbonomic_info.txt
- turbonomic_sums.txt
"""

DOCKER_COMMAND = "docker"
DOCKER_COMPOSE_COMMAND = "docker-compose"

def main():
    """
    Parse the arguments and call the designated sub-command.
    """
    parsed_args = parse_args()
    parsed_args.func(parsed_args)


def version_display(parsed_args=None):
    """
    Display information about this Turbonomic installation.

    Also include the versions and checksums of each individual component.
    """
    turbo_info_path = cwd + '/turbonomic_info.txt'
    turbo_sums_path = cwd + '/turbonomic_sums.txt'
    print "\n\nTurbonomic Build Information"
    try:
        with open(turbo_info_path, 'r') as info_file:
            content = info_file.read()
            print content
    except IOError:
        print "Error: %s not found" % turbo_info_path

    print "\n------------------\n"

    print "Images in this Distribution"
    try:
        with open(turbo_sums_path, 'r') as info_file:
            content = info_file.read()
            print content
    except IOError:
        print "Error:  %s not found" % turbo_sums_path
    print ""


def stats_display(parsed_args):
    """
    Display the dynamic image stats for one or more images.

    Refresh continually until the user types ^C
    """
    component_names = get_selected_component_names(parsed_args.components)
    call_shell_cmd([DOCKER_COMMAND, "stats"] + component_names)


def shell_to_component(parsed_args):
    """
    Execute a shell command on the target component.

    If no command is given, execute 'bash' by default
    """
    component = parsed_args.component
    command_to_exec = parsed_args.command_to_exec if len(parsed_args.command_to_exec) > 0 else \
        ["bash"]

    call_shell_cmd([DOCKER_COMPOSE_COMMAND, "exec", component] +
                   command_to_exec)

def execute_mysql_command(parsed_args):
    """
    Execute command as an SQL command on the DB component
    """
    command_to_exec = parsed_args.command_to_exec
    user = parsed_args.user
    password = parsed_args.password
    database = parsed_args.database

    call_shell_cmd([DOCKER_COMPOSE_COMMAND, "exec", "db", "mysql", "-u" + user, "-p" + password,
                   "-D" + database, "-e", " ".join(command_to_exec)])

def logs_display(parsed_args):
    """
    Display the debug logs of one or more components on the console.

    Filters the output of the 'rsyslog' component. If no components are specified, then
    all components' output is shown.

    If the "--tail <n>" argument is given, only the last 'n' lines of rsyslog output are filtered
    and displayed.

    If the "-f" argument is given, then the command will continue to display the filtered
    rsyslog output until the user types ^C
    """
    follow_arg = [parsed_args.follow] if parsed_args.follow else []
    tail_arg = ["--tail", parsed_args.tail] if parsed_args.tail else []
    if (len(parsed_args.components)) > 0:
        pattern = "'%s'" % string.join(["^%s" % name for name in parsed_args.components], "\\|")
        grep_command = ["|", "grep", pattern]
    else:
        grep_command = []
    full_command = string.join([DOCKER_COMMAND, "logs"] + follow_arg + tail_arg +
                               ["%s_rsyslog_1" % home] + grep_command, ' ')
    call_shell_cmd(full_command, shell=True)


def start_component(parsed_args):
    """
    Launch a set of components, creating a new docker image for each.

    If no components are specified, then all components will be started.

    If any component is already running, there will be no effect.
    """
    call_shell_cmd([DOCKER_COMPOSE_COMMAND, "up", "-d"] + parsed_args.components)


def stop_component(parsed_args):
    """
    Stop the docker image for a set of components.

    If no components are specified, then all components will be stopped.
    """
    call_shell_cmd([DOCKER_COMPOSE_COMMAND, "stop"] + parsed_args.components)


def restart_component(parsed_args):
    """
    Restart the docker image for a set of components by specifying 'stop' and then 'start'.

    If no components are specified, then all components will be restarted.
    """
    stop_component(parsed_args)
    start_component(parsed_args)


def show_processes(parsed_args):
    """
    Show the docker container status of a set of components.

    If no components are specified, then all components will be listed.
    """
    call_shell_cmd([DOCKER_COMPOSE_COMMAND, "ps"] + parsed_args.components)


def parse_args(args=sys.argv[1:]):
    """
    Set up the subcommand argument parsing structure, including optional arguments.
    """
    parser = argparse.ArgumentParser(prog=os.path.basename(__file__),
                                     description="Turbonomic Platform Control program")

    subparsers = parser.add_subparsers(help="Turbonomic control:  "
                                       "'<subcommand> -h' for details")

    # health
    health_parser = subparsers.add_parser("ps", help="Display List of Running Components:\n"
                                                     "  ps <component*>")
    health_parser.add_argument("components", nargs="*")
    health_parser.set_defaults(func=show_processes)

    # start
    start_parser = subparsers.add_parser("start", help="Start Components:\n"
                                                       "  start <component*>")
    start_parser.add_argument("components", nargs="*")
    start_parser.set_defaults(func=start_component)

    # stop
    stop_parser = subparsers.add_parser("stop",  help="Stop Components:\n"
                                                      "  stop <component*>")
    stop_parser.add_argument("components", nargs="*")
    stop_parser.set_defaults(func=stop_component)

    # restart
    add_parser = subparsers.add_parser("restart", help="Restart Components:\n"
                                                       "  restart <component*>")
    add_parser.add_argument("components", nargs="*")
    add_parser.set_defaults(func=restart_component)

    # version
    version_parser = subparsers.add_parser("version", help="Display Platform Versions:\n"
                                                     "  version")
    version_parser.set_defaults(func=version_display)

    # stats
    stats_parser = subparsers.add_parser("stats", help="Display Component Docker Stats:\n"
                                                       "  stats <component*>")
    stats_parser.add_argument("components", nargs="*")
    stats_parser.set_defaults(func=stats_display)

    # shell
    shell_parser = subparsers.add_parser("shell", help="Open Command Shell on Component:\n"
                                                       "  shell <component> [<command>]\n"
                                                       "default is 'bash'")
    shell_parser.add_argument("component")
    shell_parser.add_argument("command_to_exec", nargs=argparse.REMAINDER)
    shell_parser.set_defaults(func=shell_to_component)

    # sql
    sql_parser = subparsers.add_parser("sql", help="Run an SQL command on the DB Component:\n"
                                                   "  sql [-d <database>] [-u <user>] "
                                                   "[-p <password>] <command>\n"
                                                   "defaults are 'vmtdb', 'root', 'vmturbo' "
                                       )
    sql_parser.add_argument("-d", "--database", default='vmtdb')
    sql_parser.add_argument("-u", "--user", default='root')
    sql_parser.add_argument("-p", "--password", default="")
    sql_parser.add_argument("command_to_exec", nargs=argparse.REMAINDER)
    sql_parser.set_defaults(func=execute_mysql_command)

    # logs
    logs_parser = subparsers.add_parser("logs", help="Display Debug Logs for Components:\n"
                                                     "  logs [-f] [-n <n>] <component*>")
    logs_parser.add_argument("components", nargs="*")
    logs_parser.add_argument("-f", "--follow", dest="follow", action="store_const", const="-f")
    logs_parser.add_argument("-n", "--lines", dest="tail")
    logs_parser.set_defaults(func=logs_display)

    return parser.parse_args(args)


def get_selected_component_names(components):
    """
    Construct the docker image names for each component name in the input list,
    based on the 'home' for the docker-compose.yml file as is done by docker-compose.

    Assume that the docker-compose.yml lives in the same directory as this script.
    Also assume that there is only a single instances of each component.

    For example, if the 'home' folder is "docker", then the docker-compose image name for the API
    component will be "docker_api_1".
    """
    if len(components) == 0:
        component_names = get_all_component_names()
    else:
        component_names = ["%s_%s_1" % (home, component_name) for component_name in components]
    return component_names


def get_all_component_names():
    """
    Fetch the component names of all the running docker images with the docker-compose prefix and
    suffix removed.

    For example, the image name "docker_api_1" will be returned as "api".
    Returns the names as a list.
    """
    docker_ps_output = subprocess.check_output([DOCKER_COMMAND, "ps", "--format",
                                                "'{{.Names}}'"])
    component_name_string = string.rstrip(string.replace(docker_ps_output, "'", ""), "\n")
    component_names = string.split(component_name_string, '\n')
    return component_names


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
        print "\n\nok"


if __name__ == "__main__":
    """
    Set up and run the main() function, as we are called 'from the top level'.

    Determine the directory where this script resides, and the 'home' == the last component of
    the path. This 'home' will be used by docker-compose as the prefix to the image-names as
    they are created.

    Then call the 'main()' function function.
    """
    script_path = os.path.realpath(__file__)
    cwd = os.path.dirname(os.path.abspath(script_path))
    home = os.path.basename(cwd)
    main()
