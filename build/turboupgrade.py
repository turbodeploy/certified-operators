#!/usr/bin/env python

"""
Script to upgrade XL components.

"""

import argparse
import datetime
import errno
import glob
import hashlib
import itertools
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import telnetlib
import time

from distutils import dir_util
from itertools import izip_longest
from logging import handlers
from os.path import join


TURBO_DOCKER_CONF_ROOT = "/etc/docker"
DOCKER_COMPOSE_FILE = join(TURBO_DOCKER_CONF_ROOT, "docker-compose.yml")
TURBO_CHECKSUM_FILE = join(TURBO_DOCKER_CONF_ROOT, "turbonomic_sums.txt")
TURBO_INFO_FILE = join(TURBO_DOCKER_CONF_ROOT, "turbonomic_info.txt")
TURBO_UPGRADE_SPECS_DIR = "upgrade_specs"
TURBO_UPGRADE_SPEC_FILE = "turbo_upgrade_spec.yml"
TURBO_VMTCTL_LOC = "/usr/local/bin/vmtctl"
DOCKER_DIR = "/var/lib/docker/"
ISO_MOUNTPOINT = "/media/cdrom"
UPGRADE_STATUS_FILE = "/tmp/status/load_status"
FREE_SPACE_THRESHOLD_PCT = 10  # If space is below this, exit upgrade
PRE_HOOK = "pre_hook"
POST_HOOK = "post_hook"

CONTAINER_STOP_TIMEOUT_SECS = 30
COMPONENT_PORTS = {
    "db": 3306,
    "consul": 8500,
    "clustermgr": 8080,
    "rsyslog": 2514,
    "kafka1": 9092,
    "zoo1": 2181
}

# Create logger. Add console and syslog handlers
LOGGER = logging.getLogger('upgrade')
LOGGER.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
LOGGER.addHandler(ch)
sh = handlers.SysLogHandler("/dev/log")
sh.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
LOGGER.addHandler(sh)


def mkdirs(path):
    try:
        os.makedirs(path)
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            LOGGER.error(
                "Failed to create directory: %s Error:%s" %
                (path, ex))
            sys.exit(1)


def exec_cmd(exit_on_error=True, *args):
    try:
        return (0, subprocess.check_output(args, stderr=subprocess.STDOUT))
    except subprocess.CalledProcessError as ex:
        if not exit_on_error:
            return (ex.returncode, ex.output)
        LOGGER.error("Failed to execute command %s. Return code:%s. Error:%s"
                     % (" ".join(args), ex.returncode, ex.output))
        sys.exit(1)


def mount_cdrom(mount_point):
    try:
        if os.path.ismount(mount_point):
            LOGGER.info("CDROM already mounted")
            return
        out = subprocess.check_output(["mount", "/dev/cdrom", mount_point])
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to mount cdrom. Return code:%s Error:%s"
                     % (ex.returncode, ex.output))
        sys.exit(1)


def mount_iso(iso_location, mount_point):
    try:
        if os.path.ismount(mount_point):
            LOGGER.info("Iso already mounted")
            return
        out = subprocess.check_output(
            ["mount", "-o", "rw,loop", iso_location, mount_point])
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to mount iso. Return code:%s Error:%s"
                     % (ex.returncode, ex.output))
        sys.exit(1)


def topological_sort(dag):
    """
    Returns the topological ordering of the DAG(Directed Acyclic Graph).
    Implements Kahn's algorithm:
    https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
    """

    incoming_edge = {}
    output = []
    for vertex in dag:
        for edge in dag[vertex]:
            incoming_edge[edge] = incoming_edge.get(edge, 0) + 1

    queue = []
    for vertex in dag:
        if vertex not in incoming_edge:
            queue.append(vertex)

    while queue:
        edge = queue.pop(0)
        output.insert(0, edge)
        for v in dag[edge]:
            incoming_edge[v] = incoming_edge[v] - 1
            if incoming_edge[v] == 0:
                queue.append(v)

    if len(output) != len(dag):
        LOGGER.error("Graph is not a DAG")
        sys.exit()
    else:
        return output


def get_dependencies(dag, src):
    """
    Return all vertices which are reachable from vertex 'src'
    in the DAG.
    """
    queue = []
    for vertex in dag[src]:
        queue.append(vertex)

    deps = set()
    while queue:
        vertex = queue.pop(0)
        deps.add(vertex)
        for v in dag[vertex]:
            if v not in deps:
                queue.append(v)

    return deps


def load_yaml(fname):
    with open(fname, 'r') as f:
        return yaml.load(f)


def validate_checksum_record(rec, file_checksums):
    if len(rec) != 2:
        LOGGER.error("Wrong format for checksum file %s" % fname)
        sys.exit(1)
    if rec[1] in file_checksums:
        LOGGER.error("Duplicate entries in checksum file %s" % fname)
        sys.exit(1)


def parse_checksum_file(fname):
    """
    Expected format of each line is:
    checksum fileName

    Blank lines or lines starting with # are ignored
    """

    file_checksums = {}
    with open(fname, 'r') as f:
        for line in f:
            line = line.strip()
            if (line == "") or line.startswith("#"):
                continue
            rec = line.split()
            validate_checksum_record(rec, file_checksums)
            file_checksums[rec[1]] = rec[0]

    return file_checksums


def verify_sha256sum(file_path, expected_sum):
    sha256 = hashlib.sha256()
    buf_size = 64 * pow(2, 10)  # 64KB
    with open(file_path) as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest() == expected_sum


def umount(mount_point):
    try:
        out = subprocess.check_output(["umount", mount_point])
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to unmount cdrom. Return code:%s Error:%s"
                     % (ex.returncode, ex.output))
        sys.exit(1)


def exec_docker_compose_cmd(exit_on_error, cmd, *args):
    try:
        cmd = ["docker-compose", "-f", DOCKER_COMPOSE_FILE, cmd]
        cmd.extend(args)
        # redirect stderr to stdout so that all cmd output is captured.
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT,
                                      cwd=TURBO_DOCKER_CONF_ROOT)
        return (0, out)
    except subprocess.CalledProcessError as ex:
        if not exit_on_error:
            return (ex.returncode, ex.output)
        LOGGER.error("Failed to execute docker compose command:%s"
                     "Return code:%s Error:%s" % (cmd, ex.returncode, ex.output))
        sys.exit(1)


def get_container_ip(component):
    ret, container_id = exec_docker_compose_cmd(True, "ps", "-q", component)
    ret, container_json = exec_cmd(
        True, "docker", "inspect", container_id.strip())
    container_info = json.loads(container_json)
    # TODO:(karthikt) - This relies on docker networking details. This
    # has to be changed when we move to kubernetes. Or implement an approach
    # which is not dependent on the container networking details.
    ip = (container_info[0].get("NetworkSettings")
          .get("Networks")
          .get("docker_default")
          .get("IPAddress"))
    return ip


def check_connectivity(ip, port):
    """
    Return true if we can telnet to the ip:port else return false.
    """
    try:
        conn = telnetlib.Telnet(ip, port)
    except Exception as e:
        LOGGER.debug("Trouble connecting to ip:port %s:%s" % (ip, port))
        return False
    finally:
        conn.close()
        return True


def wait_until_connection_ready(component):
    ip = get_container_ip(component)
    while not check_connectivity(ip, COMPONENT_PORTS.get(component)):
        # wait indefinitely
        continue


def wait_until_log_msg(component, service_pttrn):
    wait_secs = 5
    while True:
        ret, out = exec_docker_compose_cmd(False, "logs", "--tail=1000",
                                           "rsyslog")

        if ret == 0 and re.findall(service_pttrn.get(component), out.strip()):
            break

        time.sleep(wait_secs)


def wait_until_component_ready(component):

    LOGGER.info("Waiting for component : '%s' to be READY" % component)

    wait_secs = 30
    if component in ["nginx", "zoo1", "kafka1", "influxdb", "ml-datastore"]:
        time.sleep(wait_secs)
        return
    elif component in ["db", "consul", "clustermgr", "rsyslog"]:
        wait_until_connection_ready(component)
        return

    ip = get_container_ip(component)
    port = 8080
    wait_secs = 5
    while True:
        ret, state = exec_cmd(False, "curl", "-s", "-X", "GET",
                              "http://%s:%s/state" % (ip, port),
                              "-H", '"accept: application/json;charset=UTF-8"')
        state = state.strip()

        if state.find("RUNNING") >= 0:
            break
        elif state.find("MIGRATING") >= 0:
            # print migration progress
            ret, migration_info = exec_cmd(False, "curl", "-s", "-X", "GET",
                                           "http://%s:%s/migration" % (
                                               ip, port),
                                           "-H", '"accept: application/json;charset=UTF-8"')
            #print migration_info

        time.sleep(wait_secs)


def check_free_space():
    stat = os.statvfs(DOCKER_DIR)
    total_space = stat.f_frsize * stat.f_blocks
    free_space = stat.f_frsize * stat.f_bavail
    if ((free_space * 1.0 / total_space) * 100) <= FREE_SPACE_THRESHOLD_PCT:
        LOGGER.error("Disk space below %s. Exiting" % FREE_SPACE_THRESHOLD_PCT)
        sys.exit(1)


def get_version_number(info_file):
    with open(info_file, 'r') as f:
        for line in f:
            line = line.strip()
            if (line.find("Version:") >= 0):
                return (line.strip().split()[2]).strip()

    return ""


def compare_versions(version1, version2):
    """
    Given two version strings of the form x.y.z.., return:
    1  : if version1 > version2
    0  : if version1 == version2
    -1 : if version1 < version2

    E.g.:
        compare_versions("7.9.0", "7.9.0")==0
        compare_versions("7.8.0", "7.9.0")==-1
        compare_versions("7.9.0", "7.8.0")==1
        compare_versions("7.9.0", "7.10.0")==-1
        compare_versions("7.09.0", "7.10.0")==-1
        compare_versions("7.12.12", "7.10.0")==1
        compare_versions("7.12.12", "7.12.13")==-1
        compare_versions("7.12", "7.12.13")==-1
        compare_versions("7.10", "7.12.13")==-1
        compare_versions("7.10", "7.10.0")==0
    """
    ver1_tokens = version1.split('.')
    ver2_tokens = version2.split('.')
    for v1, v2 in izip_longest(ver1_tokens, ver2_tokens, fillvalue=0):
        diff = int(v1) - int(v2)
        if diff != 0:
            return 1 if (diff > 0) else -1

    return 0


def get_component_name_from_image_file_name(fname):
    comp_name, ext = os.path.splitext(fname)
    """
    Return the name of the component given the image tarball name
    """
    d = {
        "kafka": "kafka1",
        "zookeeper": "zoo1",
        "syslog": "rsyslog",
        "reports": "reporting"
    }

    return d.get(comp_name, comp_name.replace('_', '-'))


def get_file_names(dir_path, patterns):
    return (itertools.chain.from_iterable(
            glob.iglob(join(dir_path, pattern))
            for pattern in patterns))


def convert_to_version_string(name):
    """
    Converts a string of type XXX_XXX_XXX to XXX.XXX.XXX
    e.g. 007_012_000 => 7.12.0
    """
    return ".".join(str(int(s)) for s in name.split('_'))


def get_upgrade_spec_files(upgrade_spec_loc, current_version):
    """
    Return the list of upgrade spec file paths which are for version > current_version.
    The output is sorted in increasing order of version numbers.
    """

    upgrade_spec_files = []
    for root, dirs, files in os.walk(upgrade_spec_loc):
        dirs[:] = sorted([dir for dir in dirs if
                          compare_versions(convert_to_version_string(dir),
                                           current_version) >= 0])
        if files:
            upgrade_spec_files.extend([join(root, f)
                                       for f in files if f.endswith(".yml")])

    return upgrade_spec_files


def create_abs_path(base_dir, rel_path):
    if (os.path.isabs(rel_path)):
        return rel_path
    else:
        return os.path.join(base_dir, rel_path)


def merge_dicts(dict1, dict2, merge_fn=lambda x, y: y):
    """
    Merge dictionary dict2 into dict1 and return dict1. On collision, values of
    types:
        a) dictionaries: are merged.
        b) list: are extened.
        c) others: default is to pick the value from dict2. But the
        behaviour can be overriden by passing a custom merge function.
    """
    for k, v in dict2.iteritems():
        if k in dict1:
            existing_value = dict1.get(k)
            if isinstance(v, dict) and isinstance(existing_value, dict):
                dict1[k] = merge_dicts(existing_value, v)
            elif isinstance(v, list) and isinstance(existing_value, list):
                existing_value.extend(v)
            else:
                dict1[k] = merge_fn(existing_value, v)
        else:
            dict1[k] = v

    return dict1


def set_component_data_versions(components):
    """
    Set the component data versions to "00_00_00" if it is not set.
    This will be used during data migration by the components.
    """
    for component in components:
        # check if data version is already set for the component
        ret, component_version = exec_cmd(False, "docker", "exec", "docker_consul_1",
                                          "consul", "kv", "get", "%s-1/dataVersion" % component)
        # if component_version doesn't exist, set it.
        if (ret != 0):
            LOGGER.info("Setting data version for component: %s" % component)
            ret, component_version = exec_cmd(True, "docker", "exec", "docker_consul_1",
                                              "consul", "kv", "put", "%s-1/dataVersion" % component, "00_00_00")
        if (ret != 0):
            LOGGER.error(
                "Failed to set data version for component %s" %
                component)
            sys.exit(1)


if __name__ == '__main__':
    """
    STEPS:
    1. Mount the iso
    2. Find all the images which needs to be upgraded by checking the turbonomic_sums file
    3. Replace the docker-compose.yml.* files
    4. Create the set of components_to_upgrade based on components which have new checksums.
    5. Validate the checksums
    6. Parse docker-compose.yml and turbo_upgrade_spec.yml file(if it exists)
    7. Create unified dependency graphs (G and Transpose(G)) from the yaml files. If there
       are cycles, throw error and exit.
    8. Create a topological ordering T of the graph G
    9. For each component C in the list T:
        If there is a new image for C:
            a) Untar and add image to local registry
            b) Shutdown C and all the components that depend on it (BFS on Transpose(G))
            c) Run pre-commit hooks
            d) Load the new image
            e) Start C
            f) Wait for C to become "READY". For Turbo components, we query '/state'
            endpoint. For 3rd party components, sleep for few secs or add component
            specific "READINESS" check.
            g) Run post-commit hooks
    10. Copy the turbonomic_sums.txt file
    11. Unmount the iso
    """

    parser = argparse.ArgumentParser(
        description='Upgrade XL components to a newer version.')
    parser.add_argument('--loc',
                        help="Location where the upgrade artifacts are located.\
              Default location is CD-ROM.",
                        action='store', dest="loc")

    parser.add_argument('--iso',
                        help="Path of the new iso file to upgrade from. This \
                                is an alternative to CD-ROM upgrade",
                        action='store', dest="iso_loc")
    parser.add_argument("--skip-verify", help="Skip checksum verification. \
            You can use this option when you are restarting a failed upgrade \
            so that the checksum verification of the images is not repeated \
            again.", action="store_true")
    parser.add_argument("cmd", help="Start the upgrade", choices=['start'])
    args = parser.parse_args()

    # Write to load_status file so that the upgrade progess is displayed in the UI.
    # We want to write only one line in the file. Control the number of lines
    # approximately by specifying the number of bytes before file rotation
    # kicks in.
    mkdirs(os.path.dirname(UPGRADE_STATUS_FILE))
    maxBytes = 50
    backupCount = 1
    fh = handlers.RotatingFileHandler(UPGRADE_STATUS_FILE, 'w',
                                      maxBytes, backupCount)
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    fh.setFormatter(formatter)
    LOGGER.addHandler(fh)

    LOGGER.info("Starting XL upgrade")

    check_free_space()

    if not args.loc:
        LOGGER.info("Mounting iso from CDROM")
        mkdirs(ISO_MOUNTPOINT)
        if args.iso_loc:
            mount_iso(args.iso_loc, ISO_MOUNTPOINT)
        else:
            mount_cdrom(ISO_MOUNTPOINT)
    else:
        ISO_MOUNTPOINT = args.loc

    old_version = get_version_number(TURBO_INFO_FILE)
    new_version = get_version_number(join(ISO_MOUNTPOINT,
                                          os.path.basename(TURBO_INFO_FILE)))
    if (compare_versions(old_version, new_version) > 0):
        LOGGER.error("The newer version: %s should be higher than the current"
                     + " version: %s", new_version, old_version)
        sys.exit(1)

    curr_checksums = parse_checksum_file(TURBO_CHECKSUM_FILE)
    new_checksums = parse_checksum_file(join(ISO_MOUNTPOINT,
                                             os.path.basename(TURBO_CHECKSUM_FILE)))

    my_name = os.path.basename(__file__)
    # Replace myself if there is a newer version
    if (my_name in new_checksums and
            not verify_sha256sum(__file__, new_checksums.get(my_name))):
        LOGGER.info("Newer version of the upgrade tool is available. "
                    "Upgrading to latest version")
        shutil.copy(join(ISO_MOUNTPOINT, my_name), __file__)
        LOGGER.info("Upgraded the upgrade tool. Please restart the upgrade.")
        sys.exit(1)

    try:
        import yaml
    except ImportError:
        LOGGER.info("Installing PyYAML package")
        already_installed_msg = "is already installed"
        for pkg in ["libyaml-0.1.4-11.el7_0.x86_64.rpm",
                    "pyyaml-3.10-11.el7.x86_64.rpm"]:
            retcode, output = exec_cmd(
                False, "rpm", "-i", join(ISO_MOUNTPOINT, pkg))
            if (retcode != 0 and (already_installed_msg not in output)):
                LOGGER.error("Failed to install package: %s. Aborting upgrade.\n"
                             "Return code: %s. Error: %s" % (pkg, retcode, output))
                sys.exit(1)

        import yaml

    # Backup old files
    # FIXME:(karthikt) Handle case where the script crashes or is interrupted
    # in the middle of execution. Then we should not again create a backup
    # and not reload the already existing images.
    tstamp = datetime.datetime.today().strftime('%Y-%m-%d-%H%M%S')
    shutil.copytree(TURBO_DOCKER_CONF_ROOT,
                    "%s.%s" % (TURBO_DOCKER_CONF_ROOT, tstamp))
    # update the docker compose and other files with the latest version
    files_to_update = ["*.yml*", ".env", "metron.py", "turboctl.py"]
    for fname in get_file_names(ISO_MOUNTPOINT, files_to_update):
        if not (verify_sha256sum(fname,
                                 new_checksums.get(os.path.basename(fname)))):
            LOGGER.error("Checksum mismatch for %s" % fname)
            sys.exit(1)
        shutil.copy(fname, TURBO_DOCKER_CONF_ROOT)
    vmtctl_loc = join(ISO_MOUNTPOINT,
                      os.path.basename(TURBO_VMTCTL_LOC))
    if os.path.isfile(vmtctl_loc):
        if not verify_sha256sum(vmtctl_loc,
                                new_checksums.get(os.path.basename(vmtctl_loc), "")):
            LOGGER.error("Checksum doesn't match for %s" % vmtctl_loc)
            sys.exit(1)
        shutil.copy2(vmtctl_loc, TURBO_VMTCTL_LOC)

    yaml_files_to_parse = [DOCKER_COMPOSE_FILE]
    upgrade_spec_files = get_upgrade_spec_files(join(ISO_MOUNTPOINT,
                                                     TURBO_UPGRADE_SPECS_DIR), old_version)
    yaml_files_to_parse.extend(upgrade_spec_files)

    # Mapping from vertex -> list_of_vertices
    dep_graph = {}
    dep_graph_transpose = {}
    # Map of component name to its description map(from the yaml files)
    service_desc = {}
    # We merge all the upgrade specs. If there is a cyclic dependency,
    # it will be detected during the topological sort. Also we process
    # pre_hook and post_hook scripts in the increasing order of version
    # numbers. If there are cyclic dependencies or the pre_hooks and
    # post_hook scripts from a lower version needs to be executed fully
    # before a higher version can be executed, then the upgrade should
    # be executed in steps: i.e. upgrade 1st from version x -> y and
    # then trigger upgrade from y -> z.
    for yaml_file in yaml_files_to_parse:
        LOGGER.info("Using upgrade spec file: %s" % yaml_file)
        yaml_doc = load_yaml(yaml_file)
        for k, v in yaml_doc['services'].iteritems():
            if PRE_HOOK in v:
                v[PRE_HOOK] = [create_abs_path(os.path.dirname(yaml_file), hook)
                               for hook in v.get(PRE_HOOK)]
            if POST_HOOK in v:
                v[POST_HOOK] = [create_abs_path(os.path.dirname(yaml_file), hook)
                                for hook in v.get(POST_HOOK)]
            service_desc[k] = merge_dicts(service_desc.get(k, {}), v)
            if v and ('depends_on' in v):
                for dep in v["depends_on"]:
                    dep_graph.setdefault(k, set()).add(dep)
                    dep_graph_transpose.setdefault(dep, set()).add(k)
                dep_graph_transpose.setdefault(k, set())
            else:
                dep_graph.setdefault(k, set())
                dep_graph_transpose.setdefault(k, set())

    topological_order = topological_sort(dep_graph)
    set_component_data_versions(topological_order)
    components_to_upgrade = set()
    component_to_image_loc = {}
    # We start with new_checksums as we may add new components.
    # As for the case for removing old components, it is not handled.
    # Also verify all checksum first so that we are not left with partial
    # upgrade
    LOGGER.info("Verifying checksums")
    for name, cksum in new_checksums.iteritems():
        comp_name, ext = os.path.splitext(name)
        # Only upgrade docker components which are new.
        if ext == '.tgz' and ((name not in curr_checksums) or
                              (curr_checksums[name] != cksum)):

            new_version_loc = join(ISO_MOUNTPOINT, name)
            if (not args.skip_verify) and (not verify_sha256sum(
                    new_version_loc, new_checksums.get(name))):
                LOGGER.error(
                    "Checksum verification failed for %s" %
                    new_version_loc)
                sys.exit(1)
            comp_name = get_component_name_from_image_file_name(name)
            component_to_image_loc[comp_name] = new_version_loc
            components_to_upgrade.add(comp_name)

    if not components_to_upgrade:
        LOGGER.info("No new images found")

    stopped_components = set()
    total_components_to_upgrade = min(
        len(topological_order),
        len(components_to_upgrade))
    LOGGER.info("%s components to upgrade: %s"
                % (total_components_to_upgrade,
                   " ".join([component for component in topological_order
                             if component in components_to_upgrade])))
    count = 0

    for component in topological_order:
        if component in components_to_upgrade:
            count += 1
            # We take the min here, because we may have commented out the
            # components in the docker-compose file and these may still
            # be in the checksum file.
            LOGGER.info("(%s/%s) Upgrading component : %s" %
                        (count, total_components_to_upgrade, component))
            # Get the components which depend on this component and stop them.
            deps = get_dependencies(dep_graph_transpose, component)
            if deps:
                ret, out = exec_docker_compose_cmd(True, "stop", "-t",
                                                   str(CONTAINER_STOP_TIMEOUT_SECS), *deps)
                stopped_components |= set(deps)

            # Run pre upgrade steps.
            if (service_desc.get(component) and
                    PRE_HOOK in service_desc.get(component)):
                LOGGER.info("Running pre upgrade hooks for component: %s"
                            % component)
                for pre_hook in service_desc.get(component).get(PRE_HOOK):
                    LOGGER.info("Executing pre hook %s" % pre_hook)
                    exec_cmd(True, pre_hook)

            # Stop the container.
            ret, out = exec_docker_compose_cmd(True, "stop", "-t",
                                               str(CONTAINER_STOP_TIMEOUT_SECS), component)
            LOGGER.info("Loading new image for component: %s" % component)
            exec_cmd(True, "docker", "load", "-i",
                     component_to_image_loc.get(component))

            # Start the component
            exec_docker_compose_cmd(True, "up", "-d", component)
            wait_until_component_ready(component)
            if component in stopped_components:
                stopped_components.remove(component)

            # Run post upgrade steps.
            if (service_desc.get(component) and
                    POST_HOOK in service_desc.get(component)):
                LOGGER.info("Running post upgrade hooks for component: %s"
                            % component)
                for post_hook in service_desc.get(component).get(POST_HOOK):
                    LOGGER.info("Executing post hook %s" % post_hook)
                    exec_cmd(True, post_hook)

    for component in topological_order:
        if (component in stopped_components):
            exec_docker_compose_cmd(True, "up", "-d", component)

    # Update components which have a new image but are not enabled.
    remaining_components_to_upgrade = \
        components_to_upgrade.difference(topological_order)
    LOGGER.info("Upgrading remaining %s new containers"
                % len(remaining_components_to_upgrade))
    for component in remaining_components_to_upgrade:
        LOGGER.info("Stopping component %s" % component)
        ret, out = exec_docker_compose_cmd(False, "stop", "-t",
                                           str(CONTAINER_STOP_TIMEOUT_SECS), component)
        LOGGER.info("Loading new image for component: %s" % component)
        exec_cmd(True, "docker", "load", "-i",
                 component_to_image_loc.get(component))

    # Remove dangling images
    LOGGER.info("Removing dangling images")
    ret, out = exec_cmd(True, "docker", "image", "prune", "-f")
    LOGGER.info(out)

    # copy the turbo checksum and other files
    dir_util.copy_tree(join(ISO_MOUNTPOINT, TURBO_UPGRADE_SPECS_DIR),
                       join(TURBO_DOCKER_CONF_ROOT, TURBO_UPGRADE_SPECS_DIR))
    for fname in get_file_names(ISO_MOUNTPOINT, ["*.txt"]):
        shutil.copy(fname, TURBO_DOCKER_CONF_ROOT)

    if not args.loc:
        LOGGER.info("Unmounting CDROM")
        umount(ISO_MOUNTPOINT)
    LOGGER.info("XL upgrade finished successfully")
