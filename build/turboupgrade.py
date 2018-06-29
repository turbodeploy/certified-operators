#!/bin/python

"""
Script to upgrade XL components.
"""

import datetime
import errno
import glob
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import yaml
from logging import handlers


TURBO_DOCKER_CONF_ROOT = "/etc/docker"
DOCKER_COMPOSE_FILE = os.path.join(TURBO_DOCKER_CONF_ROOT, "docker-compose.yml")
TURBO_CHECKSUM_FILE = os.path.join(TURBO_DOCKER_CONF_ROOT, "turbonomic_sums.txt")
TURBO_UPGRADE_SPEC_FILE =\
    os.path.join(TURBO_DOCKER_CONF_ROOT, "turbo_upgrade_spec.yaml")
ISO_MOUNTPOINT = "/media/cdrom"
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
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
LOGGER.addHandler(sh)

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
        LOGGER.error("Wrong format for checksum file %s"%fname)
        sys.exit(1)
    # overwrite duplicates
    if rec[1] in file_checksums:
        LOGGER.error("Duplicate entries in checksum file %s"%fname)
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
    buf_size = 64*pow(2, 10) # 64KB
    with open(file_path) as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest() == expected_sum

def mount_iso(mount_point):
    try:
        if os.path.ismount(mount_point):
            LOGGER.info("CDROM already mounted")
            return
        out = subprocess.check_output(["mount", "/dev/cdrom", mount_point])
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to mount cdrom. Return code:%s Error:%s"
            %(ex.returncode, ex.output))
        sys.exit(1)

def umount_iso(mount_point):
    try:
        out = subprocess.check_output(["umount", mount_point])
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to unmount cdrom. Return code:%s Error:%s"
            %(ex.returncode, ex.output))
        sys.exit(1)

def exec_docker_compose_cmd(exit_on_error, cmd, *args):
    try:
        cmd = ["docker-compose", "-f", DOCKER_COMPOSE_FILE, cmd]
        cmd.extend(args)
        out = subprocess.check_output(cmd)
        return (0, out)
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to execute docker compose command:%s"\
            "Return code:%s Error:%s"%(cmd, ex.returncode, ex.output))
        if not exit_on_error:
            return (ex.returncode, ex.output)
        sys.exit(1)

def exec_cmd(exit_on_error=True, *args):
    try:
        return (0, subprocess.check_output(args))
    except subprocess.CalledProcessError as ex:
        LOGGER.error("Failed to execute command %s. Return code:%s. Error:%s"
            %(" ".join(args), ex.returncode, ex.output))
        if not exit_on_error:
            return (ex.returncode, ex.output)
        sys.exit(1)

def wait_until_log_msg(component):
    # TODO:(karthikt) - For now, just grep the log messages.
    #  Should use a better mechanism than grepping.
    service_pttrn = {
        "db" : "port: 3306",
        "arangodb" : "ready for business",
        "consul" : "New leader elected",
        "clustermgr" : r"Server.*: Started",
        "rsyslog" : "rsyslogd",
        #"kafka1" : "Startup complete"
        #"zoo1" : "binding to port"
    }

    wait_secs = 5
    while True:
        ret, out = exec_docker_compose_cmd(False, "logs", "--tail=1000",
                        "rsyslog")

        if ret==0 and re.findall(service_pttrn.get(component), out.strip()):
            break

        time.sleep(wait_secs)


def wait_until_component_ready(component):

    LOGGER.info("Waiting for component : '%s' to be READY"%component)

    wait_secs = 30
    if component == "nginx" or component.startswith("mediation"):
        time.sleep(wait_secs)
        return
    elif component in ["arangodb", "db", "consul", "clustermgr", "rsyslog",
                        "kafka1", "zoo1"]:
        wait_until_log_msg(component)
        return

    ret, container_id = exec_docker_compose_cmd(True, "ps", "-q", component)
    ret, container_json = exec_cmd(True, "docker", "inspect", container_id.strip())
    container_info = json.loads(container_json)
    # TODO:(karthikt) - This relies on docker networking details. This
    # has to be changed when we move to kubernetes. Or implement an approach
    # which is not dependent on the container networking details.
    ip = (container_info[0].get("NetworkSettings")
          .get("Networks")
          .get("docker_default")
          .get("IPAddress"))
    port = 8080
    wait_secs = 5
    while True:
        ret, state = exec_cmd(False, "curl", "-X", "GET",
                         "http://%s:%s/state"%(ip, port),
                         "-H", '"accept: application/json;charset=UTF-8"')
        state = state.strip()

        if state == "RUNNING":
            break
        elif state == "MIGRATING":
            # print migration progress
            ret, migration_info = exec_cmd(False, "curl", "-X", "GET",
                             "http://%s:%s/migration/"%(ip, port),
                             "-H", '"accept: application/json;charset=UTF-8"')
            print migration_info

        time.sleep(wait_secs)

if __name__ == '__main__':
    """
    Steps:
    1. Mount the iso
    2. Find all the images which needs to be upgraded by checking the turbonomic_sums file
       Replace the docker-compose.yml.* files
       Create the set of components_to_upgrade based on components which have new checksums.
       Validate the checksums:
       sha256sum -c turbonomic_sums.txt
    3. Parse the docker compose yaml and upgrade spec yaml file.
    4. Create unified dependency graph (G and Transpose(G)) from the yaml files. If there
       are cycles, throw error and exit.
    5. Create a topological ordering T of the graph G
    6. For each component C in the list T:
        If there is a new image for C:
            a) Untar and add image to local registry
            b) Shutdown C and all the components that depend on it (BFS on Transpose(G))
            c) Run pre-commit hooks
            d) Load the new image
            e) Start C
            f) Wait for C to become "READY". For Turbo components, we query '/state'
            endpoint. For 3rd party components, sleep for 1 minute (or component
            specific logic for checking "READINESS")
            g) Run post-commit hooks
    7. Unmount the iso
    """

    my_name = os.path.basename(__file__)
    if (len(sys.argv) != 2) or (sys.argv[1] != "start"):
        print "Usage: %s start"%my_name
        sys.exit(1)

    try:
        os.makedirs(ISO_MOUNTPOINT)
    except OSError as ex:
        if ex.errno != errno.EEXIST:
            LOGGER.error("Failed to create CD mount dir: %s Error:%s"%(
                ISO_MOUNTPOINT, ex))
            sys.exit(1)

    LOGGER.info("Starting XL upgrade")

    LOGGER.info("Mounting iso from CDROM")
    mount_iso(ISO_MOUNTPOINT)

    curr_checksums = parse_checksum_file(TURBO_CHECKSUM_FILE)
    new_checksums = parse_checksum_file(os.path.join(ISO_MOUNTPOINT,
        os.path.basename(TURBO_CHECKSUM_FILE)))

    # Replace myself if there is a newer version
    if (curr_checksums.has_key(my_name) and
            new_checksums.has_key(my_name) and
            new_checksums.get(my_name) != curr_checksums.get(my_name)):
        LOGGER.info("Replacing with the new version of upgrade script.")
        shutil.copy(os.path.join(ISO_MOUNTPOINT, my_name), __file__)
        LOGGER.info("Upgraded the upgrade script. Please restart the upgrade.")

    # Backup old files
    # FIXME:(karthikt) Handle case where the script crashes or is interrupted
    # in the middle of execution. Then we should not again create a backup
    # and not reload the already existing images.
    tstamp = datetime.datetime.today().strftime('%Y-%m-%d-%H%M%S')
    shutil.copytree(TURBO_DOCKER_CONF_ROOT,
        "%s.%s"%(TURBO_DOCKER_CONF_ROOT,tstamp))
    # copy all the turbo docker compose files
    for fname in glob.glob("%s/*.yml*"%ISO_MOUNTPOINT):
        shutil.copy(fname, TURBO_DOCKER_CONF_ROOT)

    yaml_files_to_parse = [DOCKER_COMPOSE_FILE]
    if os.path.isfile(TURBO_UPGRADE_SPEC_FILE):
        LOGGER.info("Using upgrade spec file: %s"%(
            TURBO_UPGRADE_SPEC_FILE))
        yaml_files_to_parse.append(sys.argv[1])

    # Mapping from vertex -> list_of_vertices
    dep_graph = {}
    dep_graph_transpose = {}
    service_desc = {}
    for yml in yaml_files_to_parse:
        yaml_doc = load_yaml(yml)
        for k, v in yaml_doc['services'].iteritems():
            service_desc[k] = v
            if v.has_key('depends_on'):
                for dep in v["depends_on"]:
                    dep_graph.setdefault(k, set()).add(dep)
                    dep_graph_transpose.setdefault(dep, set()).add(k)
                dep_graph_transpose.setdefault(k, set())
            else:
                dep_graph.setdefault(k, set())
                dep_graph_transpose.setdefault(k, set())

    topological_order = topological_sort(dep_graph)
    components_to_upgrade = set()
    component_to_image_loc = {}
    # We start with new_checksums as we may add new components.
    # As for the case for removing old components, it is not handled.
    # Also verify all checksum first so that we are not left with partial upgrade
    LOGGER.info("Verifying checksums")
    for name, cksum in new_checksums.iteritems():
        comp_name, ext = os.path.splitext(name)
        if ((name not in curr_checksums) or (curr_checksums[name] != cksum
                and ext == '.tgz')):
            new_version_loc = os.path.join(ISO_MOUNTPOINT, name)
            verify_sha256sum(new_version_loc, new_checksums.get(name))
            comp_name = comp_name.replace('_', '-')
            component_to_image_loc[comp_name] = new_version_loc
            components_to_upgrade.add(comp_name)

    stopped_components = set()
    LOGGER.info("%s components to upgrade: %s"
        %(len(components_to_upgrade), " ".join(components_to_upgrade)))
    count = 0
    for component in topological_order:
        if component in components_to_upgrade:
            count += 1
            LOGGER.info("(%s/%s) Upgrading component : %s"%
                (count, len(components_to_upgrade), component))
            # Get the components which depend on this component and stop them.
            deps = get_dependencies(dep_graph_transpose, component)
            if deps:
                ret, out = exec_docker_compose_cmd(True, "stop", *deps)
                stopped_components |= set(deps)
            ret, out = exec_docker_compose_cmd(True, "stop", component)
            exec_cmd(True, "docker", "load", "-i",
                component_to_image_loc.get(component))
            exec_docker_compose_cmd(True, "stop", dep)
            if service_desc.get(component).has_key("pre_hook"):
                exec_cmd(True, service_desc.get(component).get("pre_hook"))
            exec_docker_compose_cmd(True, "up", "-d", component)
            wait_until_component_ready(component)
            if component in stopped_components:
                stopped_components.remove(component)

    for component in stopped_components:
        exec_docker_compose_cmd(True, "up", "-d", component)

    # Remove dangling images
    #exec_cmd(True, "docker", "images", "-qa", "-f", "dangling=true", "|",
    #                "xargs", "docker", "rmi")

    # copy the turbo checksum and info files
    for fname in glob.glob("%s/*.txt"%ISO_MOUNTPOINT):
        shutil.copy(fname, TURBO_DOCKER_CONF_ROOT)

    LOGGER.info("Unmounting CDROM")
    umount_iso(ISO_MOUNTPOINT)
    LOGGER.info("XL upgrade finished successfully")
