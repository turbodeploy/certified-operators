#!/usr/bin/env python3

# methods that provide a flattened view of properties present in the CR (custom resources)
# definitions available in t8c-operator

import os
import sys
from logger import logger
from ruamel.yaml import YAML
from pathlib import Path
import db

def flatten_props(yaml, components=None):
    """
    Read YAML-structured properties constructed by operator, and translate them into a simple
    properties dictionaries, with keys formed from paths leading to the all the property values.

    Sections of the config structure are processed in priority order from lowest to highest:

    * "global" sections are lower priority than component-specific sections
    * sections under "defaultProperties" are lower priority than "customProperties"

    Paths are flattened into keys by prefixing each property name (except an initial one) with a
    dot, and by placing array indexes in square brackets.

    :param yaml: the original tree-structured collection of properties
    :param components: names of component whose sections should be used, in order of increasing
        priority, or None to use only global sections
    :return: dictionary containing highest-priority flattened key/value pairs
    """
    result = {}
    flatten_yaml(yaml.get('defaultProperties', {}).get('global', {}), result)
    for component in (components or []):
        flatten_yaml(yaml.get('defaultProperties', {}).get(component, {}), result)
    flatten_yaml(yaml.get('customProperties', {}).get('global', {}), result)
    for component in (components or []):
        flatten_yaml(yaml.get('customProperties', {}).get(component, {}), result)
    return result


def flatten_yaml(yaml, into, prefix=""):
    """
    Flatten the given yaml structure, populating the given dictionary with flattened key/value
    pairs.

    This method is used recursively in a depth-first traversal of the YAML tree.

    :param yaml: YAML tree to be flattened
    :param into: dictionary to receive flattened pairs
    :param prefix: string representing path-so-far in YAML traversal
    :return: flattened properties dictionary
    """
    for key, value in yaml.items():
        if isinstance(value, dict):
            flatten_yaml(value, into, prefix=prefix + key + '.')
        elif isinstance(value, list):
            for i, val in enumerate(value):
                flatten_yaml(val, into, prefix=prefix + f'[{i}]')
        else:
            into[prefix + key] = str(value)


def get_config_properties(components=None):
    """
    Flatten the YAML file constructed by operator and mounted in the image.

    TURBO_CONFIG_PROPERTIES_FILE environment variable should contain the mount path for the
    YAML file.

    :param components: component whose props can override global, or None to only use global props
    :return: flattened properties dictionary
    """
    return flatten_props(YAML().load(Path(os.environ['TURBO_CONFIG_PROPERTIES_FILE'])),
                         components=components)


def main(args):
    """
    When this script is invoked from the command line, it takes a single required argument, the
    flattened name of a property, and prints that property's value on a single line. The property
    name can optionally be preceded by any or all of:

    * `-c <components>`, where <components> is a comma-separated list of component names, in
      increasing priority order, whose properties should override global properties.
    * `-db <type>:<endpoint>` where <type> is either 'postgres' or 'mysql' and <endpoint> is the
      name of an endpoint whose DbEndpoint properties should be examined
    * `-d <default>`, where <default> is a value to return if none is available from configs. This
      is especially helpful for endpoint properties that are specified in the endpoint constructor
      in the Java code.

    :param args: command line arguments, with initial arg (script path) stripped
    """
    components = None
    db_type = None
    endpoint_name = None
    default = None
    while True:
        if args[0] == '-c':
            if len(args) < 2:
                sys.exit("no component name list after -c")
            components = args[1].split(',')
            args = args[2:]
        elif args[0] == '-db':
            if len(args) < 2:
                sys.exit("no db endpoint spec after '-db'")
            (db_type, endpoint_name) = args[1].split(':', 1)
            args = args[2:]
        elif args[0] == '-d':
            if len(args) < 2:
                sys.exit("no default value after '-d'")
            default = args[1]
            args = args[2:]
        else:
            break
    if len(args) < 1:
        sys.exit("no property name specified")
    value = None
    if endpoint_name:
        value = db.get_db_config(args[0], db_type, endpoint_name, components=components)
    else:
        value = get_config_properties(components).get(args[0])
    value = value or default
    if value:
        print(value)


if __name__ == '__main__':
    main(sys.argv[1:])
