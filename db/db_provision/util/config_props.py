#!/usr/bin/env python3

# methods that provide a flattened view of properties present in the CR (custom resources)
# definitions available in t8c-operator

import os
from ruamel.yaml import YAML
from pathlib import Path


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

