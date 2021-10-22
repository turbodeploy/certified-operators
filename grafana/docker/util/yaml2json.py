#!/usr/bin/env python3

from logger import logger
import ruamel.yaml
import json
import os
import sys

global yaml


# Utility convert YAML files to JSON. This is primarily used for dashboards, which we manage as
# YAML files in our code repo so that long multi-line strings like embedded SQL queries can be
# reasonably examined in code reviews, but which must be delivered to Grafana as JSON files.

def main(in_files):
    """
    Convert the named YAML files to JSON. The YAML files must be named '*.yaml' and are removed
    after conversion as a side effect.

    :param in_files: paths to YAML files to be converted
    """
    for in_file in in_files:
        if in_file.lower().endswith('.yaml'):
            logger.info(f'Converting YAML file {in_file} to JSON')
            try:
                out_file = in_file[:-len('yaml')] + 'json'
                with open(in_file) as yaml_in:
                    data = yaml.load(yaml_in)
                with open(out_file, 'w') as json_out:
                    json.dump(data, json_out, indent=2)
                os.remove(in_file)
            except:
                logger.exception("Failed to convert file content")
        else:
            logger.warning(f'Unable to convert non-yaml file: {in_file}')


if __name__ == '__main__':
    """
    Command line invocation - establish globals and convert named files.
    """
    yaml = ruamel.yaml.YAML(typ='safe')
    main(sys.argv[1:])
