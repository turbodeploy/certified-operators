from bin import enable_reporting
import filecmp
import io
import os
import pytest
import sys
from shutil import copyfile
from ruamel.yaml import YAML

# Auto generated temporary yaml file
customResourceFile = "custom-resource.temp.test"
# name of back up file created
customResourceFileBak = "{}.bak".format(customResourceFile)

# yaml string to perform tests on
# (flushed out as yaml file first)
reportingDisabledYaml = """
spec:
  grafana:
    enabled: false
    adminPassword: admin
    grafana.ini:
      database:
        type: postgres
        password: grafana
  extractor:
    enabled: true
  reporting:
    enabled: false
  timescaledb:
    enabled: false
"""

reportingEnabledYaml = """
spec:
  grafana:
    enabled: false
    adminPassword: admin
    grafana.ini:
      database:
        type: postgres
        password: grafana
  extractor:
    enabled: true
  reporting:
    enabled: true
  timescaledb:
    enabled: false
"""

# For each test, at the end of the test, the yaml file and back up yaml file are deleted
@pytest.fixture(autouse=True)
def maintain_clean_state():
    # BEFORE
    yield
    # AFTER
    try:
        os.remove(customResourceFile)
        os.remove(customResourceFileBak)
    except OSError:
        pass


# Load yaml file 
def loadYaml(yamlFile):
    yaml = YAML()
    with open(yamlFile) as file:
        try:
            return yaml.load(file)
        except:
            print("Failed to load CR file {}".format(yamlFile))


# retrieves value from yaml data
# returns None if key/path does not exist
def getValue(data, path):
    path = path.split()
    key = path.pop()

    for p in path:
        if p not in data:
            return None
        data = data[p]

    return data[key]


# writes yaml string tp yaml file
def dumpStrToYaml(data, filename):
    yaml = YAML()
    data = yaml.load(data)
    with open(filename, 'w') as file:
        yaml.dump(data, file)


def run_main(input_set, yaml_str=reportingDisabledYaml, validate=False):
    # For each test, the in memory yaml is written out as a file
    # At the end of each test, the yaml file and back up yaml file are deleted
    dumpStrToYaml(yaml_str, customResourceFile)

    old_sys_argv = sys.argv
    old_stdin = sys.stdin
    try:
        sys.argv = ["enable_reporting_test.py", "--file", customResourceFile]
        if validate:
            sys.argv.append("--validate")
        sys.stdin = io.StringIO('\n'.join(input_set))
        enable_reporting.main()
    finally:
        sys.argv = old_sys_argv
        sys.stdin = old_stdin


class TestCustomResourceLoader:
    def test_back_up_created_on_successful_changes(self):
        # enable/disable, grafana admin pass, grafana database pass
        input_set = ['123', '123']
        run_main(input_set)
        assert os.path.isfile(customResourceFileBak)
    
    def test_back_up_file_equivalent_to_original(self):
        original_copy = "{}.copy.test".format(customResourceFile)
        dumpStrToYaml(reportingDisabledYaml, original_copy)
        # enable/disable, grafana admin pass, grafana database pass
        input_set = ['123', '123']

        run_main(input_set, reportingDisabledYaml)

        assert filecmp.cmp(original_copy, customResourceFileBak)
        os.remove(original_copy)


class TestEmbeddedReporting:
    def test_reporting_already_enabled(self):
        original_copy = "{}.copy.test".format(customResourceFile)
        dumpStrToYaml(reportingEnabledYaml, original_copy)

        run_main([], reportingEnabledYaml)

        # No changes to the file.
        assert filecmp.cmp(original_copy, customResourceFile)
        os.remove(original_copy)

    def test_validate_no_change(self):
        original_copy = "{}.copy.test".format(customResourceFile)

        # Even though we are passing in a file with reporting disabled, running in "validate"
        # mode should not cause reporting to become enabled.
        dumpStrToYaml(reportingDisabledYaml, original_copy)

        run_main([], reportingDisabledYaml, validate=True)

        # No changes to the file.
        assert filecmp.cmp(original_copy, customResourceFile)
        os.remove(original_copy)

    def test_enabled_embedded_reporting(self):
        # enable/disable, grafana admin pass, grafana database pass
        input_set = ['123', '123']
        run_main(input_set)

        data = loadYaml(customResourceFile)
        assert getValue(data, 'spec grafana enabled')
        assert getValue(data, 'spec reporting enabled')
        assert getValue(data, 'spec timescaledb enabled')
        assert getValue(data, 'spec extractor enabled')
    
    def test_userinput_is_correct(self):
        grafana_password = 'foo'
        grafana_database_password = 'bar'
        # enable/disable, grafana admin pass, grafana database pass
        input_set = [grafana_password, grafana_database_password]
        run_main(input_set)

        data = loadYaml(customResourceFile)
        assert getValue(data, 'spec grafana adminPassword') == grafana_password
        assert getValue(data, 'spec grafana grafana.ini database password') == grafana_database_password
