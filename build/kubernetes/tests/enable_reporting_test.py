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
good_password = "12345"

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

    return data[key] if key in data else None


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
        sys.argv = ["enable_reporting_test.py", "--no-apply", "--file", customResourceFile, "--no-timescaledb"]
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
        input_set = [good_password, good_password]
        run_main(input_set)
        assert os.path.isfile(customResourceFileBak)
    
    def test_back_up_file_equivalent_to_original(self):
        original_copy = "{}.copy.test".format(customResourceFile)
        dumpStrToYaml(reportingDisabledYaml, original_copy)
        # enable/disable, grafana admin pass, grafana database pass
        input_set = [good_password, good_password]

        run_main(input_set, reportingDisabledYaml)

        assert filecmp.cmp(original_copy, customResourceFileBak)
        os.remove(original_copy)


class TestEmbeddedReporting:
    def test_validate(self):
        # All flags other than reporting.enabled are set to false, which means reporting won't work.
        # Both passwords are invalid.
        very_invalid_yaml = """
            spec:
              grafana:
                enabled: false
                adminPassword: a;
                grafana.ini:
                  database:
                    password: a
              extractor:
                enabled: false
              reporting:
                enabled: true
              timescaledb:
                enabled: false
            """
        # The fixture will delete this file after the test.
        dumpStrToYaml(very_invalid_yaml, customResourceFile)

        custom_resource = enable_reporting.CustomResource(customResourceFile)

        # The embedded reporting component.
        embedded_reporting = enable_reporting.EmbeddedReporting(custom_resource)
        warnings = embedded_reporting.validate()
        # Ensure we generated one warning for each thing wrong with the config.
        assert len(warnings) == 7

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
        input_set = [good_password, good_password]
        run_main(input_set)

        data = loadYaml(customResourceFile)
        assert getValue(data, 'spec grafana enabled')
        assert getValue(data, 'spec reporting enabled')
        assert getValue(data, 'spec timescaledb enabled')
        assert getValue(data, 'spec extractor enabled')

    def test_invalid_passwords_rejected(self):
        empty_pass = ""
        invalid_1 = "#mypass"
        invalid_2 = "my;pass"
        too_long_pass = "a" * 100
        too_short_pass = 'foo'
        grafana_password = 'valid'
        grafana_database_password = 'veryvalid'
        # Attempt to put in an empty string for both.
        input_set = [empty_pass, invalid_1, invalid_2, too_long_pass, too_short_pass, grafana_password,
                     empty_pass, invalid_1, invalid_2, too_long_pass, too_short_pass, grafana_database_password]
        run_main(input_set)

        data = loadYaml(customResourceFile)
        assert getValue(data, 'spec grafana adminPassword') == grafana_password
        assert getValue(data, 'spec grafana grafana.ini database password') == grafana_database_password

    def test_userinput_is_correct(self):
        grafana_password = 'foofoo'
        grafana_database_password = 'barbar'
        # enable/disable, grafana admin pass, grafana database pass
        input_set = [grafana_password, grafana_database_password]
        run_main(input_set)

        data = loadYaml(customResourceFile)
        assert getValue(data, 'spec grafana adminPassword') == grafana_password
        assert getValue(data, 'spec grafana grafana.ini database password') == grafana_database_password

    def test_TimescaleDBIP_is_present(self):
      missingTimescaleDBIPYaml = """
          spec:
            global:
              externalIP: 10.0.2.15
            grafana:
              enabled: false
              adminPassword: admin
              grafana.ini:
                database:
                  type: postgres
                  password: grafana
            extractor:
              enabled: false
            reporting:
              enabled: false
            timescaledb:
              enabled: false
          """

      input_set = [good_password, good_password]
      run_main(input_set, missingTimescaleDBIPYaml)
      data = loadYaml(customResourceFile)
      assert getValue(data, 'spec global externalTimescaleDBIP') != None

    def test_database_type_is_present(self):
      missingDatabaseTypeYaml = """
          spec:
            grafana:
              enabled: false
              adminPassword: admin
              grafana.ini:
                database:
                  password: grafana
            extractor:
              enabled: false
            reporting:
              enabled: false
            timescaledb:
              enabled: false
          """

      input_set = [good_password, good_password]
      run_main(input_set, missingDatabaseTypeYaml)
      data = loadYaml(customResourceFile)
      assert getValue(data, 'spec grafana grafana.ini database type') != None