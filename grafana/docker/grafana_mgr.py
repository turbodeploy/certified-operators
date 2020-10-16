#!/usr/bin/env python3

import google.protobuf.empty_pb2
import grpc
import jwt
import licensing.Licensing_pb2_grpc
import logging
import os
import psutil
import setting.Setting_pb2_grpc
import sys
import time
from dataclasses import dataclass
from licensing.Licensing_pb2 import LicenseDTO, GetLicensesRequest, LicenseType
from setting.Setting_pb2 import GetMultipleGlobalSettingsRequest
from urllib.parse import urlparse

"""
REMEMBER TO SET PYTHON_PATH IN THE ENTRYPOINT.

This script is responsible for:
1) Starting Grafana at startup.
2) Fetching related information (e.g. license, e-mail settings) from the XL platform
   and injecting it into the Grafana configuration.
3) Restarting Grafana when necessary in order for the configuration from 2) to take effect.
"""

_LOGGER = logging.getLogger("grafana_mgr")

@dataclass(eq = True)
class SmtpConfig:
    host: str
    user: str
    password: str
    from_addr: str
    from_name: str = "Turbonomic"
    encryption: str = "NONE"

    def inject_into(self, env):
        # Note - these will override any SMTP settings set via the configmap.
        # For now this is okay, since we want the turbo-specified settings to take precedence.
        # In the future it may be good to allow the configmap to have an effect.
        env["GF_SMTP_ENABLED"] = "true"
        env["GF_SMTP_HOST"] = self.host
        env["GF_SMTP_USER"] = self.user
        env["GF_SMTP_PASSWORD"] = self.password
        env["GF_SMTP_FROM_NAME"] = self.from_name
        env["GF_SMTP_FROM_ADDR"] = self.from_addr
        if self.encryption == "SSL":
            env["GF_SMTP_SKIP_VERIFY"] = "false"
            env.pop("GF_SMTP_STARTTLS_POLICY", None)
        elif self.encryption == "TLS":
            env["GF_SMTP_SKIP_VERIFY"] = "true"
            env["GF_SMTP_STARTTLS_POLICY"] = "MandatoryStartTLS"
        else:
            env["GF_SMTP_SKIP_VERIFY"] = "true"
            env.pop("GF_SMTP_STARTTLS_POLICY", None)


class Grafana:
    """
    Object representing the Grafana process.
    Grafana runs as a sub-process to "grafana_mgr", and is restarted as necessary in order for
    config changes to take effect.
    """

    # This is a reference to the external Grafana process, which is started as a subprocess of
    # the grafana manager, and restarted as necessary when configuration changes require it.
    process = None

    def __init__(self, home_path, config_path, license_path):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.home_path = home_path
        self.config_path = config_path
        self.license_path = license_path
        self.grafana_env = os.environ.copy()
        self.grafana_env["GF_ENTERPRISE_LICENSE_PATH"] = self.license_path
        # There is one user that gets the "Admin" role (by default that's the "administrator"
        # account). All other users share the "report-viewer" user with a "Viewer" role.
        self.grafana_env["GF_USERS_AUTO_ASSIGN_ORG_ROLE"] = "Admin"

    def reboot(self):
        """ Restart the Grafana server. If the server is not up, starts the server. """
        self.logger.info("Restarting Grafana...")
        self.shutdown()
        self.logger.info("Starting new Grafana server instance.")
        self.process = psutil.Popen([self.home_path + "/bin/grafana-server",
                                     "--homepath=" + self.home_path, "--config=" + self.config_path,
                                     "--packaging=docker"],
                                    env=self.grafana_env)
        self.logger.info("Started Grafana server process.")

    def ensureStarted(self):
        if not self.isRunning():
            self.logger.warn("Grafana is not running (or in zombie mode). Restarting...")
            self.reboot()

    def isRunning(self):
        # The process has to exist, be running, and have a "RUNNING" status.
        # If grafana shuts down (e.g. due to failure to connect to Postgres to store its data)
        # it will be in a "Zombie" status.
        return self.process is not None and self.process.is_running() and not self.process.status() == psutil.STATUS_ZOMBIE

    def shutdown(self):
        """
          Shut down the Grafana server.
          No effect if the Grafana server is not up.
          This only works to shut down the Grafana server started up by the grafana_mgr. No effect
          if there is another grafana server running on the system.

        :return: Nothing.
        """
        if self.process:
            self.logger.info("Shutting down existing Grafana server...")
            self.process.terminate()
            try:
                self.logger.info("Waiting for Grafana server to shut down...")
                self.process.wait(30)
            except psutil.TimeoutExpired:
                self.logger.info("Killing Grafana server!!!")
                self.process.kill()
            self.logger.info("Shut down existing Grafana server.")

    def clear_license(self):
        """ Delete the enterprise license. Causes a server restart. """
        if os.path.exists(self.license_path):
            self.logger.info("Removing license file: %s", self.license_path)
            os.remove(self.license_path)
            self.reboot()

    def refresh_smtp(self, new_config):
        """ Refresh the SMTP configuration of the server. Causes a server restart. """
        self.logger.info("Refreshing SMTP config.")
        new_config.inject_into(self.grafana_env)
        self.reboot()

    def overwrite_license(self, new_content):
        """ Overwrite (or set) the enterprise license. Causes a server restart. """
        # We need to set the server domain to be the same.
        decoded_license = jwt.decode(new_content, verify=False)
        issuer = decoded_license.get('iss')
        if issuer != "https://grafana.com":
            self.logger.error("Definitely the wrong license. Issuer: " + issuer)
            return

        # Take the subdomain from the license, and inject it into the environment.
        licensed_domain = decoded_license.get('sub')
        licensed_subpath = urlparse(licensed_domain).path
        # If the licensed subpath is in sync with the "/reports" sub-path we serve Grafana at,
        # force the root URL to align with what's in the license. Grafana expects an exact
        # match between the root URL and the licensed domain, or else the license validation will
        # fail. It's unclear if there are any undesireable side effects from doing this.
        #
        # We check to make sure the licensed subpath is correct because overriding the root URL in
        # a way that changes the subpath will break the reverse proxy, and make Grafana
        # inaccessible from the Turbonomic UI, which is bad!
        if licensed_subpath in ["/reports", "/reports/"]:
            self.grafana_env["GF_SERVER_ROOT_URL"] = licensed_domain
        else:
            self.logger.warning("License sub-path %s does not match reverse proxy path.", licensed_subpath)

        if (os.path.exists(self.license_path)):
            self.logger.info("Overwriting contents of license file: %s", self.license_path)
            # Remove the old license before writing the new one.
            os.remove(self.license_path)
        else:
            self.logger.info("Writing license file: %s", self.license_path)

        with open(self.license_path, 'w') as license_file:
            license_file.write(new_content)
        self.reboot()

class SmtpUpdateOperation:
    """
    Responsible for polling the group component, looking at the global e-mail settings, and,
    if all necessary settings are set, injecting the e-mail SMTP settings into the Grafana
    configuration.
    """
    SMTP_SERVER = "smtpServer"
    SMTP_PORT = "smtpPort"
    FROM_ADDR = "fromAddress"
    SMTP_USER = "smtpUsername"
    SMTP_PASS = "smtpPassword"
    SMTP_ENCRYPTION = "smtpEncryption"

    REQUIRED = [SMTP_SERVER, SMTP_PORT, FROM_ADDR]
    OPTIONAL = [SMTP_ENCRYPTION, SMTP_USER, SMTP_PASS]

    logger = _LOGGER

    cur_smtp_config = None

    def __init__(self, grafana, group_channel):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.setting_stub = setting.Setting_pb2_grpc.SettingServiceStub(group_channel)
        self.grafana = grafana

    def run(self):
        self.logger.debug("Polling group component for SMTP settings")
        try:
            req = GetMultipleGlobalSettingsRequest()
            req.setting_spec_name.extend(self.REQUIRED)
            req.setting_spec_name.extend(self.OPTIONAL)
            setting_map = {}
            for setting in self.setting_stub.GetMultipleGlobalSettings(req):
                val = setting.string_setting_value.value
                if setting.setting_spec_name == self.SMTP_ENCRYPTION:
                    val = setting.enum_setting_value.value
                elif setting.setting_spec_name == self.SMTP_PORT:
                    val = setting.numeric_setting_value.value
                setting_map[setting.setting_spec_name] = val

            for req_name in self.REQUIRED:
                if not setting_map.get(req_name):
                    self.logger.debug("Missing SMTP setting %s. No e-mail injection", req_name)
                    return
            port_str = str(int(setting_map.get(self.SMTP_PORT)))
            new_config = SmtpConfig(
                host=setting_map.get(self.SMTP_SERVER) + ":" + port_str,
                user=setting_map.get(self.SMTP_USER),
                password=setting_map.get(self.SMTP_PASS),
                from_addr=setting_map.get(self.FROM_ADDR),
                encryption=setting_map.get(self.SMTP_ENCRYPTION)
            )
            if self.cur_smtp_config is None or self.cur_smtp_config != new_config:
                self.logger.debug("SMTP config change detected.")
                self.grafana.refresh_smtp(new_config)
                # Only overwrite the STMP config after the refresh is successfull.
                self.cur_smtp_config = new_config
            else:
                self.logger.debug("SMTP config unchanged.")
        except grpc.RpcError as rpc_error:
            self.logger.error('gRPC call to group failed: %s', rpc_error)



class LicenseUpdateOperation:
    """
    Responsible for polling the auth component for license changes, and overwriting
    the Grafana license when necessary.
    """

    NO_LICENSE_CHECKSUM = 0
    last_processed_checksum = NO_LICENSE_CHECKSUM
    logger = _LOGGER

    def __init__(self, grafana, auth_channel):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.license_stub = licensing.Licensing_pb2_grpc.LicenseManagerServiceStub(auth_channel)
        self.summary_stub = licensing.Licensing_pb2_grpc.LicenseCheckServiceStub(auth_channel)
        self.grafana = grafana

    def run(self):
        """ Run the operation. This should be done in a loop. """
        self.logger.debug("Polling auth for license summary.")
        try:
            summary_resp = self.summary_stub.getLicenseSummary(google.protobuf.empty_pb2.Empty())
            grafana_summaries = [summary for summary in
                                 summary_resp.licenseSummary.external_licenses_by_type if
                                 summary.type == LicenseDTO.ExternalLicense.Type.GRAFANA]
            new_checksum = grafana_summaries[0].checksum if len(grafana_summaries) > 0 else self.NO_LICENSE_CHECKSUM
            if new_checksum == self.last_processed_checksum:
                self.logger.debug("Grafana license summary checksum unchanged (%s).",
                              self.last_processed_checksum)
            else:
                self.logger.debug("Change detected. Last checksum: %s. New checksum: %s.",
                              self.last_processed_checksum, new_checksum)
                # Get the actual license.
                req = GetLicensesRequest()
                req.filter.type = LicenseType.EXTERNAL
                req.filter.include_expired = False
                req.filter.external_license_type = LicenseDTO.ExternalLicense.Type.GRAFANA
                response = self.license_stub.getLicenses(req)
                license_cnt = len(response.licenseDTO)
                if license_cnt > 0:
                    if license_cnt > 1:
                        self.logger.warn("Got %s grafana licenses. Using first available one.", license_cnt)
                    targetLicense = response.licenseDTO[0]
                    self.logger.info("Setting grafana licence to payload from: %s (uuid: %s)",
                                 targetLicense.filename, targetLicense.uuid)
                    self.grafana.overwrite_license(targetLicense.external.payload)
                else:
                    self.grafana.clear_license()

                # Don't update the checksum until we successfully processed the change.
                self.logger.info("Successfully processed Grafana license change.")
                self.last_processed_checksum = new_checksum
        except grpc.RpcError as rpc_error:
            self.logger.error('gRPC call to auth failed: %s', rpc_error)


def run():
    # We start up Grafana right away.
    home_path = os.environ["GF_PATHS_HOME"]
    config_path = os.environ["GF_PATHS_CONFIG"]
    license_path = os.environ.get("LICENSE_PATH")
    if license_path is None:
        license_path = "/tmp/license.jwt"

    auth_route = os.environ.get("AUTH_SERVICE_HOST")
    if auth_route is None:
        auth_route = "auth"
        _LOGGER.info("No auth host override provided. Will attempt with default: " + auth_route)
    auth_port = os.environ.get("AUTH_SERVICE_PORT_GRPC_AUTH")
    if auth_port is None:
        auth_port = "9001"
        _LOGGER.info("No auth port override provided. Will attempt with default: " + auth_port)

    group_route = os.environ.get("GROUP_SERVICE_HOST")
    if group_route is None:
        group_route = "group"
        _LOGGER.info("No group host override provided. Will attempt with default: " + group_route)
    group_port = os.environ.get("GROUP_SERVICE_PORT_GRPC_GROUP")
    if group_port is None:
        group_port = "9001"
        _LOGGER.info("No group port override provided. Will attempt with default: " + group_port)

    polling_interval_s = 30
    polling_interval_override = os.environ.get("POLL_INTERVAL_SEC")
    if polling_interval_override is not None:
        try:
            polling_interval_s = int(polling_interval_override)
        except ValueError:
            _LOGGER.warning("Invalid polling interval override: %s. Falling back to default",
                            polling_interval_override)

    grafana = Grafana(home_path, config_path, license_path)

    try:
        with grpc.insecure_channel(auth_route + ":" + auth_port) as auth_channel, \
                grpc.insecure_channel(group_route + ":" + group_port) as group_channel:
            license_update_op = LicenseUpdateOperation(grafana, auth_channel)
            email_update_op = SmtpUpdateOperation(grafana, group_channel)
            while True:
                try:
                    license_update_op.run()
                except:
                    _LOGGER.error("Failed to process auth response due to unexpected error:", sys.exc_info()[0])
                try:
                    email_update_op.run()
                except:
                    _LOGGER.error("Failed to process group response due to unexpected error:", sys.exc_info()[0])
                # Instead of sleeping for the polling interval, we sleep for 10 seconds at a time.
                # This is to check that Grafana is still running, and restart it if necessary.
                ten_sec_intervals = int(polling_interval_s / 10)
                for i in range(ten_sec_intervals):
                    # Ensure that the underlying Grafana process is still running.
                    # Grafana may crash, or fail to start up (e.g. if the extractor hasn't finished
                    # initializing the database users).
                    grafana.ensureStarted()
                    # TODO (roman, Sept 11 2020): Does this make it take longer to terminate the
                    # process?
                    time.sleep(10)
    except:
        _LOGGER.error("Shutting down after unexpected error:", sys.exc_info()[0])
        grafana.shutdown()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
