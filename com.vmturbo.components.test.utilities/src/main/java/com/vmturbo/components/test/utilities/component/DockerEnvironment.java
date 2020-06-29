package com.vmturbo.components.test.utilities.component;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.connection.DockerMachine.LocalBuilder;
import com.palantir.docker.compose.connection.DockerMachine.RemoteBuilder;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.components.test.utilities.component.ComponentCluster.Component;

/**
 * Utilities for managing the docker environment for deployed
 * components. These are a top-level class for better visibility.
 */
public class DockerEnvironment {

    private DockerEnvironment() {}

    /**
     * Port used for access kafka broker instance from the outside. Used to connect from
     * notification senders stubs.
     */
    private static int KAFKA_EXTERNAL_PORT = 9094;

    private static final String XMX_SUFFIX = "_XMX_MB";

    private static final String MEM_LIMIT_SUFFIX = "_MEM_LIMIT_MB";

    private static final String SYSTEM_PROPERTIES_SUFFIX = "_SYSTEM_PROPERTIES";

    /**
     * The function to get the -Xmx setting to use for a given mem limit.
     * Since the limit (in MB) won't be overly large, it's safe to do multiplication
     * first without fear of overflow.
     */
    private static final Function<Integer, Integer> MEM_LIMIT_TO_XMX = (limit) -> limit * 3 / 4;

    /**
     * These are environment variables that need to be set in the docker-compose.yml file.
     * See the build/.env file for the environment values in a normal dev deployment.
     */
    @VisibleForTesting
    static final ImmutableMap<String, String> ENVIRONMENT_VARIABLES =
        new ImmutableMap.Builder<String, String>()
            .put("DEV_JAVA_OPTS", "-agentlib:jdwp=transport=dt_socket,address=8000,server=y,suspend=n")
            .put("XL_VERSION", BuildProperties.get().getVersion())
            .put("METRON_ENABLED", "false")
            .put("CONSUL_PORT", "8500")
            .put("DB_PORT", "3306")
            .put("AUTH_PORT", "8080")
            .put("AUTH_DEBUG_PORT", "8000")
            .put("CLUSTERMGR_PORT", "8080")
            .put("CLUSTERMGR_DEBUG_PORT", "8000")
            .put("API_HTTP_PORT", "8080")
            .put("API_HTTPS_PORT", "9443")
            .put("API_DEBUG_PORT", "8000")
            .put("NGINX_HTTP_PORT", "80")
            .put("NGINX_HTTPS_PORT", "443")
            .put("MARKET_PORT", "8080")
            .put("MARKET_DEBUG_PORT", "8000")
            .put("ACTION_ORCHESTRATOR_PORT", "8080")
            .put("ACTION_ORCHESTRATOR_DEBUG_PORT", "8000")
            .put("TOPOLOGY_PROCESSOR_PORT", "8080")
            .put("TOPOLOGY_PROCESSOR_DEBUG_PORT", "8000")
            .put("ARANGODB_PORT", "8529")
            .put("ARANGODB_DUMP_PORT", "8599")
            .put("INFLUXDB_PORT", "8086")
            .put("INFLUXDB_DUMP_PORT", "8088")
            .put("ML_DATASTORE_PORT", "8086")
            .put("ML_DATASTORE_DEBUG_PORT", "8088")
            .put("REPOSITORY_PORT", "8080")
            .put("REPOSITORY_DEBUG_PORT", "8000")
            .put("MEDIATION_COMPONENT_PORT", "8080")
            .put("MEDIATION_COMPONENT_DEBUG_PORT", "8000")
            .put("GROUP_PORT", "8080")
            .put("GROUP_DEBUG_PORT", "8000")
            .put("HISTORY_PORT", "8080")
            .put("HISTORY_DEBUG_PORT", "8000")
            .put("EXTRACTOR_PORT", "8080")
            .put("EXTRACTOR_DEBUG_PORT", "8000")
            .put("INTEGRATION_INTERSIGHT_PORT", "8080")
            .put("INTEGRATION_INTERSIGHT_DEBUG_PORT", "8000")
            .put("RSYSLOG_PORT", "2514")



            .put("PLAN_ORCHESTRATOR_PORT", "8080")
            .put("PLAN_ORCHESTRATOR_DEBUG_PORT", "8000")
            .put("REPORTING_PORT", "8080")
            .put("REPORTING_DEBUG_PORT", "8000")
            .put("SAMPLE_PORT", "8080")
            .put("SAMPLE_DEBUG_PORT", "8000")
            .put("COST_PORT", "8080")
            .put("COST_DEBUG_PORT", "8000")
            .put("MEDIATION_VCENTER_PORT", "8080")
            .put("MEDIATION_VCENTER_DEBUG_PORT", "8000")
            .put("MEDIATION_VCENTER_BROWSING_PORT", "8080")
            .put("MEDIATION_VCENTER_BROWSING_DEBUG_PORT", "8000")
            .put("MEDIATION_STRESSPROBE_PORT", "8080")
            .put("MEDIATION_STRESSPROBE_DEBUG_PORT", "8000")
            .put("MEDIATION_STORAGESTRESSPROBE_PORT", "8080")
            .put("MEDIATION_STORAGESTRESSPROBE_DEBUG_PORT", "8000")
            .put("MEDIATION_DELEGATINGPROBE_PORT", "8080")
            .put("MEDIATION_DELEGATINGPROBE_DEBUG_PORT", "33591:8000")
            .put("MEDIATION_AIX_DEBUG_PORT", "8000")
            .put("MEDIATION_AIX_PORT", "8080")
            .put("MEDIATION_VMAX_DEBUG_PORT", "8000")
            .put("MEDIATION_VMAX_PORT", "8080")
            .put("MEDIATION_HPE3PAR_DEBUG_PORT", "8000")
            .put("MEDIATION_HPE3PAR_PORT", "8080")
            .put("MEDIATION_PURE_DEBUG_PORT", "8000")
            .put("MEDIATION_PURE_PORT", "8080")
            .put("MEDIATION_SCALEIO_DEBUG_PORT", "8000")
            .put("MEDIATION_SCALEIO_PORT", "8080")
            .put("MEDIATION_HDS_DEBUG_PORT", "8000")
            .put("MEDIATION_HDS_PORT", "8080")
            .put("MEDIATION_COMPELLENT_DEBUG_PORT", "8000")
            .put("MEDIATION_COMPELLENT_PORT", "8080")
            .put("MEDIATION_XTREMIO_DEBUG_PORT", "8000")
            .put("MEDIATION_XTREMIO_PORT", "8080")
            .put("MEDIATION_VPLEX_DEBUG_PORT", "8000")
            .put("MEDIATION_VPLEX_PORT", "8080")
            .put("MEDIATION_VMM_DEBUG_PORT", "8000")
            .put("MEDIATION_VMM_PORT", "8080")
            .put("MEDIATION_HYPERV_PORT", "8080")
            .put("MEDIATION_HYPERV_DEBUG_PORT", "8000")
            .put("MEDIATION_NETAPP_PORT", "8080")
            .put("MEDIATION_NETAPP_DEBUG_PORT", "8000")
            .put("MEDIATION_UCS_PORT", "8080")
            .put("MEDIATION_UCS_DEBUG_PORT", "8000")
            .put("MEDIATION_RHV_DEBUG_PORT", "8000")
            .put("MEDIATION_RHV_PORT", "8080")
            .put("MEDIATION_OPENSTACK_PORT", "8080")
            .put("MEDIATION_OPENSTACK_DEBUG_PORT", "8000")
            .put("MEDIATION_UCSDIRECTOR_PORT", "8080")
            .put("MEDIATION_UCSDIRECTOR_DEBUG_PORT", "8000")
            .put("MEDIATION_AWS_PORT", "8080")
            .put("MEDIATION_AWS_DEBUG_PORT", "8000")
            .put("MEDIATION_AWSBILLING_PORT", "8080")
            .put("MEDIATION_AWSBILLING_DEBUG_PORT", "8000")
            .put("MEDIATION_AWSCOST_PORT", "8080")
            .put("MEDIATION_DATADOG_PORT", "8080")
            .put("MEDIATION_AWSCOST_DEBUG_PORT", "8000")
            .put("MEDIATION_DATADOG_DEBUG_PORT", "8000")
            .put("MEDIATION_AZURE_PORT", "8080")
            .put("MEDIATION_AZURE_DEBUG_PORT", "8000")
            .put("MEDIATION_AZURECOST_PORT", "8080")
            .put("MEDIATION_AZURECOST_DEBUG_PORT", "8000")
            .put("MEDIATION_AZURE_VOLUMES_PORT", "8080")
            .put("MEDIATION_AZURE_VOLUMES_DEBUG_PORT", "8000")
            .put("MEDIATION_ONEVIEW_PORT", "8080")
            .put("MEDIATION_ONEVIEW_DEBUG_PORT", "8000")
            .put("MEDIATION_VCD_PORT", "8080")
            .put("MEDIATION_VCD_DEBUG_PORT", "8000")
            .put("MEDIATION_MSSQL_PORT", "8080")
            .put("MEDIATION_MSSQL_DEBUG_PORT", "8000")
            .put("MEDIATION_ACTIONSCRIPT_PORT", "8080")
            .put("MEDIATION_ACTIONSCRIPT_DEBUG_PORT", "8000")
            .put("MEDIATION_WMI_PORT", "8080")
            .put("MEDIATION_WMI_DEBUG_PORT", "8000")
            .put("MEDIATION_SNMP_PORT", "8080")
            .put("MEDIATION_SNMP_DEBUG_PORT", "8000")
            .put("MEDIATION_PIVOTAL_PORT", "8080")
            .put("MEDIATION_PIVOTAL_DEBUG_PORT", "8000")
            .put("MEDIATION_APPDYNAMICS_PORT", "8080")
            .put("MEDIATION_APPDYNAMICS_DEBUG_PORT", "8000")
            .put("MEDIATION_NEWRELIC_PORT", "8080")
            .put("MEDIATION_NEWRELIC_DEBUG_PORT", "8000")
            .put("MEDIATION_APPINSIGHTS_PORT", "8080")
            .put("MEDIATION_APPINSIGHTS_DEBUG_PORT", "8000")
            .put("MEDIATION_DYNATRACE_PORT", "8080")
            .put("MEDIATION_DYNATRACE_DEBUG_PORT", "8000")
            .put("MEDIATION_CLOUDFOUNDRY_PORT", "8080")
            .put("MEDIATION_CLOUDFOUNDRY_DEBUG_PORT", "8000")
            .put("MEDIATION_ISTIO_PORT", "8080")
            .put("MEDIATION_ISTIO_DEBUG_PORT", "8000")
            .put("MEDIATION_NETFLOW_PORT", "8080")
            .put("MEDIATION_NETFLOW_DEBUG_PORT", "8000")
            .put("MEDIATION_TETRATION_PORT", "8080")
            .put("MEDIATION_TETRATION_DEBUG_PORT", "8000")
            .put("MEDIATION_HYPERFLEX_PORT", "8080")
            .put("MEDIATION_HYPERFLEX_DEBUG_PORT", "8000")
            .put("MEDIATION_HORIZON_PORT", "8080")
            .put("MEDIATION_HORIZON_DEBUG_PORT", "8000")
            .put("MEDIATION_INTERSIGHT_HYPERFLEX_PORT", "8080")
            .put("MEDIATION_INTERSIGHT_HYPERFLEX_DEBUG_PORT", "8000")
            .put("MEDIATION_INTERSIGHT_UCS_PORT", "8080")
            .put("MEDIATION_INTERSIGHT_UCS_DEBUG_PORT", "8000")
            .put("MEDIATION_INTERSIGHT_SERVER_PORT", "8080")
            .put("MEDIATION_INTERSIGHT_SERVER_DEBUG_PORT", "8000")
            .put("MEDIATION_SERVICENOW_PORT", "8080")
            .put("MEDIATION_SERVICENOW_DEBUG_PORT", "8000")
            .put("MEDIATION_AZUREEA_PORT", "8080")
            .put("MEDIATION_AZUREEA_DEBUG_PORT", "8000")
            .put("MEDIATION_AZURESP_PORT", "8080")
            .put("MEDIATION_AZURESP_DEBUG_PORT", "8000")
            .put("MEDIATION_CUSTOMDATA_PORT", "8080")
            .put("MEDIATION_CUSTOMDATA_DEBUG_PORT", "8000")
            .put("MEDIATION_NUTANIX_PORT", "8080")
            .put("MEDIATION_NUTANIX_DEBUG_PORT", "8000")

            // MEMORY LIMITS and XMX Settings
            .put("DB_MEM_LIMIT_MB", "2048")
            .put("DB_MEM_PCT_FOR_BUFFER_POOL", "80")
            .put("ARANGODB_MEM_LIMIT_MB", "1024")
            .put("INFLUXDB_MEM_LIMIT_MB", "512")
            .put("AUTH_MEM_LIMIT_MB", "768")
            .put("AUTH_XMX_MB", "512")
            .put("CLUSTERMGR_MEM_LIMIT_MB", "512")
            .put("CLUSTERMGR_XMX_MB", "384")
            .put("API_MEM_LIMIT_MB", "512")
            .put("API_XMX_MB", "384")
            .put("MARKET_XMX_MB", "384")
            .put("MARKET_MEM_LIMIT_MB", "512")
            .put("ACTION_ORCHESTRATOR_XMX_MB", "384")
            .put("ACTION_ORCHESTRATOR_MEM_LIMIT_MB", "512")
            .put("TOPOLOGY_PROCESSOR_XMX_MB", "1024")
            .put("TOPOLOGY_PROCESSOR_MEM_LIMIT_MB", "2048")
            .put("SAMPLE_XMX_MB", "384")
            .put("SAMPLE_MEM_LIMIT_MB", "512")
            .put("REPOSITORY_XMX_MB", "768")
            .put("REPOSITORY_MEM_LIMIT_MB", "1024")
            .put("GROUP_XMX_MB", "384")
            .put("GROUP_MEM_LIMIT_MB", "512")
            .put("COST_XMX_MB", "384")
            .put("COST_MEM_LIMIT_MB", "512")
            .put("ML_DATASTORE_XMX_MB", "384")
            .put("ML_DATASTORE_MEM_LIMIT_MB", "512")
            .put("HISTORY_XMX_MB", "768")
            .put("HISTORY_MEM_LIMIT_MB", "1024")
            .put("PLAN_ORCHESTRATOR_XMX_MB", "384")
            .put("PLAN_ORCHESTRATOR_MEM_LIMIT_MB", "512")
            .put("REPORTING_XMX_MB", "384")
            .put("REPORTING_MEM_LIMIT_MB", "512")
            .put("MEDIATION_COMPONENT_XMX_MB", "384")
            .put("MEDIATION_COMPONENT_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VCENTER_XMX_MB", "384")
            .put("MEDIATION_VCENTER_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VCENTER_BROWSING_XMX_MB", "384")
            .put("MEDIATION_VCENTER_BROWSING_MEM_LIMIT_MB", "512")
            .put("MEDIATION_HYPERV_XMX_MB", "384")
            .put("MEDIATION_HYPERV_MEM_LIMIT_MB", "512")
            .put("MEDIATION_NETAPP_XMX_MB", "384")
            .put("MEDIATION_NETAPP_MEM_LIMIT_MB", "512")
            .put("MEDIATION_UCS_XMX_MB", "384")
            .put("MEDIATION_UCS_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VMAX_XMX_MB", "384")
            .put("MEDIATION_VMAX_MEM_LIMIT_MB", "512")
            .put("MEDIATION_HPE3PAR_XMX_MB", "384")
            .put("MEDIATION_HPE3PAR_MEM_LIMIT_MB", "512")
            .put("MEDIATION_PURE_XMX_MB", "384")
            .put("MEDIATION_PURE_MEM_LIMIT_MB", "512")
            .put("MEDIATION_SCALEIO_XMX_MB", "384")
            .put("MEDIATION_SCALEIO_MEM_LIMIT_MB", "512")
            .put("MEDIATION_HDS_XMX_MB", "384")
            .put("MEDIATION_HDS_MEM_LIMIT_MB", "512")
            .put("MEDIATION_COMPELLENT_XMX_MB", "384")
            .put("MEDIATION_COMPELLENT_MEM_LIMIT_MB", "512")
            .put("MEDIATION_XTREMIO_XMX_MB", "384")
            .put("MEDIATION_XTREMIO_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VPLEX_XMX_MB", "384")
            .put("MEDIATION_VPLEX_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VMM_XMX_MB", "384")
            .put("MEDIATION_VMM_MEM_LIMIT_MB", "512")
            .put("MEDIATION_RHV_XMX_MB", "384")
            .put("MEDIATION_RHV_MEM_LIMIT_MB", "512")
            .put("MEDIATION_OPENSTACK_XMX_MB", "384")
            .put("MEDIATION_OPENSTACK_MEM_LIMIT_MB", "512")
            .put("MEDIATION_UCSDIRECTOR_XMX_MB", "384")
            .put("MEDIATION_UCSDIRECTOR_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AWS_XMX_MB", "384")
            .put("MEDIATION_AWS_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AWSBILLING_XMX_MB", "384")
            .put("MEDIATION_AWSBILLING_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AWSCOST_XMX_MB", "384")
            .put("MEDIATION_AWSCOST_MEM_LIMIT_MB", "512")
            .put("MEDIATION_DATADOG_XMX_MB", "384")
            .put("MEDIATION_DATADOG_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AZURE_XMX_MB", "384")
            .put("MEDIATION_AZURE_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AZURE_VOLUMES_XMX_MB", "384")
            .put("MEDIATION_AZURE_VOLUMES_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AZURECOST_MEM_LIMIT_MB", "512")
            .put("MEDIATION_ONEVIEW_XMX_MB", "384")
            .put("MEDIATION_ONEVIEW_MEM_LIMIT_MB", "512")
            .put("MEDIATION_VCD_XMX_MB", "384")
            .put("MEDIATION_VCD_MEM_LIMIT_MB", "512")
            .put("MEDIATION_MSSQL_XMX_MB", "384")
            .put("MEDIATION_MSSQL_MEM_LIMIT_MB", "512")
            .put("MEDIATION_WMI_XMX_MB", "384")
            .put("MEDIATION_WMI_MEM_LIMIT_MB", "512")
            .put("MEDIATION_SNMP_XMX_MB", "384")
            .put("MEDIATION_SNMP_MEM_LIMIT_MB", "512")
            .put("MEDIATION_APPDYNAMICS_XMX_MB", "384")
            .put("MEDIATION_APPDYNAMICS_MEM_LIMIT_MB", "512")
            .put("MEDIATION_NEWRELIC_XMX_MB", "384")
            .put("MEDIATION_NEWRELIC_MEM_LIMIT_MB", "512")
            .put("MEDIATION_APPINSIGHTS_XMX_MB", "384")
            .put("MEDIATION_APPINSIGHTS_MEM_LIMIT_MB", "512")
            .put("MEDIATION_DYNATRACE_XMX_MB", "384")
            .put("MEDIATION_DYNATRACE_MEM_LIMIT_MB", "512")
            .put("MEDIATION_PIVOTAL_XMX_MB", "384")
            .put("MEDIATION_PIVOTAL_MEM_LIMIT_MB", "512")
            .put("MEDIATION_CLOUDFOUNDRY_XMX_MB", "384")
            .put("MEDIATION_CLOUDFOUNDRY_MEM_LIMIT_MB", "512")
            .put("MEDIATION_ISTIO_XMX_MB", "384")
            .put("MEDIATION_ISTIO_MEM_LIMIT_MB", "512")
            .put("MEDIATION_NETFLOW_XMX_MB", "384")
            .put("MEDIATION_NETFLOW_MEM_LIMIT_MB", "512")
            .put("MEDIATION_TETRATION_XMX_MB", "384")
            .put("MEDIATION_TETRATION_MEM_LIMIT_MB", "512")
            .put("MEDIATION_HYPERFLEX_XMX_MB", "384")
            .put("MEDIATION_HYPERFLEX_MEM_LIMIT_MB", "512")
            .put("MEDIATION_AIX_XMX_MB", "384")
            .put("MEDIATION_AIX_MEM_LIMIT_MB", "512")
            .put("MEDIATION_STRESSPROBE_XMX_MB", "768")
            .put("MEDIATION_STRESSPROBE_MEM_LIMIT_MB", "1024")
            .put("MEDIATION_STORAGESTRESSPROBE_XMX_MB", "384")
            .put("MEDIATION_STORAGESTRESSPROBE_MEM_LIMIT_MB", "512")
            .put("MEDIATION_DELEGATINGPROBE_XMX_MB", "384")
            .put("MEDIATION_DELEGATINGPROBE_MEM_LIMIT_MB", "512")
            .put("MEDIATION_ACTIONSCRIPT_XMX_MB", "384")
            .put("MEDIATION_ACTIONSCRIPT_MEM_LIMIT_MB", "512")
            .put("MEDIATION_HORIZON_XMX_MB", "384")
            .put("MEDIATION_HORIZON_MEM_LIMIT_MB", "512")
                // KAFKA PROPERTIES
            .put("KAFKA_LOG_RETENTION_HRS", "24")
            .put("KAFKA_MAX_MESSAGE_BYTES", "67108864")
            .put("KAFKA_INTERNAL_PORT", "9092")
            .put("KAFKA_INTERNAL_BROKER_ADDRESS", "kafka1")
            .put("KAFKA_EXTERNAL_PORT", Integer.toString(KAFKA_EXTERNAL_PORT))
            .put("KAFKA_EXTERNAL_BROKER_ADDRESS", getDockerHostName())
            .put("KAFKA_SERVERS", "kafka1:9092")
            .put("KAFKA_XMX_MB", "1024")
            .put("KAFKA_MEM_LIMIT", "20000")
            .put("KAFKA_LOG_TIMESTAMP_TYPE", "LogAppendTime")
            .put("ZOOKEEPER_SERVERS", "zoo1:2181")
            .put("ZOOKEEPER_MEM_LIMIT", "200")
            .put("ZOOKEEPER_XMX_MB", "100")

                // SYSTEM PROPERTIES
            .put("AUTH_SYSTEM_PROPERTIES", "")
            .put("CLUSTERMGR_SYSTEM_PROPERTIES", "")
            .put("API_SYSTEM_PROPERTIES", "")
            .put("MARKET_SYSTEM_PROPERTIES", "")
            .put("ACTION_ORCHESTRATOR_SYSTEM_PROPERTIES", "")
            .put("TOPOLOGY_PROCESSOR_SYSTEM_PROPERTIES", "")
            .put("SAMPLE_SYSTEM_PROPERTIES", "")
            .put("REPOSITORY_SYSTEM_PROPERTIES", "")
            .put("GROUP_SYSTEM_PROPERTIES", "")
            .put("HISTORY_SYSTEM_PROPERTIES", "")
            .put("PLAN_ORCHESTRATOR_SYSTEM_PROPERTIES", "")
            .put("REPORTING_SYSTEM_PROPERTIES", "")
            .put("COST_SYSTEM_PROPERTIES", "")
            .put("ML_DATASTORE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_COMPONENT_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VCENTER_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VCENTER_BROWSING_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_HYPERV_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_NETAPP_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_UCS_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VMAX_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_HPE3PAR_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_PURE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_SCALEIO_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_HDS_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_COMPELLENT_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_XTREMIO_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VPLEX_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VMM_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_RHV_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_OPENSTACK_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_UCSDIRECTOR_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AWS_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AWSBILLING_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AWSCOST_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_DATADOG_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AZURE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AZURECOST_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AZURE_VOLUMES_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_ONEVIEW_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_VCD_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_MSSQL_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_WMI_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_SNMP_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_APPDYNAMICS_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_NEWRELIC_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_APPINSIGHTS_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_DYNATRACE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_PIVOTAL_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_CLOUDFOUNDRY_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_ISTIO_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_NETFLOW_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_TETRATION_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_HYPERFLEX_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_AIX_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_STRESSPROBE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_STORAGESTRESSPROBE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_DELEGATINGPROBE_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_ACTIONSCRIPT_SYSTEM_PROPERTIES", "")
            .put("MEDIATION_HORIZON_SYSTEM_PROPERTIES", "")
            .build();

    /**
     * Construct a builder for starting up the specified components running in a docker instance running locally
     * with an appropriately configured environment.
     *
     * @param components The components whose environment variables should be set on the {@link LocalBuilder}.
     *                   Keys should be the component name (ie "market" or "topology-processor").
     *
     * @return An {@link LocalBuilder} instance capable of bringing up the specified containers.
     */
    public static LocalBuilder newLocalMachine(final Map<String, Component> components) {
        LocalBuilder builder = DockerMachine.localMachine();
        getEnvironmentVariables(components).forEach(builder::withAdditionalEnvironmentVariable);
        return builder;
    }

    /**
     * Construct a builder for starting up the specified components running in a docker instance running remotely
     * with an appropriately configured environment.
     *
     * @param components The components whose environment variables should be set on the {@link RemoteBuilder}.
     *                   Keys should be the component name (ie "market" or "topology-processor").
     *
     * @return A {@link RemoteBuilder} instance capable of bringing up the specified containers.
     */
    public static RemoteBuilder newRemoteMachine(final Map<String, Component> components) {
        RemoteBuilder builder = DockerMachine.remoteMachine();
        getEnvironmentVariables(components).forEach(builder::withAdditionalEnvironmentVariable);
        return builder;
    }

    public static String[] getDockerComposeFiles() {
        // See the path anchor file (located in resources/pathAnchor) for an explanation
        // of why we use it.
        final URL resource = ComponentUtils.class.getClassLoader().getResource("pathAnchor");
        Objects.requireNonNull(resource, "Unable to load pathAnchor." +
        "You must have a file called \"pathAnchor\" at ${project}/src/test/resources/");
        Path path = ResourcePath.getTestResource(ComponentUtils.class, "pathAnchor");
        // Go up to top-level XL directory.
        // This won't work if the layout of the modules changes!
        Path rootDir = path.getParent().getParent().getParent().getParent();

        // create the list of docker-compose files we want to load
        final String pathStr = rootDir.toString() + File.separator + "build" + File.separator;

        // separate the files into list of those that exist and those that don't
        Map<Boolean, List<String>> foundAndNotFoundFiles =
            Stream.of("docker-compose.yml","docker-compose.test.yml")
                .map(filename -> pathStr + filename)
                .map(File::new)
                .collect(Collectors.partitioningBy(File::exists,
                    Collectors.mapping(File::toString, Collectors.toList())));

        // if any of the files don't exist, throw an error
        if (foundAndNotFoundFiles.get(false).size() > 0) {
            throw new IllegalStateException("File(s) not found: " + String.join(",", foundAndNotFoundFiles.get(false)));
        }

        // otherwise, all of the files exist -- return the paths to the files
        List<String> files = foundAndNotFoundFiles.get(true);
        return files.toArray(new String[files.size()]);
    }

    @VisibleForTesting
    static Map<String, String> getEnvironmentVariables(final Map<String, Component> components) {
        final Map<String, String> environmentVariables = new HashMap<>(ENVIRONMENT_VARIABLES);
        components.forEach((name, component) -> {
            applyMemoryLimit(name, component, environmentVariables);
            applySystemProperties(name, component, environmentVariables);
        });
        return environmentVariables;
    }

    private static void applyMemoryLimit(@Nonnull final String componentName,
                                         @Nonnull final Component component,
                                         @Nonnull final Map<String, String> environmentVariables) {
        component.getMemoryLimitMb().ifPresent(limit -> {
            final String componentNamePrefix =componentNamePrefix(componentName);
            final String xmxLimit = componentNamePrefix + XMX_SUFFIX;
            final String memLimit = componentNamePrefix + MEM_LIMIT_SUFFIX;
            environmentVariables.put(memLimit, Integer.toString(limit));
            environmentVariables.put(xmxLimit, Integer.toString(MEM_LIMIT_TO_XMX.apply(limit)));
        });
    }

    private static void applySystemProperties(@Nonnull final String componentName,
                                              @Nonnull final Component component,
                                              @Nonnull final Map<String, String> environmentVariables) {
        if (component.getSystemProperties().isEmpty()) {
            return;
        }

        final String systemPropertiesVariable = componentNamePrefix(componentName) + SYSTEM_PROPERTIES_SUFFIX;
        final String systemPropertiesValues = component.getSystemProperties().entrySet().stream()
            .map(property -> String.format("-D%s=%s", property.getKey(), property.getValue()))
            .collect(Collectors.joining(" "));

        environmentVariables.put(systemPropertiesVariable, systemPropertiesValues);
    }

    /**
     * We rely on the standard that properties are prefixed with the name of the
     * component, with - replaced by _ (because docker environment variables can't
     * have dashes).
     *
     * @param componentName The component name to transform into an environment variable prefix.
     * @return The environment variable prefix version of the component name.
     */
    private static String componentNamePrefix(@Nonnull final String componentName) {
        return StringUtils.upperCase(componentName.replace("-", "_"));
    }

    /**
     * Returns the actual docker host name (or ip address). This name is used to configure Kafka
     * external advertised host.
     *
     * It is determined by DOCKER_HOST variable. If it is not set, localhost is used.
     * @return
     */
    @Nonnull
    private static String getDockerHostName() {
        final String envVar = System.getenv("DOCKER_HOST");
        if (envVar == null) {
            return "localhost";
        } else {
            try {
                final String host = new URI(envVar).getHost();
                // If we can't extract a host from the URI, fall back
                // to localhost.
                return host == null ? "localhost" : host;
            } catch (URISyntaxException e) {
                throw new RuntimeException(
                        "Could not get host name from DOCKER_HOST variable " + envVar, e);
            }
        }
    }

    @Nonnull
    public static String getKafkaBootstrapServers() {
        return getDockerHostName() + ":" + Integer.toString(KAFKA_EXTERNAL_PORT);
    }
}
