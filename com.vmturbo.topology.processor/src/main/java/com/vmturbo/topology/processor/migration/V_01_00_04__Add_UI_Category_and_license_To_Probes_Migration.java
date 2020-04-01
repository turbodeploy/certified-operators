package com.vmturbo.topology.processor.migration;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Migration for existing probes. It's needed to add new probe properties: uiProbeCategory and license.
 */
public class V_01_00_04__Add_UI_Category_and_license_To_Probes_Migration extends AbstractMigration {

    /**
     * The structure to store probe UI categories and license.
     */
    private final Map<String, Pair<String, String>> probeCategoryAndLicense = new HashMap<>();


    private final Logger logger = LogManager.getLogger();

    private final KeyValueStore keyValueStore;
    private final ObjectMapper mapper;

    /**
     * Create an instance of {@link V_01_00_04__Add_UI_Category_and_license_To_Probes_Migration}.
     *
     * @param keyValueStore from which to get persisted probes.
     */
    public V_01_00_04__Add_UI_Category_and_license_To_Probes_Migration(final KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
        this.mapper = new ObjectMapper();

        // Set the structure of UI probe categories and licensed according with probe type
        probeCategoryAndLicense.put(SDKProbeType.VCENTER.getProbeType(), Pair.of(ProbeCategory.HYPERVISOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.HYPERV.getProbeType(), Pair.of(ProbeCategory.HYPERVISOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.OPENSTACK.getProbeType(), Pair.of(ProbeCategory.PRIVATE_CLOUD.getCategory(),
                ProbeLicense.CLOUD_TARGETS.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.VMM.getProbeType(), Pair.of(ProbeCategory.PRIVATE_CLOUD.getCategory(),
                ProbeLicense.CLOUD_TARGETS.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.VCD.getProbeType(), Pair.of(ProbeCategory.PRIVATE_CLOUD.getCategory(),
                ProbeLicense.CLOUD_TARGETS.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.AWS.getProbeType(), Pair.of(ProbeCategory.PUBLIC_CLOUD.getCategory(),
                ProbeLicense.PUBLIC_CLOUD.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.AZURE.getProbeType(), Pair.of(ProbeCategory.PUBLIC_CLOUD.getCategory(),
                ProbeLicense.PUBLIC_CLOUD.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.AZURE_EA.getProbeType(), Pair.of(ProbeCategory.PUBLIC_CLOUD.getCategory(),
                ProbeLicense.PUBLIC_CLOUD.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.AZURE_SERVICE_PRINCIPAL.getProbeType(), Pair.of(ProbeCategory.PUBLIC_CLOUD.getCategory(),
                ProbeLicense.CLOUD_TARGETS.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.GCP.getProbeType(), Pair.of(ProbeCategory.PUBLIC_CLOUD.getCategory(),
                ProbeLicense.PUBLIC_CLOUD.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.POWERVM.getProbeType(), Pair.of(ProbeCategory.HYPERVISOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.APIC.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.DOCKER.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.CONTAINER_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.SNMP.getProbeType(), Pair.of(ProbeCategory.GUEST_OS_PROCESSES.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.WMI.getProbeType(), Pair.of(ProbeCategory.GUEST_OS_PROCESSES.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.APPDYNAMICS.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.DYNATRACE.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.DATADOG.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.NEWRELIC.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.APPINSIGHTS.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.TOMCAT.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.WEBSPHERE.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.WEBLOGIC.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.COMPELLENT.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.MSSQL.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.MYSQL.getProbeType(), Pair.of(ProbeCategory.APPLICATIONS_AND_DATABASES.getCategory(),
                ProbeLicense.APP_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.HDS.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.NETAPP.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.SCALEIO.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.UCS.getProbeType(), Pair.of(ProbeCategory.FABRIC_AND_NETWORK.getCategory(),
                ProbeLicense.FABRIC.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.VMAX.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.CLOUD_FOUNDRY.getProbeType(), Pair.of(ProbeCategory.CLOUD_NATIVE.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.PIVOTAL_OPSMAN.getProbeType(), Pair.of(ProbeCategory.CLOUD_NATIVE.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.VC_STORAGE_BROWSE.getProbeType(), Pair.of(ProbeCategory.STORAGE_BROWSING.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.VPLEX.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.PURE.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.HPE_3PAR.getProbeType(), Pair.of(ProbeCategory.STORAGE.getCategory(),
                ProbeLicense.STORAGE.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.RHV.getProbeType(), Pair.of(ProbeCategory.HYPERVISOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.AZURE_STORAGE_BROWSE.getProbeType(), Pair.of(ProbeCategory.STORAGE_BROWSING.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.NETFLOW.getProbeType(), Pair.of(ProbeCategory.FABRIC_AND_NETWORK.getCategory(),
                ProbeLicense.NETWORK_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.TETRATION.getProbeType(), Pair.of(ProbeCategory.FABRIC_AND_NETWORK.getCategory(),
                ProbeLicense.NETWORK_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.ISTIO.getProbeType(), Pair.of(ProbeCategory.FABRIC_AND_NETWORK.getCategory(),
                ProbeLicense.NETWORK_CONTROL.getKey()));
        probeCategoryAndLicense.put(SDKProbeType.UCSD.getProbeType(), Pair.of(ProbeCategory.ORCHESTRATOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.SERVICENOW.getProbeType(), Pair.of(ProbeCategory.ORCHESTRATOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.TERRAFORM.getProbeType(), Pair.of(ProbeCategory.ORCHESTRATOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.ACTIONSCRIPT.getProbeType(), Pair.of(ProbeCategory.ORCHESTRATOR.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.AWS_BILLING.getProbeType(), Pair.of(ProbeCategory.BILLING.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.AWS_LAMBDA.getProbeType(), Pair.of(ProbeCategory.FAAS.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.AWS_COST.getProbeType(), Pair.of(ProbeCategory.COST.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.AZURE_COST.getProbeType(), Pair.of(ProbeCategory.COST.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.GCP_COST.getProbeType(), Pair.of(ProbeCategory.COST.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.VMWARE_HORIZON_VIEW.getProbeType(), Pair.of(ProbeCategory.VIRTUAL_DESKTOP_INFRASTRUCTURE.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.INTERSIGHT.getProbeType(), Pair.of(ProbeCategory.HYPERCONVERGED.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.INTERSIGHT_UCS.getProbeType(), Pair.of(ProbeCategory.HYPERCONVERGED.getCategory(),
                null));
        probeCategoryAndLicense.put(SDKProbeType.INTERSIGHT_HX.getProbeType(), Pair.of(ProbeCategory.HYPERCONVERGED.getCategory(),
                null));
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        // The process of migration:
        // 1. Parse json.
        // 2. Get probeType.
        // 3. If it's a known probe type get Pair of uiProbeCategory and license.
        //    For some probes the license can be null.
        // 4. If it's unknown probe then use probeCategory as uiProbeCategory.
        // 5. Create a new json and update keyValueStore
        logger.debug("Start probes migration: add 'uiProbeCategory' and 'license' properties " +
                "if don't exist");
        String msg = "All probes were updated. Upgrade finished.";
        MigrationStatus status = MigrationStatus.SUCCEEDED;
        final Map<String, String> persistedProbes =
                keyValueStore.getByPrefix(ProbeStore.PROBE_KV_STORE_PREFIX);
        for (Entry<String, String> entry : persistedProbes.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            final String probeId = entry.getKey()
                    .substring(ProbeStore.PROBE_KV_STORE_PREFIX.length());
            try {
                Map<String, Object> probeProperties =
                        mapper.readValue(value, new TypeReference<Map<String, Object>>() {});
                if (probeProperties.get("uiProbeCategory") != null) {
                    continue;
                }
                String probeType = (String)probeProperties.get("probeType");
                Pair<String, String> property = probeCategoryAndLicense.get(probeType);
                String uiCategory;
                String license = null;
                if (property != null) {
                    uiCategory = property.getLeft();
                    license = property.getRight();
                } else {
                    uiCategory = (String)probeProperties.get("probeCategory");
                }
                probeProperties.put("uiProbeCategory", uiCategory);
                if (license != null) {
                    probeProperties.put("license", license);
                }
                String updatedProbe = mapper.writeValueAsString(probeProperties);
                keyValueStore.put(key, updatedProbe);
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse configuration of probe with id " + probeId + ".", e);
                msg = "Problems occurred during probes migration. Check logs for more details.";
                status = MigrationStatus.FAILED;
            }
        }

        logger.info(msg);
        return updateMigrationProgress(status, 100, msg);
    }
}
