package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ID;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_ID;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_SPEC;

import javax.annotation.Nonnull;

import com.google.gson.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Migration to remove the GCP cost probe and associated targets to allow for re-registration with
 * updated identity metadata.
 */
public class V_01_01_14__GCP_Remove_Cost_Probe_Targets extends AbstractProbeTargetMigration {

    private final Logger logger = LogManager.getLogger();

    private static final String MIGRATION_PURPOSE =
        "Migration to remove GCP Cost probes and targets to allow identity metadata migration";

    /**
     * Creates the migration instance.
     *
     * @param keyValueStore The KV store.
     */
    public V_01_01_14__GCP_Remove_Cost_Probe_Targets(@Nonnull KeyValueStore keyValueStore) {
        super(keyValueStore);
    }

    @Override
    String migrationPurpose() {
        return MIGRATION_PURPOSE;
    }

    @Override
    boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson) {
        return probeType.equalsIgnoreCase(SDKProbeType.GCP_COST.getProbeType());
    }

    @Override
    void updateProbe(String probeId, String probeType, JsonObject probeJson, String consulProbeKey) {

        logger.info("Removing GCP Cost probe key '{}' from the KV store", consulProbeKey);
        keyValueStore.removeKey(consulProbeKey);
    }

    @Override
    void processIndividualTarget(JsonObject secretFields, JsonObject targetInfoObject, String consulTargetKey) {

        String targetId = targetInfoObject.get(FIELD_ID).getAsString();
        if (!targetInfoObject.has(FIELD_SPEC)) {
            logger.info("targetId={} doesn't have spec field", targetId);
            return;
        }
        JsonObject spec = targetInfoObject.get(FIELD_SPEC).getAsJsonObject();
        String probeId = spec.get(FIELD_PROBE_ID).getAsString();

        String probeType = probeIdToProbeTypeMap.get(probeId);
        if (probeType == null) {
            logger.info("Ignoring targetId={} probeId=\"{}\" with unrecognized probe type", targetId, probeId);
            return;
        }

        if (probeType.equalsIgnoreCase(SDKProbeType.GCP_COST.getProbeType())) {
            logger.info("Removing targetId={} probeType={} from KV store", targetId, probeType);
            keyValueStore.removeKey(consulTargetKey);
        }
    }
}
