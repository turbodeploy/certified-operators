package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_ACCOUNT_VALUE;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_ID;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_PROBE_ID;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_SPEC;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_TARGET_INFO;

import javax.annotation.Nonnull;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Fix targets so that the resource level cost flag is removed from targets that shouldn't have it.
 */
public class V_01_01_20__Remove_NonGCP_Resource_Level_Flag extends AbstractProbeTargetMigration {

    private static final String RESOURCE_LEVEL_DISCOVERY_FLAG = "resourceLevelCostEnabled";
    private static final String PROPERTY_KEY = "key";

    /**
     * Construct the migration.
     *
     * @param keyValueStore key value store to persist information
     */
    V_01_01_20__Remove_NonGCP_Resource_Level_Flag(@Nonnull final KeyValueStore keyValueStore) {
        super(keyValueStore);
    }

    @Override
    String migrationPurpose() {
        return " Fix targets so that the resource level cost flag is removed from targets that shouldn't have it.";
    }

    @Override
    boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson) {
        return true;
    }

    @Override
    void updateProbe(String probeId, String probeType, JsonObject probeJson, String consulProbeKey) {
    }

    @Override
    void processIndividualTarget(JsonObject secretFields, JsonObject targetInfoObject, String consulTargetKey) {
        final String targetId = targetInfoObject.get(FIELD_ID).getAsString();
        if (!targetInfoObject.has(FIELD_SPEC)) {
            logger.error("target {} doesn't have spec field", targetId);
            return;
        }
        final JsonObject spec = targetInfoObject.get(FIELD_SPEC).getAsJsonObject();
        final String probeId = spec.get(FIELD_PROBE_ID).getAsString();
        final String probeType = probeIdToProbeTypeMap.get(probeId);
        if (SDKProbeType.GCP_BILLING.getProbeType().equals(probeType)) {
            return;
        }
        if (!spec.has(FIELD_ACCOUNT_VALUE)) {
            logger.warn("target {} spec doesn't have accountValue list", targetId);
            return;
        }
        final JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        for (int i = 0; i < accountValues.size(); i++) {
            final JsonObject accountValue = accountValues.get(i).getAsJsonObject();
            final JsonElement key = accountValue.get(PROPERTY_KEY);
            if (RESOURCE_LEVEL_DISCOVERY_FLAG.equals(key.getAsString())) {
                accountValues.remove(i);
            }
        }
        // update consul
        final JsonObject targetInfoWrapper = new JsonObject();
        targetInfoWrapper.add(FIELD_TARGET_INFO, new JsonPrimitive(targetInfoObject.toString()));
        keyValueStore.put(consulTargetKey, secretFields.toString() + targetInfoWrapper.toString());
    }
}
