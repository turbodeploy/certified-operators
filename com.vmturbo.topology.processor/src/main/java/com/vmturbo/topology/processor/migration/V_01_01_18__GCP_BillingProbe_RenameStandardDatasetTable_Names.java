package com.vmturbo.topology.processor.migration;

import javax.annotation.Nonnull;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Migrates the older targets with the fields standardCostDataSetName to costDataSetName
 * and standardCostTableName to costTableName.
 */
public class V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names extends AbstractProbeTargetMigration {

    static final String FIELD_ACCOUNT_VALUE = "accountValue";
    static final String FIELD_SPEC = "spec";
    static final String FIELD_TARGET_INFO = "targetInfo";
    static final String FIELD_ID = "id";
    static final String FIELD_PROBE_ID = "probeId";

    private static final String GCP_BILLING_PROBE_TYPE = SDKProbeType.GCP_BILLING.toString();
    private static final String STANDARD_COST_DATASET_NAME = "standardCostDataSetName";
    private static final String STANDARD_COST_TABLE_NAME = "standardCostTableName";
    private static final String COST_DATA_SET_NAME = "costExportDataSetName";
    private static final String COST_DATA_TABLE_NAME = "costExportTableName";

    private final Logger logger = LogManager.getLogger(getClass());


    /**
     * Create the migration object.
     *
     * @param keyValueStore consul
     */
    protected V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names(@Nonnull KeyValueStore keyValueStore) {
        super(keyValueStore);
    }

    @Override
    String migrationPurpose() {
        return "Switch the GCP Billing probe Standard Cost dataset and table names and"
                + " migrate their values to the new fields \"CostExportDastaSet\" and \"CostExport Table Name\"";
    }

    @Override
    boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson) {
        if (!GCP_BILLING_PROBE_TYPE.equals(probeType)) {
            logger.trace("probeId={} probeType={} not AWS non-billing probe", probeId, probeType);
            return false;
        }
        return true;
    }

    @Override
    void updateProbe(String probeId, String probeType, JsonObject probeJson,
            String consulProbeKey) {
        // do nothing
    }

    @Override
    void processIndividualTarget(JsonObject secretFields, JsonObject targetInfoObject,
            String consulTargetKey) {
            String targetId = targetInfoObject.get(FIELD_ID).getAsString();
            if (!targetInfoObject.has(FIELD_SPEC)) {
                logger.error("targetId={} doesn't have spec field", targetId);
                return;
            }
            JsonObject spec = targetInfoObject.get(FIELD_SPEC).getAsJsonObject();
            String probeId = spec.get(FIELD_PROBE_ID).getAsString();

            String probeType = probeIdToProbeTypeMap.get(probeId);
            if (probeType == null) {
                logger.debug("Skipping target with targetId={} probeId=\"{}\"", targetId,
                        probeId);
                return;
            }

            if (!spec.has(FIELD_ACCOUNT_VALUE)) {
                logger.debug("targetId={} probeId=\"{}\" spec doesn't have accountValue list",
                        targetId, probeId);
                return;
            }
            JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
            for (int i = 0; i < accountValues.size(); i++) {
                final JsonObject accountVal = accountValues.get(i).getAsJsonObject();
                final JsonElement key = accountVal.get("key");
                if (key == null) {
                    continue;
                }
                if (COST_DATA_SET_NAME.equals(key.getAsString())
                        || COST_DATA_TABLE_NAME.equals(key.getAsString())) {
                    logger.info("targetId={} probeId=\"{}\" field=\"{}\" already exists.",
                            targetId, probeId, key);
                }
                if (STANDARD_COST_DATASET_NAME.equals(key.getAsString())) {
                        accountVal.remove("key");
                        accountVal.addProperty("key", COST_DATA_SET_NAME);
                } else if (STANDARD_COST_TABLE_NAME.equals(key.getAsString())) {
                        accountVal.remove("key");
                        accountVal.addProperty("key", COST_DATA_TABLE_NAME);
                }
            }
            // update consul
            final JsonObject targetInfoWrapper = new JsonObject();
            targetInfoWrapper.add(FIELD_TARGET_INFO, new JsonPrimitive(targetInfoObject.toString()));
            logger.info("Updated targetId={} probeType='{}", targetId, probeType);
            keyValueStore.put(consulTargetKey, secretFields.toString() + targetInfoWrapper.toString());
        }
}
