package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_ACCOUNT_VALUE;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_ID;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_PROBE_ID;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_SPEC;
import static com.vmturbo.topology.processor.migration.V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names.FIELD_TARGET_INFO;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Migrate the GCP Billing probe targets so that the cost export dataset and table names are
 * properly set and the resource level discovery flag is enabled or disabled accordingly.
 */
public class V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag extends AbstractProbeTargetMigration {

    private static final String RESOURCE_LEVEL_COST_DATASET_NAME = "resourceLevelCostDataSetName";
    private static final String RESOURCE_LEVEL_COST_TABLE_NAME = "resourceLevelCostTableName";
    private static final String RESOURCE_LEVEL_DISCOVERY_FLAG = "resourceLevelCostEnabled";
    private static final String COST_DATA_SET_NAME = "costExportDataSetName";
    private static final String COST_DATA_TABLE_NAME = "costExportTableName";
    private static final String PROPERTY_VALUE = "stringValue";
    private static final String PROPERTY_KEY = "key";

    /**
     * Construct the migration.
     *
     * @param keyValueStore key value store to persist information
     */
    V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(@Nonnull final KeyValueStore keyValueStore) {
        super(keyValueStore);
    }

    @Override
    String migrationPurpose() {
        return "Set or unset the GCP Billing Probe's resource level discovery flag based on "
                + "whether Standard or Resource Level dataset and table names are present.";
    }

    @Override
    boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson) {
        return SDKProbeType.GCP_BILLING.toString().equals(probeType);
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
        if (!SDKProbeType.GCP_BILLING.getProbeType().equals(probeType)) {
            return;
        }
        if (!spec.has(FIELD_ACCOUNT_VALUE)) {
            logger.warn("target {} spec doesn't have accountValue list", targetId);
            return;
        }
        final JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        Optional<Integer> resourceLevelDatasetIndex = Optional.empty();
        Optional<Integer> resourceLevelTableIndex = Optional.empty();
        Optional<Integer> costDatasetIndex = Optional.empty();
        Optional<Integer> costTableIndex = Optional.empty();
        Optional<Integer> flagIndex = Optional.empty();
        boolean fieldsArePopulated = true;
        for (int i = 0; i < accountValues.size(); i++) {
            final JsonObject accountValue = accountValues.get(i).getAsJsonObject();
            final JsonElement key = accountValue.get(PROPERTY_KEY);
            final JsonElement value = accountValue.get(PROPERTY_VALUE);
            if (RESOURCE_LEVEL_COST_DATASET_NAME.equals(key.getAsString())) {
                resourceLevelDatasetIndex = Optional.of(i);
                fieldsArePopulated = fieldsArePopulated && !Strings.isNullOrEmpty(value.getAsString());
            } else if (RESOURCE_LEVEL_COST_TABLE_NAME.equals(key.getAsString())) {
                resourceLevelTableIndex = Optional.of(i);
                fieldsArePopulated = fieldsArePopulated && !Strings.isNullOrEmpty(value.getAsString());
            } else if (COST_DATA_SET_NAME.equals(key.getAsString())) {
                costDatasetIndex = Optional.of(i);
            } else if (COST_DATA_TABLE_NAME.equals(key.getAsString())) {
                costTableIndex = Optional.of(i);
            } else if (RESOURCE_LEVEL_DISCOVERY_FLAG.equals(key.getAsString())) {
                flagIndex = Optional.of(i);
            }
        }
        final JsonObject flagAcctVal;
        if (!flagIndex.isPresent()) {
            flagAcctVal = new JsonObject();
            flagAcctVal.addProperty(PROPERTY_KEY, RESOURCE_LEVEL_DISCOVERY_FLAG);
            accountValues.add(flagAcctVal);
        } else {
            flagAcctVal = accountValues.get(flagIndex.get()).getAsJsonObject();
        }
        if (resourceLevelDatasetIndex.isPresent() && resourceLevelTableIndex.isPresent() && fieldsArePopulated) {
            flagAcctVal.addProperty(PROPERTY_VALUE, "true");
            migrateAccountValue(accountValues, resourceLevelDatasetIndex.get(), costDatasetIndex, COST_DATA_SET_NAME);
            migrateAccountValue(accountValues, resourceLevelTableIndex.get(), costTableIndex, COST_DATA_TABLE_NAME);
        } else if (!flagIndex.isPresent()) {
            flagAcctVal.addProperty(PROPERTY_VALUE, "false");
        }
        if (resourceLevelDatasetIndex.isPresent() && resourceLevelTableIndex.isPresent()) {
            accountValues.remove(Math.max(resourceLevelDatasetIndex.get(), resourceLevelTableIndex.get()));
            accountValues.remove(Math.min(resourceLevelDatasetIndex.get(), resourceLevelTableIndex.get()));
        } else {
            resourceLevelDatasetIndex.ifPresent(accountValues::remove);
            resourceLevelTableIndex.ifPresent(accountValues::remove);
        }

        // update consul
        final JsonObject targetInfoWrapper = new JsonObject();
        targetInfoWrapper.add(FIELD_TARGET_INFO, new JsonPrimitive(targetInfoObject.toString()));
        keyValueStore.put(consulTargetKey, secretFields.toString() + targetInfoWrapper.toString());
    }

    private void migrateAccountValue(@Nonnull final JsonArray accountValues, final int newValueIndex,
            @Nonnull final Optional<Integer> existingValueIndex, @Nonnull final String propertyKey) {
        final JsonObject datasetAcctVal;
        if (existingValueIndex.isPresent()) {
            datasetAcctVal = accountValues.get(existingValueIndex.get()).getAsJsonObject();
            logger.debug("Existing account value: {}", datasetAcctVal::toString);
        } else {
            datasetAcctVal = new JsonObject();
            accountValues.add(datasetAcctVal);
        }
        datasetAcctVal.addProperty(PROPERTY_KEY, propertyKey);
        datasetAcctVal.addProperty(PROPERTY_VALUE, getPropertyValueAtIndex(accountValues, newValueIndex));
        logger.debug("New account value: {}", datasetAcctVal::toString);
    }

    private String getPropertyValueAtIndex(@Nonnull final JsonArray accountValues, final int index) {
        return accountValues.get(index).getAsJsonObject().get(PROPERTY_VALUE).getAsString();
    }
}
