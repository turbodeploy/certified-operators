package com.vmturbo.topology.processor.migration;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS;
import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS_COST;
import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS_LAMBDA;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_DEFINITION;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ID;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_ID;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_SPEC;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_TARGET_INFO;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import com.vmturbo.kvstore.KeyValueStore;

/**
 * This class migrates AWS, AWS Cost and AWS Lambda probes and targets, and add accountType to
 * account definitions.
 */
public class V_01_01_08__AWS_Add_AccountType extends AbstractProbeTargetMigration {

    // all AWS probes that need update
    private static final ImmutableSet<String> NON_BILLING_AWS_PROBE_TYPES
            = ImmutableSet.of(AWS.getProbeType(), AWS_COST.getProbeType(), AWS_LAMBDA.getProbeType());
    private static final String MIGRATION_PURPOSE = "Migration to add accountType for AWS non-billing probes and targets";
    protected static final String ACCOUNT_TYPE = "accountType";
    protected static final String ACCOUNT_VALUE_STANDARD = "Standard";
    protected static final String ACCOUNT_VALUE_GOVCLOUD_US = "GovCloud_US";

    // the new accountType field to be added to probe
    private final JsonObject probeAccountTypeField;

    // the new standard accountType field to be added to target
    private final JsonObject targetStandardAccountType;

    /**
     * Create the migration object.
     *
     * @param keyValueStore consul
     */
    public V_01_01_08__AWS_Add_AccountType(@Nonnull KeyValueStore keyValueStore) {
        super(keyValueStore);
        this.probeAccountTypeField = createProbeAccountTypeField();
        this.targetStandardAccountType = createTargetStandardAccountType();
    }

    @Override
    String migrationPurpose() {
        return MIGRATION_PURPOSE;
    }

    @Override
    boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson) {
        if (!NON_BILLING_AWS_PROBE_TYPES.contains(probeType)) {
            logger.trace("probeId={} probeType={} not AWS non-billing probe", probeId, probeType);
            return false;
        }
        if (!probeJson.has(FIELD_ACCOUNT_DEFINITION)) {
            logger.error("probeId={} probeType={} Can't find accountDefinition", probeId, probeType);
            return false;
        }
        return true;
    }

    @Override
    void updateProbe(String probeId, String probeType, JsonObject probeJson, String consulProbeKey) {
        JsonArray accountDefs = probeJson.get(FIELD_ACCOUNT_DEFINITION).getAsJsonArray();
        for (int i = 0; i < accountDefs.size(); i++) {
            final JsonObject accountDef = accountDefs.get(i).getAsJsonObject();
            final JsonElement customDef = accountDef.get("customDefinition");
            if (customDef == null) {
                continue;
            }
            final JsonElement key = customDef.getAsJsonObject().get("name");
            if (key == null) {
                continue;
            }
            if (ACCOUNT_TYPE.equals(key.getAsString())) {
                logger.info("probeId={} probeType={} accountType field already exists.", probeId, probeType);
                return;
            }
        }
        // Add the new account type definition to probe account definition.
        accountDefs.add(probeAccountTypeField);
        logger.info("probeType={} add def={}", probeType, probeAccountTypeField);
        // update consul
        keyValueStore.put(consulProbeKey, probeJson.toString());
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
            logger.trace("targetId={} probeId=\"{}\" not processed AWS probe type", targetId, probeId);
            return;
        }

        if (!spec.has(FIELD_ACCOUNT_VALUE)) {
            logger.trace("targetId={} probeId=\"{}\" spec doesn't have accountValue list", targetId, probeId);
            return;
        }
        JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        for (int i = 0; i < accountValues.size(); i++) {
            final JsonObject accountVal = accountValues.get(i).getAsJsonObject();
            final JsonElement key = accountVal.get("key");
            if (key == null) {
                continue;
            }
            if (ACCOUNT_TYPE.equals(key.getAsString())) {
                logger.info("targetId={} probeId=\"{}\" accountType field already exists.", targetId, probeId);
                return;
            }
        }

        logger.info("targetId={} probeType='{}' Starting target migration", targetId, probeType);
        accountValues.add(targetStandardAccountType);

        // update consul
        final JsonObject targetInfoWrapper = new JsonObject();
        targetInfoWrapper.add(FIELD_TARGET_INFO, new JsonPrimitive(targetInfoObject.toString()));
        logger.trace("targetId={} probeType='{}' targetInfo={}", targetId, probeType, targetInfoWrapper.toString());
        keyValueStore.put(consulTargetKey, secretFields.toString() + targetInfoWrapper.toString());
    }

    private JsonObject createProbeAccountTypeField() {
        final JsonObject customDef = new JsonObject();
        customDef.addProperty("name", ACCOUNT_TYPE);
        customDef.addProperty("displayName", "Account Type");
        customDef.addProperty("description", "AWS Account type, Standard or GovCloud (US).");
        customDef.addProperty("verificationRegex", ".*");
        customDef.addProperty("isSecret", false);
        customDef.addProperty("primitiveValue", "STRING");
        customDef.addProperty("isMultiline", false);

        final JsonArray allowedVals = new JsonArray();
        allowedVals.add(ACCOUNT_VALUE_STANDARD);
        allowedVals.add(ACCOUNT_VALUE_GOVCLOUD_US);

        final JsonObject probeField = new JsonObject();
        probeField.add("customDefinition", customDef);
        probeField.addProperty("mandatory", true);
        probeField.addProperty("isTargetDisplayName", false);
        probeField.add("allowedValues", allowedVals);
        return probeField;
    }

    private JsonObject createTargetStandardAccountType() {
        final JsonObject targetField = new JsonObject();
        targetField.addProperty("key", ACCOUNT_TYPE);
        targetField.addProperty("stringValue", ACCOUNT_VALUE_STANDARD);
        return targetField;
    }
}
