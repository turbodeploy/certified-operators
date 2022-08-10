package com.vmturbo.topology.processor.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;

import javax.annotation.Nonnull;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountTypeTest.FakeKeyValueStore;

/**
 * Test the migration of resource level dataset and table name fields and the resource level
 * discovery enabled flag within the GCP billing probe.
 */
public class V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag_Test {

    private static final String TARGET_ID = "12345";
    private static final String STANDARD_TABLE_NAME = "standard_table_123";
    private static final String STANDARD_DATASET_NAME = "standard_dataset_123";
    private static final String RESOURCE_TABLE_NAME = "resource_table_123";
    private static final String RESOURCE_DATASET_NAME = "resource_dataset_123";
    private static final JsonObject SECRET_FIELDS = new JsonObject();

    /**
     * Test that the correct probe is migrated.
     */
    @Test
    public void testShouldUpdateProbe() {
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(
                        new FakeKeyValueStore(Collections.emptyMap()));
        assertTrue(migration.shouldUpdateProbe("", SDKProbeType.GCP_BILLING.toString(), null));
        assertFalse(migration.shouldUpdateProbe("", "", null));
    }

    /**
     * Test that if no resource level dataset or table name is present, the resource level
     * discovery flag is false.
     */
    @Ignore
    @Test
    public void testMigrateStandardTarget() {
        final FakeKeyValueStore keyValueStore = new FakeKeyValueStore(new HashMap<>());
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        final String targetWithResourceFlagDisabled = makeTargetString(STANDARD_DATASET_NAME,
                STANDARD_TABLE_NAME,
                ",{\"key\":\"resourceLevelCostEnabled\",\"stringValue\":\"false\"}");

        final JsonObject targetWithNoResourceLevelFields = makeTargetJsonObject(
                makeTargetString(STANDARD_DATASET_NAME, STANDARD_TABLE_NAME, ""));
        migration.processIndividualTarget(SECRET_FIELDS, targetWithNoResourceLevelFields, TARGET_ID);
        assertEquals(targetWithResourceFlagDisabled, unwrapTargetInfo(keyValueStore));

        final JsonObject targetWithEmptyResourceLevelFields = makeTargetJsonObject(
                makeTargetString(STANDARD_DATASET_NAME, STANDARD_TABLE_NAME,
                        ",{\"key\":\"resourceLevelCostDataSetName\",\"stringValue\":\"\"},{\"key\":\"resourceLevelCostTableName\",\"stringValue\":\"\"}"));
        migration.processIndividualTarget(SECRET_FIELDS, targetWithEmptyResourceLevelFields, TARGET_ID);
        assertTrue(keyValueStore.get(TARGET_ID).isPresent());
        assertEquals(targetWithResourceFlagDisabled, unwrapTargetInfo(keyValueStore));

        final JsonObject targetWithOneResourceLevelField = makeTargetJsonObject(
                makeTargetString(STANDARD_DATASET_NAME, STANDARD_TABLE_NAME,
                        ",{\"key\":\"resourceLevelCostDataSetName\",\"stringValue\":\"resource_dataset_123\"}"));
        migration.processIndividualTarget(SECRET_FIELDS, targetWithOneResourceLevelField, TARGET_ID);
        assertTrue(keyValueStore.get(TARGET_ID).isPresent());
        assertEquals(targetWithResourceFlagDisabled, unwrapTargetInfo(keyValueStore));

        final JsonObject targetWithOneEmptyResourceLevelField = makeTargetJsonObject(
                makeTargetString(STANDARD_DATASET_NAME, STANDARD_TABLE_NAME,
                        ",{\"key\":\"resourceLevelCostDataSetName\",\"stringValue\":\"\"},{\"key\":\"resourceLevelCostTableName\",\"stringValue\":\"resource_table_123\"}"));
        migration.processIndividualTarget(SECRET_FIELDS, targetWithOneEmptyResourceLevelField, TARGET_ID);
        assertTrue(keyValueStore.get(TARGET_ID).isPresent());
        assertEquals(targetWithResourceFlagDisabled, unwrapTargetInfo(keyValueStore));
    }



    /**
     * Test that if resource level dataset and table name are present, these are migrated properly
     * and the resource level discovery flag is enabled.
     */
    @Ignore
    @Test
    public void testMigrateResourceLevelTarget() {
        final FakeKeyValueStore keyValueStore = new FakeKeyValueStore(new HashMap<>());
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        final String migratedTarget = makeTargetString(RESOURCE_DATASET_NAME, RESOURCE_TABLE_NAME,
                ",{\"key\":\"resourceLevelCostEnabled\",\"stringValue\":\"true\"}");
        final JsonObject targetWithOnlyResourceFields = makeTargetJsonObject("{\"id\":\""
                + TARGET_ID + "\",\"spec\":{\"accountValue\":[{\"key\":\"resourceLevelCostDataSetName\",\"stringValue\":\""
                + RESOURCE_DATASET_NAME + "\"},{\"key\":\"resourceLevelCostTableName\",\"stringValue\":\""
                + RESOURCE_TABLE_NAME + "\"}]}}");
        migration.processIndividualTarget(SECRET_FIELDS, targetWithOnlyResourceFields, TARGET_ID);
        assertTrue(keyValueStore.get(TARGET_ID).isPresent());
        assertEquals(migratedTarget, unwrapTargetInfo(keyValueStore));
    }

    /**
     * Test that if both standard and resource level dataset/table names are present,
     * the resource level info is used and the resource level discovery flag is enabled.
     */
    @Ignore
    @Test
    public void testMigrateStandardAndResourceLevelTarget() {
        final FakeKeyValueStore keyValueStore = new FakeKeyValueStore(new HashMap<>());
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        final String migratedTarget = makeTargetString(RESOURCE_DATASET_NAME, RESOURCE_TABLE_NAME,
                ",{\"key\":\"resourceLevelCostEnabled\",\"stringValue\":\"true\"}");

        final JsonObject targetWithResourceAndStandardFields = makeTargetJsonObject(
                makeTargetString(STANDARD_DATASET_NAME, STANDARD_TABLE_NAME,
                        ",{\"key\":\"resourceLevelCostDataSetName\",\"stringValue\":\""
                                + RESOURCE_DATASET_NAME
                                + "\"},{\"key\":\"resourceLevelCostTableName\",\"stringValue\":\""
                                + RESOURCE_TABLE_NAME + "\"}"));
        migration.processIndividualTarget(SECRET_FIELDS, targetWithResourceAndStandardFields, TARGET_ID);
        assertEquals(migratedTarget, unwrapTargetInfo(keyValueStore));
    }

    private String makeTargetString(@Nonnull final String dataset, @Nonnull final String table,
            @Nonnull final String otherAttributes) {
        return "{\"id\":\"" + TARGET_ID + "\",\"spec\":{\"accountValue\":[{\"key\":\"costExportDataSetName\",\"stringValue\":\""
                + dataset + "\"},{\"key\":\"costExportTableName\",\"stringValue\":\"" + table
                + "\"}" + otherAttributes + "]}}";
    }

    private JsonObject makeTargetJsonObject(@Nonnull final String targetString) {
        return JsonParser.parseString(targetString).getAsJsonObject();
    }

    private String unwrapTargetInfo(@Nonnull final FakeKeyValueStore kvstore) {
        assertTrue(kvstore.get(TARGET_ID).isPresent());
        final String wrapped = kvstore.get(TARGET_ID).get();
        // value within kvstore takes the following form:
        // {secrets}{"targetInfo": "{target info as an escaped string}"}
        final String unwrapped = wrapped.replace("{}{\"targetInfo\":\"", "").replace("}\"}", "}");
        return unwrapped.replace("\\\"", "\"");
    }
}
