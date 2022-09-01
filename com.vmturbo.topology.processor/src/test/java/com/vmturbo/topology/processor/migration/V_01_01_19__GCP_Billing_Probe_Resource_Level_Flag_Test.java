package com.vmturbo.topology.processor.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.function.TriFunction;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountTypeTest.FakeKeyValueStore;

/**
 * Test the migration of resource level dataset and table name fields and the resource level
 * discovery enabled flag within the GCP billing probe.
 */
public class V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag_Test {

    private static final String TARGET_ID = "12345";
    private static final String TARGET_CONSUL_KEY = "targets/" + TARGET_ID;
    private static final String PROBE_ID = "98765";
    private static final String STANDARD_TABLE_NAME = "standard_table_123";
    private static final String STANDARD_DATASET_NAME = "standard_dataset_123";
    private static final String RESOURCE_TABLE_NAME = "resource_table_123";
    private static final String RESOURCE_DATASET_NAME = "resource_dataset_123";
    // value within kvstore takes the following form:
    // {secrets}{"targetInfo": "target info object as an escaped string"}
    private static final TriFunction<String, String, String, String> TARGET_INFO_STRING = (probeId, targetId, accountVals) ->
            "{}{\"targetInfo\":\"{\\\"id\\\":\\\"" + targetId + "\\\",\\\"spec\\\":{\\\"probeId\\\":\\\"" + probeId + "\\\",\\\"accountValue\\\":[" + accountVals + "]}}\"}";
    private static final UnaryOperator<String> RESOURCE_TABLE_ACCT_VAL = (table) ->
            "{\\\"key\\\":\\\"resourceLevelCostTableName\\\",\\\"stringValue\\\":\\\"" + table + "\\\"}";
    private static final UnaryOperator<String> COST_TABLE_ACCT_VAL = (table) ->
            "{\\\"key\\\":\\\"costExportTableName\\\",\\\"stringValue\\\":\\\"" + table + "\\\"}";
    private static final UnaryOperator<String> COST_DATASET_ACCT_VAL = (dataset) ->
            "{\\\"key\\\":\\\"costExportDataSetName\\\",\\\"stringValue\\\":\\\"" + dataset + "\\\"}";
    private static final UnaryOperator<String> RESOURCE_DATASET_ACCT_VAL = (dataset) ->
            "{\\\"key\\\":\\\"resourceLevelCostDataSetName\\\",\\\"stringValue\\\":\\\"" + dataset + "\\\"}";
    private static final UnaryOperator<String> RESOURCE_FLAG_ACCT_VAL = (flag) ->
            "{\\\"key\\\":\\\"resourceLevelCostEnabled\\\",\\\"stringValue\\\":\\\"" + flag + "\\\"}";
    private static final String RESOURCE_TABLE_NAME_FIELD = "resourceLevelCostTableName";
    private static final String RESOURCE_DATA_SET_FIELD = "resourceLevelCostDataSetName";

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
    @Test
    public void testMigrateStandardTarget() {
        final FakeKeyValueStore keyValueStore = setUpFakeKeyValueStore(makeStdTargetString(""));
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        migration.doStartMigration();
        checkAccountValues(keyValueStore, "false", STANDARD_DATASET_NAME, STANDARD_TABLE_NAME);

        keyValueStore.put(TARGET_CONSUL_KEY, makeStdTargetString(
                "," + RESOURCE_DATASET_ACCT_VAL.apply("") + "," + RESOURCE_TABLE_ACCT_VAL.apply("")));
        migration.doStartMigration();
        checkAccountValues(keyValueStore, "false", STANDARD_DATASET_NAME, STANDARD_TABLE_NAME);

        keyValueStore.put(TARGET_CONSUL_KEY, makeStdTargetString(
                "," + RESOURCE_DATASET_ACCT_VAL.apply(RESOURCE_DATASET_NAME)));
        migration.doStartMigration();
        checkAccountValues(keyValueStore, "false", STANDARD_DATASET_NAME, STANDARD_TABLE_NAME);

        keyValueStore.put(TARGET_CONSUL_KEY, makeStdTargetString(","
                + RESOURCE_DATASET_ACCT_VAL.apply("") + ","
                        + RESOURCE_TABLE_ACCT_VAL.apply(RESOURCE_TABLE_NAME)));
        migration.doStartMigration();
        checkAccountValues(keyValueStore, "false", STANDARD_DATASET_NAME, STANDARD_TABLE_NAME);
    }

    /**
     * Test that if resource level dataset and table name are present, these are migrated properly
     * and the resource level discovery flag is enabled.
     */
    @Test
    public void testMigrateResourceLevelTarget() {
        final FakeKeyValueStore keyValueStore = setUpFakeKeyValueStore(
                TARGET_INFO_STRING.apply(PROBE_ID, TARGET_ID,
                        RESOURCE_TABLE_ACCT_VAL.apply(RESOURCE_TABLE_NAME) + ","
                                + RESOURCE_DATASET_ACCT_VAL.apply(RESOURCE_DATASET_NAME)));
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        migration.doStartMigration();
        checkAccountValues(keyValueStore, "true", RESOURCE_DATASET_NAME, RESOURCE_TABLE_NAME);
    }

    /**
     * Test that if both standard and resource level dataset/table names are present,
     * the resource level info is used and the resource level discovery flag is enabled.
     */
    @Test
    public void testMigrateStandardAndResourceLevelTarget() {
        final FakeKeyValueStore keyValueStore = setUpFakeKeyValueStore(makeStdTargetString(
                "," + RESOURCE_DATASET_ACCT_VAL.apply(RESOURCE_DATASET_NAME) + ","
                        + RESOURCE_TABLE_ACCT_VAL.apply(RESOURCE_TABLE_NAME)));
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        migration.doStartMigration();
        checkAccountValues(keyValueStore, "true", RESOURCE_DATASET_NAME, RESOURCE_TABLE_NAME);
    }

    /**
     * Test that during this migration, GCP billing targets are migrated as appropriate and all
     * other targets are untouched.
     */
    @Test
    public void testOnlyGcpBillingTargetsModified() {
        final FakeKeyValueStore keyValueStore =
                setUpFakeKeyValueStore(TARGET_INFO_STRING.apply(PROBE_ID, TARGET_ID, ""));
        keyValueStore.put("probes/00000", "{\"probeType\": \"something\"}");
        keyValueStore.put("targets/111111", TARGET_INFO_STRING.apply("00000", "111111", ""));
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        migration.doStartMigration();
        assertTrue(keyValueStore.get("targets/111111").isPresent());
        assertFalse(keyValueStore.get("targets/111111").get().contains("\\\"resourceLevelCostEnabled\\\""));
        assertTrue(keyValueStore.get(TARGET_CONSUL_KEY).isPresent());
        assertTrue(keyValueStore.get(TARGET_CONSUL_KEY).get().contains("\\\"resourceLevelCostEnabled\\\""));
    }

    /**
     * Test that if a GCP billing target has a resource flag account value already, no changes are made.
     */
    @Test
    public void testTargetsOnlyMigratedOnce() {
        final String upToDateTarget = TARGET_INFO_STRING.apply(PROBE_ID, TARGET_ID,
                RESOURCE_FLAG_ACCT_VAL.apply("true"));
        final FakeKeyValueStore keyValueStore = setUpFakeKeyValueStore(upToDateTarget);
        final V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag migration =
                new V_01_01_19__GCP_Billing_Probe_Resource_Level_Flag(keyValueStore);
        migration.doStartMigration();
        assertTrue(keyValueStore.get(TARGET_CONSUL_KEY).isPresent());
        assertEquals(upToDateTarget, keyValueStore.get(TARGET_CONSUL_KEY).get());
    }

    private static void checkAccountValues(@Nonnull final FakeKeyValueStore keyValueStore,
            @Nonnull final String resourceFlag, @Nonnull final String dataset,
            @Nonnull final String table) {
        final Optional<String> result = keyValueStore.get(TARGET_CONSUL_KEY);
        assertTrue(result.isPresent());
        assertTrue(result.get().contains(RESOURCE_FLAG_ACCT_VAL.apply(resourceFlag)));
        assertTrue(result.get().contains(COST_DATASET_ACCT_VAL.apply(dataset)));
        assertTrue(result.get().contains(COST_TABLE_ACCT_VAL.apply(table)));
        assertFalse(result.get().contains(RESOURCE_DATA_SET_FIELD));
        assertFalse(result.get().contains(RESOURCE_TABLE_NAME_FIELD));
    }

    private static FakeKeyValueStore setUpFakeKeyValueStore(@Nullable final String target) {
        final FakeKeyValueStore keyValueStore = new FakeKeyValueStore(new HashMap<>());
        keyValueStore.put("probes/" + PROBE_ID, "{\"probeType\": \"GCP Billing\"}");
        if (target != null) {
            keyValueStore.put(TARGET_CONSUL_KEY, target);
        }
        return keyValueStore;
    }

    private String makeStdTargetString(@Nonnull final String otherAttributes) {
        return TARGET_INFO_STRING.apply(PROBE_ID, TARGET_ID,
                COST_DATASET_ACCT_VAL.apply(STANDARD_DATASET_NAME) + ","
                        + COST_TABLE_ACCT_VAL.apply(STANDARD_TABLE_NAME) + otherAttributes);
    }
}
