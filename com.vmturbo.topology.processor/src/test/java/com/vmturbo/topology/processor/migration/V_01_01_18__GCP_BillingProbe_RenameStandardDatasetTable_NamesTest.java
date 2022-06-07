package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_SPEC;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_TARGET_INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.io.Files;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountTypeTest.FakeKeyValueStore;

/**
 * Tests class to test migration of the standard dataset and table name fields in
 * the GCP billing probe.
 */
public class V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_NamesTest {

    private final Logger logger = LogManager.getLogger();
    private static final String STANDARD_COST_DATASET_NAME = "standardCostDataSetName";
    private static final String STANDARD_COST_TABLE_NAME = "standardCostTableName";
    private static final String COST_DATA_SET_NAME = "costExportDataSetName";
    private static final String COST_DATA_TABLE_NAME = "costExportTableName";

    private static final String PROBE_1_ID = "probes/73548994148144";

    private static final String TARGET_1_ID = "targets/73732399126048";

    private static final String GCP_BILLING_TARGET =
            "{\"secretFields\":[\"sk\",\"sk1\"]}{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"74246331426608\\\",\\n  \\\"spec\\\": {\\n    \\\"probeId\\\": \\\"73548994148144\\\",\\n    \\\"accountValue\\\": [{\\n      \\\"key\\\": \\\"serviceAccountKey\\\",\\n      \\\"stringValue\\\": \\\"sakey\\\"\\n    }, {\\n      \\\"key\\\": \\\"address\\\",\\n      \\\"stringValue\\\": \\\"GCP ENG Billing\\\"\\n    }, {\\n      \\\"key\\\": \\\"secureProxy\\\",\\n      \\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyUsername\\\",\\n      \\\"stringValue\\\": \\\"turbo\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyPassword\\\",\\n      \\\"stringValue\\\": \\\"b\\\"\\n    }, {\\n      \\\"key\\\": \\\"standardCostDataSetName\\\",\\n      \\\"stringValue\\\": \\\"turbo_eng_billing_export\\\"\\n    }, {\\n      \\\"key\\\": \\\"billingAccountId\\\",\\n      \\\"stringValue\\\": \\\"01AAB5-DE376A-EBB741\\\"\\n    }, {\\n      \\\"key\\\": \\\"pricingExportDataSetName\\\",\\n      \\\"stringValue\\\": \\\"turbo_eng_billing_export\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyHost\\\",\\n      \\\"stringValue\\\": \\\"10.10.174.64\\\"\\n    }, {\\n      \\\"key\\\": \\\"standardCostTableName\\\",\\n      \\\"stringValue\\\": \\\"gcp_billing_export_v1_01AAB5_DE376A_EBB741\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyPort\\\",\\n      \\\"stringValue\\\": \\\"4128\\\"\\n    }, {\\n      \\\"key\\\": \\\"pricingExportTableName\\\",\\n      \\\"stringValue\\\": \\\"cloud_pricing_export\\\"\\n    }, {\\n      \\\"key\\\": \\\"projectID\\\",\\n      \\\"stringValue\\\": \\\"turbonomic-eng\\\"\\n    }],\\n    \\\"isHidden\\\": false,\\n    \\\"readOnly\\\": false,\\n    \\\"derivedTargetIds\\\": [\\\"74380823045280\\\"],\\n    \\\"lastEditingUser\\\": \\\"administrator\\\",\\n    \\\"lastEditTime\\\": \\\"1654174971082\\\"\\n  },\\n  \\\"displayName\\\": \\\"GCP ENG Billing\\\"\\n}\"}";

    /**
     * Test migration of the standard dataset and table name fields in
     * the GCP billing probe.
     */
    @Test
    public void testMigrateCostDatasetTableNames() {
        Map<String, String> probesAndTargets = new HashMap<>();
        final String probe1 = parseJsonFileToString("migration/Probe1-gcp-billing.json");
        probesAndTargets.put(PROBE_1_ID, probe1);
        probesAndTargets.put(TARGET_1_ID, GCP_BILLING_TARGET);
        KeyValueStore testKeyStore = new FakeKeyValueStore(probesAndTargets);
        MigrationProgressInfo migrationResult = new V_01_01_18__GCP_BillingProbe_RenameStandardDatasetTable_Names(testKeyStore).doStartMigration();
        // Verify that migration is successful.
        assertEquals(MigrationStatus.SUCCEEDED, migrationResult.getStatus());

        // Test that GCP billing  targets are updated with the 2 new fields
        // with values from the standard data set and table names.
        // while AWS Billing target not.
        final Optional<String> newTarget1 = testKeyStore.get(TARGET_1_ID);
        assertTrue(valuesMigrated(newTarget1.get()));


    }


    private boolean valuesMigrated(String targetJsonstring) {
        try {
            JsonReader reader = new JsonReader(new StringReader(targetJsonstring));
            JsonObject secretFields = new JsonParser().parse(reader).getAsJsonObject();
            JsonObject targetJSON = new JsonParser().parse(reader).getAsJsonObject();
            final JsonPrimitive targetInfo = targetJSON.getAsJsonPrimitive(FIELD_TARGET_INFO);
            final JsonReader reader2 = new JsonReader(new StringReader(targetInfo.getAsString()));
            JsonObject targetInfoObject = new JsonParser().parse(reader2).getAsJsonObject();

            if (!targetInfoObject.has(FIELD_SPEC)) {
                logger.info("No target spec found.");
                return false;
            }
            JsonObject spec = targetInfoObject.get(FIELD_SPEC).getAsJsonObject();
            JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
            boolean iscostDatasetSet = false;
            boolean iscostTableSet = false;
            for (int i = 0; i < accountValues.size(); i++) {
                final JsonObject accountVal = accountValues.get(i).getAsJsonObject();
                final JsonElement key = accountVal.get("key");
                if (key == null) {
                    continue;
                }
                final JsonElement value = accountVal.get("stringValue");
                if (COST_DATA_SET_NAME.equals(key.getAsString())) {
                    iscostDatasetSet = value != null;
                } else if (COST_DATA_TABLE_NAME.equals(key.getAsString())) {
                    iscostTableSet = value != null;
                } else if (STANDARD_COST_TABLE_NAME.equals(key.getAsString())
                        || STANDARD_COST_DATASET_NAME.equals(key.getAsString())) {
                    return false;
                }
            }
            return iscostDatasetSet && iscostTableSet;

        } catch (RuntimeException e) {
            logger.error("Unable to read JSON {}", targetJsonstring);
            return false;
        }
    }

    private String parseJsonFileToString(String fileName) {
        File file1 = ResourcePath.getTestResource(getClass(), fileName).toFile();
        try {
            return Files.asCharSource(file1, Charset.defaultCharset()).read();
        } catch (IOException e) {
            logger.error("IO Exception during parsing json file " + fileName);
            return "";
        }
    }


}
