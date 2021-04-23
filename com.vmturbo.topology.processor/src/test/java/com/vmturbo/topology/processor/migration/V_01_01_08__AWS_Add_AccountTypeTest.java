package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_DEFINITION;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_TARGET_INFO;
import static com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountType.ACCOUNT_TYPE;
import static com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountType.ACCOUNT_VALUE_STANDARD;
import static com.vmturbo.topology.processor.migration.V_01_01_08__AWS_Add_AccountType.ACCOUNT_VALUE_US_GOVERNMENT;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.io.Files;
import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Test AWS Probes and Targets migration.
 */
public class V_01_01_08__AWS_Add_AccountTypeTest {

    private final Logger logger = LogManager.getLogger();

    private static final String PROBE_1_ID = "probes/73548994148144";
    private static final String PROBE_2_ID = "probes/73548994134528";
    private static final String PROBE_3_ID = "probes/73548994127888";

    private static final String TARGET_1_ID = "targets/73732399126048";
    private static final String AWS_BILLING_TARGET = "{\"secretFields\":[\"secretField1\",\"secretField2\"]}{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"73732399126048"
            + "\\\",\\n  \\\"spec\\\": {\\n    \\\"probeId\\\": \\\"73548994148144\\\",\\n    \\\"accountValue\\\": [{\\n      \\\"key\\\": \\\"proxyPort\\\",\\n      "
            + "\\\"stringValue\\\": \\\"8080\\\"\\n    }, {\\n      \\\"key\\\": \\\"password\\\",\\n      \\\"stringValue\\\": \\\"PASSWORD\\\"\\n    }, {\\n      \\\"key"
            + "\\\": \\\"address\\\",\\n      \\\"stringValue\\\": \\\"aws billing\\\"\\n    }, {\\n      \\\"key\\\": \\\"iamRole\\\",\\n      \\\"stringValue\\\": \\\"false"
            + "\\\"\\n    }, {\\n      \\\"key\\\": \\\"username\\\",\\n      \\\"stringValue\\\": \\\"USERNAME\\\"\\n    }, {\\n      \\\"key\\\": \\\"secureProxy\\\",\\n      "
            + "\\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"reportPathPrefix\\\",\\n      \\\"stringValue\\\": \\\"reportPath\\\"\\n    }, {\\n      "
            + "\\\"key\\\": \\\"bucketRegion\\\",\\n      \\\"stringValue\\\": \\\"us-west-2\\\"\\n    }, {\\n      \\\"key\\\": \\\"bucketName\\\",\\n      \\\"stringValue\\\": "
            + "\\\"bucketName\\\"\\n    }],\\n    \\\"isHidden\\\": false,\\n    \\\"readOnly\\\": false\\n  },\\n  \\\"displayName\\\": \\\"aws billing\\\"\\n}\"}";

    private static final String TARGET_2_ID = "targets/73532264562736";
    private static final String AWS_COST_TARGET = "{\"secretFields\":[\"secretField1\",\"secretField2\"]}{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"73532264562736\\\",\\n  \\\"spec"
            + "\\\": {\\n    \\\"probeId\\\": \\\"73548994134528\\\",\\n    \\\"accountValue\\\": [{\\n      \\\"key\\\": \\\"address\\\",\\n      \\\"stringValue\\\": \\\"aws"
            + "\\\"\\n    }, {\\n      \\\"key\\\": \\\"secureProxy\\\",\\n      \\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyUsername\\\",\\n      "
            + "\\\"stringValue\\\": \\\"\\\"\\n    }, {\\n      \\\"key\\\": \\\"iamRoleArn\\\",\\n      \\\"stringValue\\\": \\\"\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyPassword"
            + "\\\",\\n      \\\"stringValue\\\": \\\"proxyPassword\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyHost\\\",\\n      \\\"stringValue\\\": \\\"\\\"\\n    }, {\\n      "
            + "\\\"key\\\": \\\"iamRole\\\",\\n      \\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyPort\\\",\\n      \\\"stringValue\\\": \\\"8080"
            + "\\\"\\n    }, {\\n      \\\"key\\\": \\\"password\\\",\\n      \\\"stringValue\\\": \\\"password\\\"\\n    }, {\\n      \\\"key\\\": \\\"pricingIdentifier\\\",\\n      "
            + "\\\"stringValue\\\": \\\"111111\\\"\\n    }, {\\n      \\\"key\\\": \\\"name\\\",\\n      \\\"stringValue\\\": \\\"AWS Cost\\\"\\n    }, {\\n      \\\"key\\\": \\\"username"
            + "\\\",\\n      \\\"stringValue\\\": \\\"string\\\"\\n    }],\\n    \\\"isHidden\\\": true,\\n    \\\"readOnly\\\": false\\n  },\\n  \\\"displayName\\\": \\\"AWS Cost\\\"\\n}\"}";


    private static final String TARGET_3_ID = "targets/73732398810480";
    private static final String AWS_TARGET = "{\"secretFields\":[\"secretField1\",\"secretField2\"]}{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"73732398810480\\\","
            + "\\n  \\\"spec\\\": {\\n    \\\"probeId\\\": \\\"73548994127888\\\",\\n    \\\"accountValue\\\": [{\\n      \\\"key\\\": \\\"proxyPort\\\",\\n      "
            + "\\\"stringValue\\\": \\\"8080\\\"\\n    }, {\\n      \\\"key\\\": \\\"password\\\",\\n      \\\"stringValue\\\": \\\"password\\\"\\n    }, {\\n      "
            + "\\\"key\\\": \\\"iamRole\\\",\\n      \\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"username\\\",\\n      \\\"stringValue\\\": "
            + "\\\"USERNAME\\\"\\n    }, {\\n      \\\"key\\\": \\\"secureProxy\\\",\\n      \\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": "
            + "\\\"address\\\",\\n      \\\"stringValue\\\": \\\"aws\\\"\\n    }],\\n    \\\"isHidden\\\": false,\\n    \\\"readOnly\\\": false,\\n    \\\"derivedTargetIds"
            + "\\\": [\\\"89024\\\"]\\n  },\\n  \\\"displayName\\\": \\\"aws\\\"\\n}\"}";

    private static final String TARGET_4_ID = "targets/73898382117264";
    private static final String AWS_TARGET_2 = "{\"secretFields\":[\"secretField1\",\"secretField2\"]}{\"targetInfo\":\"{\\n  \\\"id\\\": \\\"73898382117264\\\",\\n  "
            + "\\\"spec\\\": {\\n    \\\"probeId\\\": \\\"73548994127888\\\",\\n    \\\"accountValue\\\": [{\\n      \\\"key\\\": \\\"accountType\\\",\\n      "
            + "\\\"stringValue\\\": \\\"Standard\\\"\\n    }, {\\n      \\\"key\\\": \\\"proxyPort\\\",\\n      \\\"stringValue\\\": \\\"8080\\\"\\n    }, {\\n      "
            + "\\\"key\\\": \\\"iamRole\\\",\\n      \\\"stringValue\\\": \\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"secureProxy\\\",\\n      \\\"stringValue\\\": "
            + "\\\"false\\\"\\n    }, {\\n      \\\"key\\\": \\\"username\\\",\\n      \\\"stringValue\\\": \\\"username\\\"\\n    }, {\\n      \\\"key\\\": \\\"address"
            + "\\\",\\n      \\\"stringValue\\\": \\\"aws_qa\\\"\\n    }, {\\n      \\\"key\\\": \\\"password\\\",\\n      \\\"stringValue\\\": \\\"password\\\"\\n    }],\\n    "
            + "\\\"isHidden\\\": false,\\n    \\\"readOnly\\\": false,\\n    \\\"derivedTargetIds\\\": [\\\"73532264562736\\\"]\\n  },\\n  \\\"displayName\\\": \\\"aws_qa\\\"\\n}\"}";

    /**
     * Test migration for AWS probes and targets.
     */
    @Test
    public void testMigration() {
        Map<String, String> probesAndTargets = new HashMap<>();
        // aws billing probe
        final String probe1 = parseJsonFileToString("migration/Probe1-aws-billing.json");
        // aws cost probe
        final String probe2 = parseJsonFileToString("migration/Probe2-aws-cost.json");
        // aws probe
        final String probe3 = parseJsonFileToString("migration/Probe3-aws.json");
        probesAndTargets.put(PROBE_1_ID, probe1);
        probesAndTargets.put(PROBE_2_ID, probe2);
        probesAndTargets.put(PROBE_3_ID, probe3);
        probesAndTargets.put(TARGET_1_ID, AWS_BILLING_TARGET);
        probesAndTargets.put(TARGET_2_ID, AWS_COST_TARGET);
        probesAndTargets.put(TARGET_3_ID, AWS_TARGET);
        probesAndTargets.put(TARGET_4_ID, AWS_TARGET_2);

        KeyValueStore consul = new FakeKeyValueStore(probesAndTargets);
        new V_01_01_08__AWS_Add_AccountType(consul).doStartMigration();

        // Test that AWS billing probe is not updated.
        final Optional<String> newProbe1 = consul.get(PROBE_1_ID);
        assertFalse(probeHasAccountType(newProbe1.get()));
        assertTrue(probe1 == newProbe1.get());
        // Test that AWS Cost probe already contains accountType,
        // so will not be updated.
        final Optional<String> newProbe2 = consul.get(PROBE_2_ID);
        assertTrue(probeHasAccountType(newProbe2.get()));
        assertTrue(probe2 == newProbe2.get());
        // Test that AWS probe is updated with accountType.
        final Optional<String> newProbe3 = consul.get(PROBE_3_ID);
        assertTrue(probeHasAccountType(newProbe3.get()));
        assertFalse(probe3 == newProbe3.get());

        // Test that AWS and AWS Cost targets are updated with accountType as Standard,
        // while AWS Billing target not.
        final Optional<String> newTarget1 = consul.get(TARGET_1_ID);
        assertFalse(targetHasStandardAccountType(newTarget1.get()));
        assertTrue(AWS_BILLING_TARGET == newTarget1.get());
        final Optional<String> newTarget2 = consul.get(TARGET_2_ID);
        assertTrue(targetHasStandardAccountType(newTarget2.get()));
        assertFalse(AWS_COST_TARGET == newTarget2.get());
        final Optional<String> newTarget3 = consul.get(TARGET_3_ID);
        assertTrue(targetHasStandardAccountType(newTarget3.get()));
        assertFalse(AWS_TARGET == newTarget3.get());
        // Test that for AWS target that already contains accountType, target will not be updated.
        final Optional<String> newTarget4 = consul.get(TARGET_4_ID);
        assertTrue(targetHasStandardAccountType(newTarget4.get()));
        assertTrue(AWS_TARGET_2 == newTarget4.get());
    }

    private boolean probeHasAccountType(String probeString) {
        if (probeString == null) {
            return false;
        }
        JsonObject json = null;
        try {
            JsonReader reader = new JsonReader(new StringReader(probeString));
            json = new JsonParser().parse(reader).getAsJsonObject();
        } catch (JsonIOException | JsonSyntaxException e) {
            logger.error("Exception during parsing string to json object");
            return false;
        }
        if (!json.has(FIELD_ACCOUNT_DEFINITION)) {
            return false;
        }
        JsonArray accountDefs = json.get(FIELD_ACCOUNT_DEFINITION).getAsJsonArray();
        for (int i = 0; i < accountDefs.size(); i++) {
            final JsonObject accountDef = accountDefs.get(i).getAsJsonObject();
            final JsonObject customDef = accountDef.get("customDefinition").getAsJsonObject();
            final String name = customDef.get("name").getAsString();
            if (ACCOUNT_TYPE.equals(name)) {
                JsonArray jsonArray = accountDef.get("allowedValues").getAsJsonArray();
                Set<String> allowedAccountTypes = new HashSet<>();
                for (int j = 0; j < jsonArray.size(); j++) {
                    allowedAccountTypes.add(jsonArray.get(j).getAsString());
                }
                assertThat(allowedAccountTypes, containsInAnyOrder(ACCOUNT_VALUE_STANDARD, ACCOUNT_VALUE_US_GOVERNMENT));
                return true;
            }
        }
        return false;
    }

    private boolean targetHasStandardAccountType(String targetString) {
        if (targetString == null) {
            return false;
        }
        JsonObject json = null;
        try {
            JsonReader reader = new JsonReader(new StringReader(targetString));
            // parse the first element - secret fields
            new JsonParser().parse(reader).getAsJsonObject();
            // parse the second element - target info
            json = new JsonParser().parse(reader).getAsJsonObject();
        } catch (JsonIOException | JsonSyntaxException e) {
            logger.error("Exception during parsing string to json object");
            return false;
        }
        final JsonPrimitive targetInfo = json.getAsJsonPrimitive(FIELD_TARGET_INFO);
        final JsonReader reader2 = new JsonReader(new StringReader(targetInfo.getAsString()));
        final JsonObject targetJson = new JsonParser().parse(reader2).getAsJsonObject();
        final JsonObject spec = targetJson.get("spec").getAsJsonObject();
        JsonArray accountValues = spec.get("accountValue").getAsJsonArray();
        for (int i = 0; i < accountValues.size(); i++) {
            final JsonObject accountVal = accountValues.get(i).getAsJsonObject();
            final String key = accountVal.get("key").getAsString();
            if (ACCOUNT_TYPE.equals(key)) {
                assertEquals(ACCOUNT_VALUE_STANDARD, accountVal.get("stringValue").getAsString());
                return true;
            }
        }
        return false;
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

    /**
     * Fake KeyValueStore used for testing AWS probes and targets migration.
     */
    public static class FakeKeyValueStore implements KeyValueStore {
        Map<String, String> map;

        FakeKeyValueStore(Map<String, String> map) {
            this.map = map;
        }

        @Override
        public Map<String, String> getByPrefix(final String keyPrefix) {
            Map<String, String> resultMap = new HashMap<>();
            map.entrySet().stream().forEach(entry -> {
                if (entry.getKey().startsWith(keyPrefix)) {
                    resultMap.put(entry.getKey(), entry.getValue());
                }
            });
            return resultMap;
        }

        @Override
        public void put(final String key, final String value) {
            map.put(key, value);
        }

        @Override
        public Optional<String> get(final String key) {
            return Optional.ofNullable(map.get(key));
        }

        // the following are useless methods for test.
        @Override
        public String get(final String key, final String defaultValue) {
            return null;
        }

        @Override
        public boolean containsKey(final String key) {
            return false;
        }

        @Override
        public void removeKeysWithPrefix(final String prefix) {}

        @Override
        public void removeKey(final String key) {}
    }
}
