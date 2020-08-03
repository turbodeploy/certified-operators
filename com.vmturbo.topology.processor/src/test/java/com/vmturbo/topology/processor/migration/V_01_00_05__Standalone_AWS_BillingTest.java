package com.vmturbo.topology.processor.migration;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS;
import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS_BILLING;
import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE;
import static com.vmturbo.topology.processor.probes.ProbeStore.PROBE_KV_STORE_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.util.JsonFormat;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Test that V_01_00_04_StandaloneAWS_Billing, which migrates the AWS Billing probe to be stand alone.
 * This migration effects all the AWS probes.
 */
public class V_01_00_05__Standalone_AWS_BillingTest {

    private static final String PROBE_CATEGORY_INITIAL = "initialCategory";
    private static final String PROBE_CATEGORY_FINAL = "finalCategory";
    private static final String PROBE_TYPE = "probeType";


    private static final String FIELD_DISPLAY_NAME = "Display name";
    private static final String FIELD_DESCRIPTION = "Description";
    private static final String spec_aws_development_string = "{\"probeId\": \"73453106613520\",\"accountValue\": "
        + "[{ \"key\": \"password\", \"stringValue\": \"AAAAAQAAACBe+zYxB2P6RaBL5gOkXrk0ALm9qeMOl40mVIt/ZpPIqwAAABA8TPx33h3AcGIfDPwyakIyAAAAOA9iHFjw2OaV9j/T7AQwkNq1z/ym44QFHdH7IAjc6ucB5JJr8aL42Q7938rsUesuNrNtwHK7WDTr\"},"
        + "{  \"key\": \"port\", \"stringValue\": \"0\" },"
        + "{  \"key\": \"username\", \"stringValue\": \"AKIAIF4PPFRCVX2EPLPQ\"},"
        + "{  \"key\": \"reportPathPrefix\", \"stringValue\": \"daily/turbonomic-cost-and-usage-reports\"},"
        + "{  \"key\": \"bucketRegion\", \"stringValue\": \"us-west-2\"},"
        + "{  \"key\": \"bucketName\", \"stringValue\": \"turbonomic-cost-and-usage-reports\"}, "
        + "{  \"key\": \"address\", \"stringValue\": \"Development\"} "
        + "], \"isHidden\": false, \"readOnly\": false, "
        + "\"derivedTargetIds\": [\"73453119181649\", \"73453119181648\"]}";

    private static final String spec_aws_billing_string = "{\"probeId\": \"73453106569008\","
        + "\"accountValue\": ["
        + "{  \"key\": \"address\", \"stringValue\": \"Development_billing\"},"
        + "{  \"key\": \"username\", \"stringValue\": \"AKIAIF4PPFRCVX2EPLPQ\"}, "
        + "{  \"key\": \"password\",  \"stringValue\": \"AAAAAQAAACBBlIALIW+c3CpiutTYx8cOTdsWKjJxZ6tNbXrewiOY/QAAABA2cC78yBMSNpBmIio2cuvyAAAAOMuWfrGHXh86uQs246GSWZPVTKfCUHH7EyO19Uy7yY47EfiAheGUrF93lrPBee6eih2VetTTHUHO\"}, "
        + "{  \"key\": \"iamRole\",  \"stringValue\": \"arn:aws:iam::00000:role/turbonomics-service-role/itx-TurbonomicsServiceRole-000000\"}, "
        + "{  \"key\": \"bucketName\", \"stringValue\": \"turbonomic-cost-and-usage-reports\"}, "
        + "{  \"key\": \"bucketRegion\", \"stringValue\": \"us-west-2\"}, "
        + "{  \"key\": \"reportPathPrefix\", \"stringValue\": \"daily/turbonomic-cost-and-usage-reports\"}, "
        + "{  \"key\": \"proxy\", \"stringValue\": \"\"}, "
        + "{  \"key\": \"port\", \"stringValue\": \"0\"}, "
        + "{  \"key\": \"proxyUser\", \"stringValue\": \"\"}, "
        + "{  \"key\": \"proxyPassword\", \"stringValue\": \"AAAAAQAAACCapgx/Ok0aNCHM+ygbOGn9et06mSDu2MP1utv4B4BKYgAAABDxzWYFIKbwiBCITnKlDU7HAAAAELunA6iOMTk1wK8L4x+uh04\\u003d\"}],"
        + " \"isHidden\": true, \"readOnly\": false}";

    private static final String probeInfoString = "{"
        + "    \"probeType\": \"AWS Cost\","
        + "    \"probeCategory\": \"Cost\","
        + "    \"accountDefinition\": [{"
        + "    \"customDefinition\": {"
        + "        \"name\": \"name\","
        + "            \"displayName\": \"Name\","
        + "            \"description\": \"The name of the target for logging\","
        + "            \"verificationRegex\": \".*\","
        + "            \"isSecret\": false,"
        + "            \"primitiveValue\": \"STRING\""
        + "    },"
        + "    \"mandatory\": true,"
        + "        \"isTargetDisplayName\": true"
        + "}],"
        + "   \"targetIdentifierField\": [\"pricingIdentifier\"],"
        + "   \"fullRediscoveryIntervalSeconds\": 3600,"
        + "   \"entityMetadata\": [{"
        + "   \"entityType\": \"BUSINESS_ACCOUNT\", "
        + "       \"nonVolatileProperties\": [{"
        + "       \"name\": \"id\""
        + "   }]"
        + "}, {"
        + "   \"entityType\": \"CLOUD_SERVICE\","
        + "       \"nonVolatileProperties\": [{"
        + "       \"name\": \"id\""
        + "   }]"
        + "}],"
        + "    \"actionPolicy\": [{"
        + "    \"entityType\": \"UNKNOWN\","
        + "        \"policyElement\": [{"
        + "        \"actionType\": \"NONE\","
        + "            \"actionCapability\": \"NOT_EXECUTABLE\""
        + "    }]"
        + "}],"
        + "    \"creationMode\": \"DERIVED\""
        + "}";

    private static final KeyValueStore kvStore = Mockito.mock(KeyValueStore.class);
    private static final IdentityProvider idenityProvider = Mockito.mock(IdentityProvider.class);
    private V_01_00_05__Standalone_AWS_Billing migration = null;

    /**
     * Create instance of migration before each test.
     */
    @Before
    public void init() {
        migration = new V_01_00_05__Standalone_AWS_Billing(kvStore, idenityProvider);
    }

    /**
     * Test processSpec method with AWS Target.
     */
    @Test
    public void testProcessSpecWithAwsTarget() {
        JsonObject spec = new JsonParser().parse(spec_aws_development_string).getAsJsonObject();
        String probeType = AWS.getProbeType();
        String probeId = spec.get("probeId").getAsString();
        String targetKey = "targetKey";
        String targetId = "73453114215728";

        migration.processSpec(spec, targetKey, targetId, probeType);
        boolean foundIamRole = false;
        String foundIamRoleValue = null;
        boolean foundS3Field = false;
        boolean foundIamRoleArn = false;
        JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        // iterate over the fields
        for (int i = accountValues.size() - 1; i >= 0; i--) {
            JsonObject accountValue = accountValues.get(i).getAsJsonObject();
            String key = accountValue.get(V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE_KEY).getAsString();
            if (V_01_00_05__Standalone_AWS_Billing.s3Fields.contains(key)) {
                foundS3Field = true;
            } else if (V_01_00_05__Standalone_AWS_Billing.FIELD_IAM_ROLE.contains(key)) {
                foundIamRole = true;
                foundIamRoleValue = accountValue.get(V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE_STRING_VALUE).getAsString();
            } else if (V_01_00_05__Standalone_AWS_Billing.FIELD_IAM_ROLE_ARN.contains(key)) {
                foundIamRoleArn = true;
            }
        }
        assertFalse(foundS3Field);
        assertTrue(foundIamRole);
        assertTrue("true".equals(foundIamRoleValue) || "false".equals(foundIamRoleValue));
        if (foundIamRoleArn) {
            assertTrue("true".equals(foundIamRoleValue));
        }
    }

    /**
     * Test processSpec method with AWS Billing Target.
     */
    @Test
    public void testProcessSpecWithAwsBillingTarget() {
        JsonObject spec = new JsonParser().parse(spec_aws_billing_string).getAsJsonObject();
        String probeType = AWS_BILLING.getProbeType();
        String targetKey = "targetKey";
        String targetId = "73453114215728";

        migration.processSpec(spec, targetKey, targetId, probeType);
        JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        boolean foundIamRole = false;
        String foundIamRoleValue = null;
        int numberOfS3FieldsFound = 0;
        boolean foundIamRoleArn = false;
        // iterate over the fields
        for (int i = accountValues.size() - 1; i >= 0; i--) {
            JsonObject accountValue = accountValues.get(i).getAsJsonObject();
            String key = accountValue.get(V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE_KEY).getAsString();
            if (V_01_00_05__Standalone_AWS_Billing.s3Fields.contains(key)) {
                numberOfS3FieldsFound++;
            } else if (V_01_00_05__Standalone_AWS_Billing.FIELD_IAM_ROLE.contains(key)) {
                foundIamRole = true;
                foundIamRoleValue = accountValue.get(V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE_STRING_VALUE).getAsString();
            } else if (V_01_00_05__Standalone_AWS_Billing.FIELD_IAM_ROLE_ARN.contains(key)) {
                String value = accountValue.get(V_01_00_05__Standalone_AWS_Billing.FIELD_ACCOUNT_VALUE_STRING_VALUE).getAsString();
                if (!Strings.isNullOrEmpty(value)) {
                    foundIamRoleArn = true;
                }
            }
        }
        assertTrue("numberOfS3FieldsFound=" + numberOfS3FieldsFound + " != 3", numberOfS3FieldsFound == 3);
        assertTrue(foundIamRole);
        assertTrue("true".equals(foundIamRoleValue) || "false".equals(foundIamRoleValue));
        if (foundIamRoleArn) {
            assertTrue("true".equals(foundIamRoleValue));
        }
        Gson gson = new GsonBuilder().create();
        //deep copy allowed only from version 2.8.2.
        JsonObject clonedObject = gson.fromJson(gson.toJson(spec), JsonObject.class);
        // run migration again and check if changes anything in account values.
        migration.processSpec(spec, targetKey, targetId, probeType);
        assertEquals(clonedObject, spec);
    }

    /**
     * Test adding IAM Role field to a probe.
     */
    @Test
    public void testAddProbeIamRoleField() {
        JsonArray accountDefs = new JsonArray();
        String probeType = "AWS Accounts";
        assertTrue("accountDefinitions.size()=" + accountDefs.size() + " != 0",
                accountDefs.size() == 0);
        // call method
        migration.addIamRoleField(accountDefs, probeType);

        assertTrue("accountDefinitions.size()=" + accountDefs.size() + " != 1",
                accountDefs.size() == 1);
        JsonObject accountDef = accountDefs.get(0).getAsJsonObject();
        assertTrue(accountDef != null);
        String isTarget = accountDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_IS_TARGET_DISPLAY_NAME).getAsString();
        assertTrue(isTarget.equals(V_01_00_05__Standalone_AWS_Billing.VALUE_FALSE));
        String mandatory = accountDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_MANDATORY).getAsString();
        assertTrue(mandatory.equals(V_01_00_05__Standalone_AWS_Billing.VALUE_TRUE));
        JsonObject customeDef = accountDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_CUSTOM_DEFINITION).getAsJsonObject();
        assertTrue(customeDef != null);
        String name = customeDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_NAME).getAsString();
        assertTrue(name.equals(V_01_00_05__Standalone_AWS_Billing.FIELD_IAM_ROLE));

        String displayName = customeDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_DISPLAY_NAME).getAsString();
        assertTrue(displayName.equals(V_01_00_05__Standalone_AWS_Billing.IAM_ROLE_DISPLAY_NAME));
        String description = customeDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_DESCRIPTION).getAsString();
        assertTrue(description.equals(V_01_00_05__Standalone_AWS_Billing.IAM_ROLE_DESCRIPTION));
        String verification = customeDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_VERIFICATION).getAsString();
        assertTrue(verification.equals(V_01_00_05__Standalone_AWS_Billing.IAM_ROLE_VERIFICATION));
        String primitiveValue = customeDef.get(V_01_00_05__Standalone_AWS_Billing.FIELD_PRIMITIVE_VALUE).getAsString();
        assertTrue(primitiveValue.equals(PrimitiveValue.BOOLEAN.name()));
    }

    /**
     * Test process AWS probes.
     * 1) test AWS probe, change probe type to AWS Accounts
     */
    @Test
    public void testProcessAwsProbesTopLevel() {
        // move "AWS" probeType to "AWS Accounts"
        String probeIdString = "123456789";
        String probeType = "AWS";
        BiMap<String, String> probeIdToProbeTypeMap = HashBiMap.create();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_TYPE, "AWS");

        migration.processAwsProbe(probeIdString, probeType, jsonObject);
        assertTrue(jsonObject.get(V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_TYPE).getAsString().equals(AWS.getProbeType()));

        probeType = AWS_BILLING.getProbeType();
        jsonObject = new JsonObject();
        jsonObject.addProperty(V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_TYPE, AWS_BILLING.getProbeType());
        jsonObject.addProperty(V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_CATEGORY, ProbeCategory.BILLING.getCategory());
        migration.processAwsProbe(probeIdString, probeType, jsonObject);
        assertTrue(jsonObject.get(V_01_00_05__Standalone_AWS_Billing.FIELD_PROBE_CATEGORY).getAsString()
            .equals(ProbeCategory.BILLING.getCategory()));
    }

    private static final String AWS_TARGET_ID_1 = "20000000";
    private static final String AWS_BILLING_TARGET_ID_1 = "20000001";
    private static final String AWS_BILLING_TARGET_ID_2 = "20000002";
    private static final String AWS_BILLING_TARGET_ID_3 = "20000003";
    private static final String AWS_COST_TARGET_ID_1 = "20000004";

    private static final String AWS_PROBE_ID_1 = "10000000";

    /**
     * Test method processAwsTargetToRemoveBillingTargets with multiple AWS Billing targets.
     */
    @Test
    public void testProcessAwsTargetToRemoveBillingTargets() {

        Set<String> awsBillingTargetIds = new HashSet<>();
        awsBillingTargetIds.add(AWS_BILLING_TARGET_ID_1);
        awsBillingTargetIds.add(AWS_BILLING_TARGET_ID_2);
        awsBillingTargetIds.add(AWS_BILLING_TARGET_ID_3);

        JsonArray targetIds = new JsonArray();
        targetIds.add(AWS_BILLING_TARGET_ID_1);
        targetIds.add(AWS_COST_TARGET_ID_1);
        targetIds.add(AWS_BILLING_TARGET_ID_2);

        assertTrue(targetIds.size() == 3);

        boolean madeUpdate = migration.processAwsTargetToRemoveBillingTargets(targetIds, awsBillingTargetIds,
            AWS_TARGET_ID_1, AWS.getProbeType());

        assertTrue(madeUpdate);
        assertTrue(targetIds.size() == 2);
    }


    private final String probeId = "1111111";
    private final String probeType = "AWS probe";

    /**
     * Test updateIdentityProvider method returns true.
     * The string representation of probe info has not changed.
     */
    @Test
    public void testUpdateIdentityProviderSameString() {
        boolean succeeded = migration.updateIdentityProvider(probeId, probeType, probeInfoString, probeInfoString);
        assertTrue(succeeded);
    }

    /**
     * Test updateIdentityProvider method returns false.
     * Exception parsing the json for getProbeId
     *
     */
    @Test
    public void testUpdateIdentityProviderFailsJsonParsing() {
        boolean succeeded = migration.updateIdentityProvider(probeId, probeType, "junk", probeInfoString);
        assertFalse(succeeded);
    }

    /**
     * Test updateIdentityProvider method return true.
     * The identity provider fails on updateProbe.
     */
    @Test
    public void testUpdateIdentityProviderTrueNotFoundInIdentityProvider() {
        final long probeId = 123456789L;
        final String probeIdString = "123456789";
        final KeyValueStore localKvStore = Mockito.mock(KeyValueStore.class);
        final IdentityProvider localIdenityProvider = Mockito.mock(IdentityProvider.class);

        final MediationMessage.ProbeInfo.Builder probeInfoBuilder = MediationMessage.ProbeInfo.newBuilder();
        try {
            JsonFormat.parser().merge(probeInfoString, probeInfoBuilder);
            MediationMessage.ProbeInfo localProbeInfo = probeInfoBuilder.build();
            Mockito.when(localIdenityProvider.getProbeId(localProbeInfo)).thenReturn(probeId);
        } catch (Exception e) {
            assertTrue(false);
        }
        V_01_00_05__Standalone_AWS_Billing localMigration = new V_01_00_05__Standalone_AWS_Billing(localKvStore,
            localIdenityProvider);

        boolean succeeded = migration.updateIdentityProvider(probeIdString, probeType, probeInfoString, "junk");
        assertFalse(succeeded);
    }

    /**
     * Test migration works when we encounter a folder containing probe properties.
     */
    @Test
    public void testProbeProperty() {
        final KeyValueStore localKvStore = Mockito.mock(KeyValueStore.class);
        final IdentityProvider localIdenityProvider = Mockito.mock(IdentityProvider.class);
        V_01_00_05__Standalone_AWS_Billing localMigration = new V_01_00_05__Standalone_AWS_Billing(localKvStore,
                localIdenityProvider);
        Map<String, String> sampleKVStoreEntries = ImmutableMap.of(
                PROBE_KV_STORE_PREFIX.concat("/73462156846800"), probeInfoString,
                PROBE_KV_STORE_PREFIX.concat("/73462156846801"), new JsonObject().toString(),
                PROBE_KV_STORE_PREFIX.concat("/73462156846800/"), new JsonObject().toString());
        Mockito.when(localKvStore.getByPrefix(PROBE_KV_STORE_PREFIX)).thenReturn(sampleKVStoreEntries);
        localMigration.updateProbes(localKvStore);
    }
}
