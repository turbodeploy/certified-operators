package com.vmturbo.topology.processor.migration;

import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS;
import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS_BILLING;
import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS_COST;
import static com.vmturbo.platform.sdk.common.util.SDKProbeType.AWS_LAMBDA;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * This class migrates the AWS probes and targets to support stand alone AWS Billing, which previously was
 * derived from the AWS probe.
 * This change includes
 *
 * <p>Targets:
 * 1) eliminating the S3 bucket fields from the AWS, AWS Cost and AWS Lambda targets.
 * 2) change AWS Billing target's field isHidden from true to false.
 * 3) remove AWS billing target ids from AWS targets' derivedTargetId list
 * 4) change name of iamRole field to iamRoleArn.
 * 5) after 4) add iamRole field with a true value if iamRoleArn field is not null or empty, else false.
 * </p>
 *
 * <p>Probes:
 *  1) change AWS probe
 *    a) change address instance type's account value displayName from "Address" to "Custom Target Name".
 *    b) change address instance type's account value description from "Enter aws.amazon.com or your AWS address." to "Enter a unique target name."
 *    c) change iamRole instance type's account value name to iamRoleArn.
 *    d) add boolean instance type iamRole with account value displayName "IAM Role", and description
 *       "Authenticate using either IAM Role ARN or Access Key and Secret Access Key."
 *    e) change username, password and iamRoleArn instance types' account value Constraint to Mandatory
 *    f) add dependencyKey="iamRole" and dependencyValue to the account value for instance types username, password and iamRoleArn
 *    g) remove the three S3 buckets instance types and account values.
 * 2) change AWS Billing probe
 *    a) change address instance type's account value displayName from "Address" to "Custom Target Name".
 *    b) change address instance type's account value description from "Enter aws.amazon.com or your AWS address." to "Enter a unique target name."
 *    c) change iamRole instance type's account value name to iamRoleArn.
 *    d) add boolean instance type iamRole with account value displayName "IAM Role", and description
 *       "Authenticate using either IAM Role ARN or Access Key and Secret Access Key."
 *    e) change username, password and iamRoleArn instance types' account value Constraint to Mandatory
 *    f) add dependencyKey="iamRole" and dependencyValue to the account value for instance types username, password and iamRoleArn
 *    g) change the three S3 fields to Mandatory
 *    h) change probeCategory from Billing to CloudManagement.
 *    i) change creationMode from "DERIVED" to "STAND_ALONE"
 *  3) change AWS lambda probe
 *    a) change address instance type's account value displayName from "Address" to "Custom Target Name".
 *    b) change address instance type's account value description from "Enter aws.amazon.com or your AWS address." to "Enter a unique target name."
 *    c) change iamRole instance type's account value name to iamRoleArn.
 *    d) add boolean instance type iamRole with account value displayName "IAM Role", and description
 *       "Authenticate using either IAM Role ARN or Access Key and Secret Access Key."
 *    e) change username, password and iamRoleArn instance types' account value Constraint to Mandatory
 *    f) add dependencyKey="iamRole" and dependencyValue to the account value for instance types username, password and iamRoleArn
 *    g) remove the three S3 fields
 *  4) change AWS cost probe (which is derived from AWS probe)
 *    a) change address instance type's account value displayName from "Address" to "Custom Target Name".
 *    b) change address instance type's account value description from "Enter aws.amazon.com or your AWS address." to "Enter a unique target name."
 *    c) change iamRole instance type's account value name to iamRoleArn.
 *    d) add boolean instance type iamRole with account value displayName "IAM Role", and description
 *       "Authenticate using either IAM Role ARN or Access Key and Secret Access Key."
 *    e) remove the three S3 fields
 * </p>
 */
public class V_01_00_05__Standalone_AWS_Billing extends AbstractMigration {

    private final Logger logger = LogManager.getLogger(getClass());

    private final KeyValueStore keyValueStore;

    // all AWS probes
    ImmutableSet<String> PROBE_TYPES_ALL_AWS = ImmutableSet.of(AWS.getProbeType(),
        AWS_BILLING.getProbeType(), AWS_COST.getProbeType(), AWS_LAMBDA.getProbeType());
    // all AWS probes excluding AWS_BILLING.
    ImmutableSet<String> PROBE_TYPES_ALL_BUT_BILLING = ImmutableSet.of(AWS.getProbeType(),
        AWS_COST.getProbeType(), AWS_LAMBDA.getProbeType());
    // all AWS probes excluding AWS_COST.
    ImmutableSet<String> PROBE_TYPES_ALL_BUT_COST = ImmutableSet.of(AWS.getProbeType(),
        AWS_BILLING.getProbeType(), AWS_LAMBDA.getProbeType());

    // probe field names.
    static final String FIELD_ACCOUNT_DEFINITION = "accountDefinition";
    static final String FIELD_ADDRESS = "address";
    static final String FIELD_BUCKET_REGION = "bucketRegion";
    static final String FIELD_BUCKET_NAME = "bucketName";
    static final String FIELD_CREATION_MODE = "creationMode";
    static final String FIELD_CUSTOM_DEFINITION = "customDefinition";
    static final String FIELD_DEFAULT_VALUE = "defaultValue";
    static final String FIELD_DEPENDENCY_KEY = "dependencyKey";
    static final String FIELD_DEPENDENCY_VALUE = "dependencyValue";
    static final String FIELD_DESCRIPTION = "description";
    static final String FIELD_DISPLAY_NAME = "displayName";
    static final String FIELD_IAM_ROLE = "iamRole";
    static final String FIELD_IAM_ROLE_ARN = "iamRoleArn";
    static final String FIELD_IS_SECRET = "isSecret";
    static final String FIELD_IS_TARGET_DISPLAY_NAME = "isTargetDisplayName";
    static final String FIELD_MANDATORY = "mandatory";
    static final String FIELD_NAME = "name";
    static final String FIELD_PASSWORD = "password";
    static final String FIELD_PRIMITIVE_VALUE = "primitiveValue";
    static final String FIELD_PROBE_ID = "probeId";
    static final String FIELD_PROBE_TYPE = "probeType";
    static final String FIELD_PROBE_CATEGORY = "probeCategory";
    static final String FIELD_REPORT_PATH_PREFIX = "reportPathPrefix";
    static final String FIELD_USERNAME = "username";
    static final String FIELD_VERIFICATION = "verificationRegex";

    static final ImmutableSet<String> s3Fields = ImmutableSet.of(FIELD_REPORT_PATH_PREFIX,
        FIELD_BUCKET_REGION, FIELD_BUCKET_NAME);

    // field values
    static final String ADDRESS_DISPLAY_NAME = "Custom Target Name";
    static final String ADDRESS_DESCRIPTION = "Enter a unique target name.";
    // IAM Role related valued
    static final String IAM_ROLE_DISPLAY_NAME = "IAM Role";
    static final String IAM_ROLE_DESCRIPTION = "Authenticate using either IAM Role ARN or Access Key and Secret Access Key.";
    static final String IAM_ROLE_VERIFICATION = "(true|false)";
    static final String VALUE_TRUE = "true";
    static final String VALUE_FALSE = "false";
    static final String PROBE_TYPE_CLOUD_MANAGEMENT = "Cloud Management";

    static final ImmutableSet<String> dependencyFields = ImmutableSet.of(FIELD_IAM_ROLE_ARN,
        FIELD_PASSWORD, FIELD_USERNAME);

    // target field names
    static final String FIELD_ACCOUNT_VALUE = "accountValue";
    static final String FIELD_ACCOUNT_VALUE_KEY = "key";
    static final String FIELD_ACCOUNT_VALUE_VALUE = "value";
    static final String FIELD_ACCOUNT_VALUE_STRING_VALUE = "stringValue";
    static final String FIELD_DERIVED_TARGET_IDS = "derivedTargetIds";
    static final String FIELD_ID = "id";
    static final String FIELD_IS_HIDDEN = "isHidden";
    static final String FIELD_SPEC = "spec";
    static final String FIELD_TARGET_INFO = "targetInfo";

    /**
     * Create the migration object.
     *
     * @param keyValueStore consul
     */
    public V_01_00_05__Standalone_AWS_Billing(@Nonnull KeyValueStore keyValueStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("Starting migration.");
        // process all the AWS probes
        BiMap<String, String> probeIdToProbeTypeMap = updateProbes(keyValueStore);
        // process all AWS targets
        updateTargets(keyValueStore, probeIdToProbeTypeMap);

        String msg = "All AWS targets and probes migrated. Upgrade finished.";
        logger.info(msg);
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
    }

    /**
     * Update AWS probes.
     *
     * @param keyValueStore consul
     * @return map from probe ID to probe type
     */
    @Nullable
    private BiMap<String, String> updateProbes(KeyValueStore keyValueStore) {
        logger.debug("Starting probe migration.");
        // map from probe ID to probe type
        BiMap<String, String> probeIdToProbeTypeMap = HashBiMap.create();
        // get the JSON for all the previously stored probes
        final Map<String, String> persistedProbes =
            keyValueStore.getByPrefix(ProbeStore.PROBE_KV_STORE_PREFIX);
        for (Entry<String, String> entry : persistedProbes.entrySet()) {
            final String key = entry.getKey();
            final String probeId = key.substring(ProbeStore.PROBE_KV_STORE_PREFIX.length());
            final JsonReader reader = new JsonReader(new StringReader(entry.getValue()));
            JsonObject json = new JsonParser().parse(reader).getAsJsonObject();
            String probeType = json.get("probeType").getAsString();
            if (!PROBE_TYPES_ALL_AWS.contains(probeType)) {
                logger.trace("probeId={} probeType='{}' not an AWS probe", probeId, probeType);
                continue;
            }
            // Only AWS probes
            // process the AWS probe
            processAwsProbe(probeId, probeType, json, probeIdToProbeTypeMap);
            // update consul
            keyValueStore.put(key, json.toString());
        }
        return probeIdToProbeTypeMap;
    }

    /**
     * Handle one AWS probe at a time.
     *
     * @param probeId               probe ID
     * @param probeType             probe Type
     * @param json                  json object representing the probe.  This value will be updated in this method and
     *                              written back to consul by the caller.
     * @param probeIdToProbeTypeMap probe ID to probe type map.  This map is constructed in this method.
     */
    @VisibleForTesting
    void processAwsProbe(final String probeId, final String probeType, JsonObject json,
                                 BiMap<String, String> probeIdToProbeTypeMap) {
        logger.debug("probeId={} probeType='{}' Starting probe migration.",
            probeId, probeType);
        // update bi map
        probeIdToProbeTypeMap.put(probeId, probeType);
        if (probeType.equals(AWS_BILLING.getProbeType())) {
            // AWS Billing probe: change probe category and creation mode
            json.remove(FIELD_PROBE_CATEGORY);
            json.addProperty(FIELD_PROBE_CATEGORY, ProbeCategory.CLOUD_MANAGEMENT.getCategory());
            json.remove(FIELD_CREATION_MODE);
            json.addProperty(FIELD_CREATION_MODE, CreationMode.STAND_ALONE.name());
            logger.trace("probeId={} probeType='{}' AWS Billing Category and creation mode", probeId, probeType);
        }
        if (!json.has(FIELD_ACCOUNT_DEFINITION)) {
            logger.error("probeId={} probeType='{}' Can't find accountDefinition", probeId, probeType);
        } else {
            JsonArray accountDefs = json.get(FIELD_ACCOUNT_DEFINITION).getAsJsonArray();
            // iterate over account definitions
            logger.trace("probeId={} probeType='{}' iterate over accountDefs", probeId, probeType);
            for (int i = 0; i < accountDefs.size(); i++) {
                JsonObject accountDef = accountDefs.get(i).getAsJsonObject();
                processAccountDefinition(accountDef, probeId, probeType);
            }
            if (PROBE_TYPES_ALL_BUT_BILLING.contains(probeType)) {
                removeS3Fields(accountDefs, probeType);
            }
            addIamRoleField(accountDefs, probeType);
        }
    }

    /**
     * Add IAM Role account definition.
     *
     * @param accountDefs where to add IAM role account definition
     * @param probeType   needed for log message.
     */
    @VisibleForTesting
    void addIamRoleField(JsonArray accountDefs, String probeType) {
        JsonObject customDef = new JsonObject();
        customDef.addProperty(FIELD_NAME, FIELD_IAM_ROLE);
        customDef.addProperty(FIELD_DISPLAY_NAME, IAM_ROLE_DISPLAY_NAME);
        customDef.addProperty(FIELD_DESCRIPTION, IAM_ROLE_DESCRIPTION);
        customDef.addProperty(FIELD_VERIFICATION, IAM_ROLE_VERIFICATION);
        customDef.addProperty(FIELD_PRIMITIVE_VALUE, PrimitiveValue.BOOLEAN.name());
        JsonObject def = new JsonObject();
        def.add(FIELD_CUSTOM_DEFINITION, customDef);
        def.addProperty(FIELD_IS_TARGET_DISPLAY_NAME, VALUE_FALSE);
        def.addProperty(FIELD_MANDATORY, VALUE_TRUE);
        def.addProperty(FIELD_DEFAULT_VALUE, VALUE_FALSE);
        accountDefs.add(def);
        logger.trace("probeType='{}' add def={}", probeType, def);
    }

    /**
     * Find all S3 bucket fields and remove them.
     *
     * @param accountDefs array of account definitions.
     * @param probeType   needed for log message.
     */
    private void removeS3Fields(JsonArray accountDefs, String probeType) {
        // interate over account definitions in reverse order
        for (int i = accountDefs.size() - 1; i >= 0; i--) {
            JsonObject accountDef = accountDefs.get(i).getAsJsonObject();
            JsonObject customDef = accountDef.get(FIELD_CUSTOM_DEFINITION).getAsJsonObject();
            if (customDef == null) {
                continue;
            }
            String name = customDef.get(FIELD_NAME).getAsString();
            if (s3Fields.contains(name)) {
                accountDefs.remove(i);
                logger.trace("probeType='{}' remove S3 field={}", probeType, accountDef.toString());
            }
        }
    }

    /**
     * Process an account definition.
     *
     * @param accountDef the account definition
     * @param probeId  probe ID
     * @param probeType  probe type
     */
    private void processAccountDefinition(JsonObject accountDef, String probeId, String probeType) {
        boolean changed = false;
        JsonObject customDef = accountDef.get(FIELD_CUSTOM_DEFINITION).getAsJsonObject();
        if (customDef == null) {
            return;
        }
        String name = customDef.get(FIELD_NAME).getAsString();
        if (FIELD_ADDRESS.equals(name)) {
            customDef.remove(FIELD_DISPLAY_NAME);
            customDef.addProperty(FIELD_DISPLAY_NAME, ADDRESS_DISPLAY_NAME);
            customDef.remove(FIELD_DESCRIPTION);
            customDef.addProperty(FIELD_DESCRIPTION, ADDRESS_DESCRIPTION);
            changed = true;
            logger.trace("probeId={} probeType='{}' field={} update", probeId, probeType, name);
        } else if (FIELD_IAM_ROLE.equals(name)) {
            customDef.remove(FIELD_NAME);
            customDef.addProperty(FIELD_NAME, FIELD_IAM_ROLE_ARN);
            changed = true;
            logger.trace("probeId={} probeType='{}' field={} change to iamRoleArn ", probeId, probeType, name);
            name = FIELD_IAM_ROLE_ARN;
        }
        if (PROBE_TYPES_ALL_BUT_COST.contains(probeType) && dependencyFields.contains(name)) {
            accountDef.remove(FIELD_MANDATORY);
            accountDef.addProperty(FIELD_MANDATORY, VALUE_TRUE);
            // add dependency fields to all AWS probes other than cost.
            accountDef.remove(FIELD_DEPENDENCY_KEY);
            accountDef.addProperty(FIELD_DEPENDENCY_KEY, FIELD_IAM_ROLE);
            accountDef.remove(FIELD_DEPENDENCY_VALUE);
            if (name.equals(FIELD_IAM_ROLE_ARN)) {
                accountDef.addProperty(FIELD_DEPENDENCY_VALUE, VALUE_TRUE);
                logger.trace("probeId={} probeType='{}' field={} add dependencyKey={} with dependencyValue={}",
                    probeId, probeType, name, FIELD_IAM_ROLE, VALUE_TRUE);
            } else {
                accountDef.addProperty(FIELD_DEPENDENCY_VALUE, VALUE_FALSE);
                logger.trace("probeId={} probeType='{}' field={} add dependencyKey={} with dependencyValue={}",
                    probeId, probeType, name, FIELD_IAM_ROLE, VALUE_FALSE);
            }
            changed = true;
        }
        if (AWS.getProbeType().equals(probeType) && s3Fields.contains(name)) {
            accountDef.remove(FIELD_MANDATORY);
            accountDef.addProperty(FIELD_MANDATORY, VALUE_TRUE);
            logger.trace("probeType='{}' field={}  change {} to {}",
                probeType, name, FIELD_MANDATORY, VALUE_TRUE);
        }
        if (changed) {
            logger.trace("probeId={} probeType='{}' processed accountDef={}", probeId, probeType, accountDef.toString());
        }
    }

    /*
     * Keep track of AWS and AWS Billing targets.
     */
    private Set<String> awsTargetKeys = new HashSet<>();
    private Set<String> awsBillingTargetIds = new HashSet<>();

    /**
     * Update targets.
     *
     * @param keyValueStore         consul
     * @param probeIdToProbeTypeMap bidirectional map from probe ID to probe type
     */
    private void updateTargets(KeyValueStore keyValueStore,
                               BiMap<String, String> probeIdToProbeTypeMap) {
        logger.info("Starting target migration.");
        final Map<String, String> persistedTargets =
            keyValueStore.getByPrefix(TargetStore.TARGET_KV_STORE_PREFIX);
        // handle all other AWS targets
        // iterate through persisted targets
        for (Entry<String, String> entry : persistedTargets.entrySet()) {
            final JsonReader reader = new JsonReader(new StringReader(entry.getValue()));
            final JsonObject secretFields = new JsonParser().parse(reader).getAsJsonObject();
            final JsonObject targetJson = new JsonParser().parse(reader).getAsJsonObject();
            final JsonPrimitive targetInfo = targetJson.getAsJsonPrimitive(FIELD_TARGET_INFO);
            final JsonReader reader2 = new JsonReader(new StringReader(targetInfo.getAsString()));
            final JsonObject targetInfoObject = new JsonParser().parse(reader2).getAsJsonObject();
            processTarget(secretFields, targetInfoObject, entry.getKey(), probeIdToProbeTypeMap);

        }
        awsBillingTargetNoLongerDerived(persistedTargets, probeIdToProbeTypeMap);
    }

    /**
     * Process a target.
     *
     * @param secretFields secrets
     * @param targetInfoObject non secret infor
     * @param targetKey target Key
     * @param probeIdToProbeTypeMap map from probe ID to probe type
     */
    private void processTarget(JsonObject secretFields, JsonObject targetInfoObject,
                               String targetKey, BiMap<String, String> probeIdToProbeTypeMap) {

        Set<String> awsProbeIds = probeIdToProbeTypeMap.keySet();
        String targetId = targetInfoObject.get(FIELD_ID).getAsString();
        JsonObject spec = targetInfoObject.get(FIELD_SPEC).getAsJsonObject();
        String probeId = spec.get(FIELD_PROBE_ID).getAsString();
        if (!awsProbeIds.contains(probeId)) {
            // not AWS target
            logger.trace("targetId={} probeId=\"{}\" not an AWS target", targetId, probeId);
            return;
        }
        String probeType = probeIdToProbeTypeMap.get(probeId);

        logger.debug("targetId={} probeType='{}' Starting target migration targetInfo={}.",
            targetId, probeType, targetInfoObject.toString());
        processSpec(spec, targetKey, targetId, probeId, probeType);


        // Write changes back to consul
        final JsonObject targetInfoWrapper = new JsonObject();
        targetInfoWrapper.add(FIELD_TARGET_INFO,
            new JsonPrimitive(targetInfoObject.toString()));
        logger.trace("targetId={} probeType='{}' targetInfo={}", targetId, probeType, targetInfoWrapper.toString());
        keyValueStore.put(targetKey, secretFields.toString() + targetInfoWrapper.toString());
    }

    /**
     * Process the specification of the target.
     *
     * @param spec  specification
     * @param targetKey target Key
     * @param targetId target ID
     * @param probeId  probe ID
     * @param probeType  probe type
     */
    @VisibleForTesting
    void processSpec(JsonObject spec, String targetKey, String targetId, String probeId, String probeType) {
        // Only AWS Targets
        if (AWS.getProbeType().equals(probeType)) {
            // save AWS target key
            awsTargetKeys.add(targetKey);
        }
        if (AWS_BILLING.getProbeType().equals(probeType)) {
            // save AWS Billing target ID
            awsBillingTargetIds.add(targetId);
            // change isHidden from true to false
            spec.remove(FIELD_IS_HIDDEN);
            spec.addProperty(FIELD_IS_HIDDEN, VALUE_FALSE);
            logger.trace("AWS Billing targetId={} probeType='{}' change isHidden to false",
                targetId, probeType);
        }
        // Determine iamRole's value.
        boolean iamRoleValue = false;
        JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        // iterate over the fields
        for (int i = accountValues.size() - 1; i >= 0; i--) {
            JsonObject accountValue = accountValues.get(i).getAsJsonObject();
            String key = accountValue.get(FIELD_ACCOUNT_VALUE_KEY).getAsString();
            if (s3Fields.contains(key) && PROBE_TYPES_ALL_BUT_BILLING.contains(probeType)) {
                // remove S3 fields
                accountValues.remove(i);
                logger.trace("targetId={} probeType='{}' remove S3 field={}", targetId, probeType, key);
            } else if (FIELD_IAM_ROLE.equals(key)) {
                String value = accountValue.get(FIELD_ACCOUNT_VALUE_STRING_VALUE).getAsString();
                if (!Strings.isNullOrEmpty(value)) {
                    // IAM Role ARN is specified, therefore IAM Role is true.
                    logger.trace("targetId={} probeType='{}' iamRole is true, because iamRoleArn={}", targetId,
                        probeType, accountValue.toString());
                    iamRoleValue = true;
                }
                accountValue.addProperty(FIELD_ACCOUNT_VALUE_KEY, FIELD_IAM_ROLE_ARN);
                logger.trace("targetId={} probeType='{}' convert iamRole to iamRoleArn {}", targetId, probeType,
                    accountValue.toString());
            }
        }
        addIamRoleFieldToTarget(spec, iamRoleValue, targetId, probeType);
    }

    /**
     * Add IAM role field to target.
     *
     * @param spec target's specification
     * @param iamRoleValue  IAM role value
     * @param targetId target ID
     * @param probeType probe type
     */
    private void addIamRoleFieldToTarget(JsonObject spec, boolean iamRoleValue, String targetId, String probeType) {
        // add iamRole field
        JsonObject iamRole = new JsonObject();
        iamRole.addProperty("key", FIELD_IAM_ROLE);
        iamRole.addProperty("stringValue", String.valueOf(iamRoleValue));
        logger.trace("targetId={} probeType='{}' add iamRole={}", targetId, probeType, iamRole.toString());
        JsonArray accountValues = spec.get(FIELD_ACCOUNT_VALUE).getAsJsonArray();
        accountValues.add(iamRole);
    }

    /**
     * Remove AWS Billing Target from AWS Target's derived targets.
     *
     * @param persistedTargets consul
     * @param probeIdToProbeTypeMap BiMap from probe ID to probe Type
     */
    private void awsBillingTargetNoLongerDerived(Map<String, String> persistedTargets,
                                                 BiMap<String, String> probeIdToProbeTypeMap) {
        if (awsTargetKeys.size() == 0 || awsBillingTargetIds.size() == 0) {
            logger.trace("probeType='AWS' awsTargetKey.size()={} or awsBillingTargetId.size()={} == 0",
                awsTargetKeys.size(), awsBillingTargetIds.size());
            return;
        }
        for (String awsTargetKey : awsTargetKeys) {
            // remove AWS Billing target from AWS target's derivedTargetIds field.
            logger.debug("awsBillingTargetNoLongerDerived: AWS key={}", awsTargetKey);
            final JsonReader reader = new JsonReader(new StringReader(persistedTargets.get(awsTargetKey)));
            final JsonObject secretFields = new JsonParser().parse(reader).getAsJsonObject();
            final JsonObject targetJson = new JsonParser().parse(reader).getAsJsonObject();
            final JsonPrimitive targetInfo = targetJson.getAsJsonPrimitive(FIELD_TARGET_INFO);
            final JsonReader reader2 = new JsonReader(new StringReader(targetInfo.getAsString()));
            final JsonObject targetInfoObject = new JsonParser().parse(reader2).getAsJsonObject();

            String targetId = targetInfoObject.get(FIELD_ID).getAsString();
            JsonObject spec = targetInfoObject.get(FIELD_SPEC).getAsJsonObject();
            String probeId = spec.get(FIELD_PROBE_ID).getAsString();
            String probeType = probeIdToProbeTypeMap.get(probeId);
            JsonArray targetIds = spec.get(FIELD_DERIVED_TARGET_IDS).getAsJsonArray();
            logger.trace("targetId={} probeType='{}' targetIds={}", targetId, probeType, targetIds);

            boolean madeUpdate = processAwsTargetToRemoveBillingTargets(targetIds, awsBillingTargetIds, targetId, probeType);

            if (madeUpdate) {
                // Write changes back to consul
                final JsonObject targetInfoWrapper = new JsonObject();
                targetInfoWrapper.add(FIELD_TARGET_INFO,
                    new JsonPrimitive(targetInfoObject.toString()));
                keyValueStore.put(awsTargetKey, secretFields.toString() + targetInfoWrapper.toString());
                logger.debug("targetId={} probeType='{}' targetInfo={}", targetId, probeType, targetInfoWrapper.toString());
            }

        }
    }

    /**
     * Remove any AWS billing targets from an AWS target's derivedTargetIds.
     *
     * @param targetIds AWS derived target ids
     * @param awsBillingTargetIds set of AWS billing target IDs.
     * @param targetId target ID
     * @param probeType probe Type target Key
     * @return true if AWS billing target id found, else false.
     */
    @VisibleForTesting
    boolean processAwsTargetToRemoveBillingTargets(JsonArray targetIds, Set<String> awsBillingTargetIds,
                                                           String targetId, String probeType) {
        boolean madeUpdate = false;
        for (int i = 0; i < targetIds.size(); i++) {
            String id = targetIds.get(i).getAsString();
            if (awsBillingTargetIds.contains(id)) {
                targetIds.remove(i);
                logger.trace("targetId={} probeType='{}' remove AWS Billing targetId={}",
                    targetId, probeType, id);
                madeUpdate = true;
                break;
            }
        }
        return madeUpdate;
    }
}
