package com.vmturbo.topology.processor.migration;

import java.io.StringReader;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Handle migration of saved targets.  Remove the parentId field and make sure derivedTargetIds
 * field is correctly populated based on parentId values.
 */
public class V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    private final KeyValueStore keyValueStore;

    private static final String TARGET_INFO = "targetInfo";

    private static final String TARGET_SPEC = "spec";

    private static final String DERIVED_TARGETS = "derivedTargetIds";

    private static final String PARENT_ID = "parentId";

    /**
     * Constructor.
     *
     * @param keyValueStore from which to get persisted targets.
     */
    public V_01_00_02__TargetSpec_Fix_Derived_Targets_Migration(final KeyValueStore keyValueStore) {
        this.keyValueStore = keyValueStore;
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        // We want to handle 2 different migrations here:
        // 1. Migrating from 7.17.  TargetSpec contains parentId field for derived targets but no
        // derived targets field for the parent targets.  We want to clear out the parent target id
        // field and add derived targets field into parent target.
        // 2. Migrating from earlier versions of 7.21.  Now parent id field will not be correct in
        // cases where derived target has more than 1 parent.  Here the derived target id field
        // needs to be converted from List<String> to List<Long> and parent target id field should
        // be cleared.
        Map<String, Set<String>> derivedTargetIdsByParentId = Maps.newHashMap();
        // get the JSON for all the previously stored targets
        final Map<String, String> persistedTargets =
            keyValueStore.getByPrefix(TargetStore.TARGET_KV_STORE_PREFIX);
        final Set<String> targetsNeedingMigration = Sets.newHashSet();
        // First iterate over saved targets and identify those that need to be migrated.  We only
        // need to migrate those that have parentId set and those that appear in other targets'
        // parentId fields.  We need to iterate twice because we can't be sure that we know all the
        // derived targets of a parent until we've examine all the targets' parentId fields.
        persistedTargets.entrySet().forEach(entry -> {
                final String targetId = entry.getKey()
                    .substring(TargetStore.TARGET_KV_STORE_PREFIX.length());
                // persisted target consists of 2 strings.  The first is secret fields of the
                // account values and the second is a string representing TargetInfo.
                final JsonReader reader = new JsonReader(new StringReader(entry.getValue()));
                final JsonObject secretFields = new JsonParser().parse(reader).getAsJsonObject();
                final JsonObject targetJson = new JsonParser().parse(reader).getAsJsonObject();
                // TargetInfo is stored as a String
                final JsonPrimitive targetInfo = targetJson.getAsJsonPrimitive(TARGET_INFO);
                // Convert the String to a JSonObject so we can easily parse out the fields
                final JsonReader reader2 = new JsonReader(new StringReader(
                    targetInfo.getAsString()));
                final JsonObject targetInfoObject = new JsonParser()
                    .parse(reader2).getAsJsonObject();
                // Extract the TargetSpec from the TargetInfo
                final JsonObject spec = targetInfoObject.getAsJsonObject(TARGET_SPEC);
                // If there is a parentId, we should migrate this target to get rid of parentId and
                // the parent target, in case the parent didn't have a derivedTargetIds field
                final JsonPrimitive parentId = spec.getAsJsonPrimitive(PARENT_ID);
                if (parentId != null) {
                    targetsNeedingMigration.add(parentId.getAsString());
                    targetsNeedingMigration.add(targetId);
                    derivedTargetIdsByParentId.computeIfAbsent(parentId.getAsString(),
                        k -> Sets.newHashSet()).add(targetId);
                }
                // Add derived targets so that we can later reconstitute the derivedTargetIds entry
                // for this target.
                final JsonArray derivedTargets = spec.getAsJsonArray(DERIVED_TARGETS);
                if (derivedTargets != null) {
                    derivedTargets.forEach(jsonElement -> derivedTargetIdsByParentId
                        .computeIfAbsent(entry.getKey(), k -> Sets.newHashSet())
                        .add(jsonElement.getAsString()));
                }
        });

        logger.info("Starting migration to delete parentId field from targets.");

        if (targetsNeedingMigration.isEmpty()) {
            String msg = "No targets with parentId to upgrade. Upgrade finished.";
            logger.info(msg);
            return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
        }

        updateMigrationProgress(MigrationStatus.RUNNING, 0F,
            "Migrating " + targetsNeedingMigration.size() + " targets.");
        // Now iterate over targets and update those we previously identified as needing migration.
        // Remove any parentId fields and update derivedTargetIds fields.
        persistedTargets.entrySet().stream()
            .filter(entry -> targetsNeedingMigration.contains(entry.getKey()
                .substring(TargetStore.TARGET_KV_STORE_PREFIX.length())))
            .forEach(entry -> {
                final String targetId = entry.getKey()
                    .substring(TargetStore.TARGET_KV_STORE_PREFIX.length());
                logger.info("Migrating target {}.", targetId);
                final JsonReader reader = new JsonReader(new StringReader(entry.getValue()));
                final JsonObject secretFields = new JsonParser().parse(reader).getAsJsonObject();
                final JsonObject targetJson = new JsonParser().parse(reader).getAsJsonObject();
                final JsonPrimitive targetInfo = targetJson.getAsJsonPrimitive(TARGET_INFO);
                final JsonReader reader2 = new JsonReader(new StringReader(targetInfo.getAsString()));
                final JsonObject targetInfoObject = new JsonParser().parse(reader2).getAsJsonObject();
                final JsonObject spec = targetInfoObject.getAsJsonObject(TARGET_SPEC);
                // Update the TargetSpec JSON by removing parentId and adding in derivedTargetIds
                spec.remove(PARENT_ID);
                final Set<String> derivedTargetIds =
                    derivedTargetIdsByParentId.get(targetId);
                if (derivedTargetIds != null && !derivedTargetIds.isEmpty()) {
                    final JsonArray derivedTargets = new JsonArray();
                    derivedTargetIds.forEach(derivedTargetId ->
                        derivedTargets.add(derivedTargetId));
                    spec.add(DERIVED_TARGETS, derivedTargets);
                }
                // Update TargetInfo with the updated TargetSpec
                targetInfoObject.add(TARGET_SPEC, spec);
                final JsonObject targetInfoWrapper = new JsonObject();
                // Add TargetInfo as a primitive JSON String because that's how we serialize it in
                // the code.
                targetInfoWrapper.add(TARGET_INFO,
                    new JsonPrimitive(targetInfoObject.toString()));
                // Add back in the secret fields when we persist it
                keyValueStore.put(entry.getKey(), secretFields.toString()
                    + targetInfoWrapper.toString());
                logger.info("Done migrating target {}.", entry.getKey());
            });
        final String msg = "All targets with parentId migrated. Upgrade finished.";
        logger.info(msg);
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
    }
}
