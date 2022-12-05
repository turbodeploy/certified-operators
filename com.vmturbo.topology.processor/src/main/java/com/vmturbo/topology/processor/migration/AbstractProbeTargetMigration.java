package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.migration.V_01_00_05__Standalone_AWS_Billing.FIELD_TARGET_INFO;
import static com.vmturbo.topology.processor.probeproperties.KVBackedProbePropertyStore.PROBE_PROPERTY_PREFIX;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.gson.JsonElement;
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
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Abstract class that handles generic logic to migrate probes and targets, which includes
 * 1. Extract all probes and targets information from key-value store.
 * 2. Iterate over individual probe or target, and parse as String or Json fields.
 * 3. During probe migration, probeIdToProbeTypeMap records the probes that have been migrated.
 * Subclasses define detailed migration logic with the String or Json fields for each probe
 * or target, and update key-value store with the migration result.
 */
public abstract class AbstractProbeTargetMigration extends AbstractMigration {

    protected static final String PROBE_ID = "probeId";
    protected static final String TARGET_ID = "id";
    protected static final String TARGET_INFO = "targetInfo";
    protected static final String TARGET_NAME = "displayName";
    protected static final String TARGET_SPEC = "spec";
    protected static final String ACCOUNT_VALUE = "accountValue";
    protected static final String ACCOUNT_VALUE_KEY = "key";
    protected static final String ACCOUNT_VALUE_VALUE = "stringValue";

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final KeyValueStore keyValueStore;
    // map from probe ID to probe type, which is populated in updateProbes and used in updateTargets.
    protected final Map<String, String> probeIdToProbeTypeMap;

    /**
     * Create the migration object.
     *
     * @param keyValueStore consul
     */
    protected AbstractProbeTargetMigration(@Nonnull KeyValueStore keyValueStore) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        this.probeIdToProbeTypeMap = new HashMap<>();
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("{} starts", migrationPurpose());
        processProbes();
        processTargets();
        final String msg = migrationPurpose() + " finished. Upgrade succeeded.";
        logger.info(msg);
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
    }

    private void processProbes() {
        logger.info("Starting probe migration.");
        // get the JSON for all the previously stored probes
        final Map<String, String> persistedProbes =
                keyValueStore.getByPrefix(ProbeStore.PROBE_KV_STORE_PREFIX);
        for (Entry<String, String> entry : persistedProbes.entrySet()) {
            final String consulProbeKey = entry.getKey();
            // Probe properties are also stored in consul with key starting with PROBE_KV_STORE_PREFIX:
            // Regular probe key stored in consul - "probes/73548994127888"
            // Probe property key stored in consul - "probes/73444795833808/probeproperties/monitoring.thread.timeout.sec"
            // Skip entries for probe properties.
            if (consulProbeKey.contains(PROBE_PROPERTY_PREFIX)) {
                logger.trace("Skipping entry for probe property " + consulProbeKey);
                continue;
            }
            final String probeId = consulProbeKey.substring(ProbeStore.PROBE_KV_STORE_PREFIX.length());
            JsonObject json = null;
            try {
                JsonReader reader = new JsonReader(new StringReader(entry.getValue()));
                json = new JsonParser().parse(reader).getAsJsonObject();
            } catch (RuntimeException e) {
                logger.error("probeId={} unable to read JSON!", probeId);
                continue;
            }
            JsonElement probeTypeElement = json.get("probeType");
            if (probeTypeElement == null) {
                continue;
            }
            final String probeType = probeTypeElement.getAsString();
            if (shouldUpdateProbe(probeId, probeType, json)) {
                logger.info("probeId={} probeType={} Starting probe migration.", probeId, probeType);
                probeIdToProbeTypeMap.put(probeId, probeType);
                updateProbe(probeId, probeType, json, consulProbeKey);
            }
        }
    }

    private void processTargets() {
        logger.info("Starting target migration.");
        // get the JSON for all the previously stored targets
        final Map<String, String> persistedTargets =
                keyValueStore.getByPrefix(TargetStore.TARGET_KV_STORE_PREFIX);
        for (Entry<String, String> entry : persistedTargets.entrySet()) {
            final String consulTargetKey = entry.getKey();
            // Target probe properties are also stored in consul with key starting with TARGET_KV_STORE_PREFIX:
            // Regular target key stored in consul - "targets/73732399126048"
            // Target probe property key stored in consul - "targets/73826873424496/probeproperties/executor.service.timeout.sec"
            // Skip entries for target properties.
            if (consulTargetKey.contains(PROBE_PROPERTY_PREFIX)) {
                logger.trace("Skipping entry for target probe property " + consulTargetKey);
                continue;
            }
            try {
                final JsonReader reader = new JsonReader(new StringReader(entry.getValue()));
                final JsonObject secretFields = new JsonParser().parse(reader).getAsJsonObject();
                final JsonObject targetJson = new JsonParser().parse(reader).getAsJsonObject();
                final JsonPrimitive targetInfo = targetJson.getAsJsonPrimitive(FIELD_TARGET_INFO);
                final JsonReader reader2 = new JsonReader(new StringReader(targetInfo.getAsString()));
                final JsonObject targetInfoObject = new JsonParser().parse(reader2).getAsJsonObject();
                // Process target and update consul
                processIndividualTarget(secretFields, targetInfoObject, consulTargetKey);
            } catch (RuntimeException e) {
                logger.error("Could not process target " + consulTargetKey, e);
                continue;
            }
        }
    }

    abstract String migrationPurpose();

    abstract boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson);

    abstract void updateProbe(String probeId, String probeType, JsonObject probeJson, String consulProbeKey);

    abstract void processIndividualTarget(JsonObject secretFields, JsonObject targetInfoObject, String consulTargetKey);
}
