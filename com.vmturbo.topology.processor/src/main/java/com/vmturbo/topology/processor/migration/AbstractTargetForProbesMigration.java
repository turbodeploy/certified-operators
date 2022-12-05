package com.vmturbo.topology.processor.migration;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Abstract class that handles generic logic to migrate target information discovered from certain probes.
 */
public abstract class AbstractTargetForProbesMigration extends AbstractProbeTargetMigration {

    protected final Set<SDKProbeType> scopedProbeTypes;

    /**
     * Create the migration object.
     *
     * @param keyValueStore consul
     */
    protected AbstractTargetForProbesMigration(@Nonnull KeyValueStore keyValueStore, @Nonnull Set<SDKProbeType> scopedProbeTypes) {
        super(keyValueStore);
        this.scopedProbeTypes = Objects.requireNonNull(scopedProbeTypes);
    }

    @Override
    boolean shouldUpdateProbe(String probeId, String probeType, JsonObject probeJson) {
        return inScope(probeType);
    }

    @Override
    void processIndividualTarget(JsonObject secretFields, JsonObject targetInfoObject, String consulTargetKey) {
        // Prechecks before processing the target information.
        final String targetId = targetInfoObject.get(TARGET_ID).getAsString();
        if (!targetInfoObject.has(TARGET_SPEC)) {
            logger.error("Target {} doesn't have spec", targetId);
            return;
        }

        final JsonObject spec = targetInfoObject.get(TARGET_SPEC).getAsJsonObject();
        final String probeId = spec.get(PROBE_ID).getAsString();
        final String probeType = probeIdToProbeTypeMap.get(probeId);

        // Check if target is scoped to the list of probe types provided
        if (!inScope(probeType)) {
            return;
        }

        // Only update the target info in the store if we decide that there is a change required.
        if (processTargetInfoDiscoveredByProbe(targetInfoObject)) {
            // Updating the consul key with the mutated target info object should refresh the target with
            // the changes
            final JsonObject targetInfo = new JsonObject();
            targetInfo.add(TARGET_INFO, new JsonPrimitive(targetInfoObject.toString()));
            keyValueStore.put(consulTargetKey, secretFields.toString() + targetInfo);
        }

    }

    /**
     * Determines if the specified probeType is within the scope of probe types that this migration
     * is responsible for.
     * @param probeType Probe type
     * @return whether the probe type is within scope.
     */
    boolean inScope(@Nonnull String probeType) {
        return scopedProbeTypes.stream().anyMatch(sdkProbeType ->  sdkProbeType.getProbeType().equals(probeType));
    }

    /**
     * Process the target information that was discovered by a subset of probe types.
     * @param targetInfoObject The target information.
     * @return Determines whether the target in question must be updated in the store.
     */
    abstract boolean processTargetInfoDiscoveredByProbe(JsonObject targetInfoObject);
}
