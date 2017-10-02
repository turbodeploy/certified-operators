package com.vmturbo.topology.processor.api;

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Information about an entity discovered by the {@link TopologyProcessor}.
 */
public interface EntityInfo {
    /**
     * Get the OID of the entity.
     *
     * @return The OID of the entity.
     */
    long getEntityId();

    /**
     * Return whether the entity is present.
     *
     * @return True if the entity exists in the topology processor, false otherwise.
     */
    boolean found();

    /**
     * Return the OIDs of the targets that have discovered this entity,
     * and the OIDs of the probes associated with those targets.
     *
     * @return Map of targetId -> probeId for each target that discovered this entity.
     */
    @Nonnull
    Map<Long, Long> getTargetIdToProbeId();
}
