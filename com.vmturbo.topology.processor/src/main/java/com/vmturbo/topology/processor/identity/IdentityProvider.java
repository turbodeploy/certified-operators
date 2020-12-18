package com.vmturbo.topology.processor.identity;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;

/**
 * The Identity Provider is responsible for management of OID's for all
 * relevant objects in the Topology processor. The identity provider
 * implementation decides what properties of object descriptors to use
 * to determine equivalence.
 *
 * <p>The most important function is OID assignment for entities. The OIDs
 * assigned here propagate throughout the system, and the Identity Provider
 * is the source of truth for entity identity information.
 *
 * <p>ID assignment for targets, probes, discoveries, actions etc. also
 * happens here.
 */
public interface IdentityProvider extends DiagsRestorable<Void> {

    /**
     * Get the target ID for the target described by a given spec.
     *
     * @param targetSpec The spec describing the target.
     * @return The OID to use to identify the target.
     */
    long getTargetId(@Nonnull TargetSpec targetSpec);

    /**
     * Get the probe ID for the probe described by a probeInfo.
     *
     * @param probeInfo The object describing the probe.
     * @return The OID to use to identify the probe.
     * @throws IdentityProviderException If there was an error assigning the ID.
     */
    long getProbeId(@Nonnull ProbeInfo probeInfo) throws IdentityProviderException;

    /**
     * Get the entity ID for the entities discovered by a specific probe
     * and described by the given DTO.
     *
     * @param probeId The Id of the probe that discovered the entity.
     * @param entityDTOs A list of the descriptors of the entities' properties.
     * @return A map from the assigned OID to the descriptor it was assigned to. Not all entities
     *         in the input are guaranteed to have an ID - for example, if the probe that discovered
     *         an entity didn't provide identity metadata for the entity type, the entity won't get
     *         an ID.
     * @throws IdentityServiceException if unable to fetch or persist the generated entity
     *         identities.
     */
    Map<Long, EntityDTO> getIdsForEntities(long probeId, @Nonnull List<EntityDTO> entityDTOs)
            throws IdentityServiceException;

    /**
     * Get an entity ID for a clone of an entity in the topology.
     *
     * @param inputEntity The entity to clone.
     * @return The OID to use to identify the clone.
     */
    long getCloneId(@Nonnull TopologyEntityDTOOrBuilder inputEntity);

    /**
     * Generate an ID for a new operation.
     *
     * @return The OID to use to identify the operation.
     */
    long generateOperationId();

    /**
     * Generate an ID for a new topology for topologies broadcast to the reset of
     * the services in the system.
     *
     * @return The OID to use to identify a topology.
     */
    long generateTopologyId();

    /**
     *  Update the probeInfo with the new value.
     *
     * @param probeInfo The object describing the probe.
     */
    void updateProbeInfo(ProbeInfo probeInfo);

    /**
     * Makes the current thread wait until the store gets initialized or a timeout occurs.
     * @throws InterruptedException if any thread interrupted the current thread before
     * or while the current thread was waiting for a notification.
     */
     void waitForInitializedStore() throws InterruptedException;
}
