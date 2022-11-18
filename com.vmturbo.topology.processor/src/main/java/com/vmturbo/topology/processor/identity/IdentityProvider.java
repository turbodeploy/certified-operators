package com.vmturbo.topology.processor.identity;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityView;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityIdentifyingPropertyValues;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;

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
public interface IdentityProvider extends BinaryDiagsRestorable<Void>, RequiresDataInitialization {

    /**
     * Get the target ID for the target described by a given spec.
     *
     * @param targetSpec The spec describing the target.
     * @return The OID to use to identify the target.
     */
    long getTargetId(@Nonnull TargetSpec targetSpec);

    /**
     * Generate an ID for the probe registration given the probe info and the transport.
     *
     * @param probeInfo the probe info
     * @param transport the transport of the probe registration
     * @return The OID to use to identify the probe registration
     */
    long getProbeRegistrationId(@Nonnull ProbeInfo probeInfo,
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport);

    /**
     * Get the probe ID for the probe described by a probeInfo.
     *
     * @param probeInfo The object describing the probe.
     * @return The OID to use to identify the probe.
     * @throws IdentityProviderException If there was an error assigning the ID.
     */
    long getProbeId(@Nonnull ProbeInfo probeInfo) throws IdentityProviderException;

    /**
     * Expire the stale oids.
     * @return the number of expired oids
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the expiration oid thread was interrupted
     * @throws TimeoutException if the wait timed out
     */
    int expireOids() throws InterruptedException, ExecutionException, TimeoutException;

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
    long getCloneId(@Nonnull TopologyEntityView inputEntity);

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

    /**
     * Get all the oids that currently exist in the cache.
     * @return {@link LongSet} containing the oids
     * @throws IdentityUninitializedException If the identity service initialization is incomplete.
     */
    Set<Long> getCurrentOidsInIdentityCache() throws IdentityUninitializedException;

    /**
     * Returns the underlying store of the service.
     * @return the underlying store
     */
    IdentityServiceUnderlyingStore getStore();

    /**
     * Initialize a {@link StaleOidManagerImpl}.
     *
     * @param getCurrentOids supplier of oids contained in the entity store
     */
    void initializeStaleOidManager(@Nonnull Supplier<Set<Long>> getCurrentOids);

    /**
     *  Utility to return OID of an entity from its raw properties.
     *
     * @param identifyingProperties Collection that contains property name as key and a string as value.
     *                              Ie: <code>{id : 123, displayName: vm1, tag: myTag}</code>.
     * @param probeId               The OID to use to identify the probe.
     * @param entityType            The entityType to get the identifying properties for.
     * @return  The OID wrapped as an Optional<>, if a match in the cache is found; Optional.empty(), else
     * @throws IdentityServiceException                 If unable to fetch the entity property.
     * @throws IdentityServiceStoreOperationException   In the case of an error interacting with the underlying store.
     * @throws IdentityUninitializedException           If the identity service initialization is incomplete
     */
    Optional<Long> getOidFromProperties(Map<String, String> identifyingProperties, long probeId,
            EntityDTO.EntityType entityType)
            throws IdentityServiceException, IdentityServiceStoreOperationException,
            IdentityUninitializedException;

    /**
     * Get or generate OIDs for provided identifyingPropertyValues.
     *
     * @param probeId from which identities are discovered.
     * @param identifyingPropertyValues for which OIDs are to be retrieved or generated.
     * @return A map from the assigned OID to the provided EntityIdentifyingPropertyValues instance.
     * @throws IdentityServiceException if unable to fetch or persist the generated entity identities.
     */
    Map<Long, EntityIdentifyingPropertyValues> getIdsFromIdentifyingPropertiesValues(
        long probeId, List<EntityIdentifyingPropertyValues> identifyingPropertyValues)
        throws IdentityServiceException;

    /**
     * Restore Identity from string list based diags.
     * This guarantee backward compatibility for String-extended diagnosable .
     *
     * @param diagsLines    list of Strings (lines) from Identity.diags file
     * @param context       @Nullable context
     */
    void restoreStringDiags(List<String> diagsLines, @Nullable Void context);
}
