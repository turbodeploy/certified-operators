package com.vmturbo.topology.processor.probes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Interface for registering probes and the transports used to talk to them.
 */
public interface ProbeStore extends RequiresDataInitialization {
    /**
     * Key prefix of values stored in the key/value store.
     */
    String PROBE_KV_STORE_PREFIX = "probes/";

    /**
     * Normalized messages for when no valid transports are available.
     */
    String NO_TRANSPORTS_MESSAGE = "Failed to connect to probe. Check if probe is running";

    /**
     * Return a no transports message containing the probe id.
     *
     * @param probeId the id of the probe
     * @return the constructed message
     */
    default String noTransportMessage(final long probeId) {
        return String.format(NO_TRANSPORTS_MESSAGE + " (probe id %s)", probeId);
    }

    /**
     * Return a no transports message containing both the probe id and the communication binding channel.
     *
     * @param probeId the id of the probe
     * @param channel the communication binding channel
     * @return the constructed message
     */
    default String noTransportMessage(final long probeId, final String channel) {
        return String.format(NO_TRANSPORTS_MESSAGE + " (probe id %s, channel %s)", probeId, channel);
    }

    /**
     * Method registers a new probe with a specified transport.
     *
     * @param probeInfo Probe to register
     * @param containerInfo Info of the corresponding mediation container
     * @param transport Transport to use for this probe
     * @return Whether a new probe has been registered (<code>true</code>) or this is just a new
     *         transport for existing probe (<code>false</code>).
     * @throws ProbeException If new probe info differs from the existing probe info
     * @throws NullPointerException If <code>probeInfo</code> is null
     */
    boolean registerNewProbe(@Nonnull ProbeInfo probeInfo,
                             @Nonnull ContainerInfo containerInfo,
                             @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport)
            throws ProbeException;

    /**
     * Retrieve the transports associated with a particular probe. You can use each transport
     * to communicate with the associated instance of that probe.
     *
     * @param probeId Id of the probe to get transports for.
     * @return A collection of registered transports, each associated with an instance of that probe.
     * @throws ProbeException If probeId doesn't exist.
     */
    Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransport(
            long probeId) throws ProbeException;

    /**
     * Fetch and return all transports that could be used by the given target, taking into account
     * of the communication binding channel, if one is present.  If no matched transports exist,
     * then an empty set is returned.
     *
     * @param target the target
     * @return the set of transports that can be used for the given target
     */
    @Nonnull
    Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransportsForTarget(
            @Nonnull Target target) throws ProbeException;

    /**
     * Remove the transport (i.e. disconnect an instance of the probe). If a probe has no more
     * associated transports it's considered disconnected.
     *
     * @param transport The transport to remove.
     */
    void removeTransport(ITransport<MediationServerMessage, MediationClientMessage> transport);

    /**
     * Retrieve probe information by ID.
     *
     * @param probeId Id of the probe to retrieve.
     * @return The information of the probe, or an empty Optional.
     */
    Optional<ProbeInfo> getProbe(long probeId);

    /**
     * Retrieve probe id by probe type name.
     *
     * @param probeTypeName Name of the probe to retrieve.
     * @return The ID of the probe with the given type, or an empty Optional if no probe
     *         with the given type is registered.
     */
    Optional<Long> getProbeIdForType(@Nonnull final String probeTypeName);

    /**
     * Retrieve probe info by probe type name.
     *
     * @param probeTypeName Name of the probe to retrieve.
     * @return The ID of the probe with the given type, or an empty Optional if no probe
     *         with the given type is registered.
     */
    Optional<ProbeInfo> getProbeInfoForType(@Nonnull final String probeTypeName);


    /**
     * Retrieve probe ids for a probe category.
     *
     * @param probeCategory the category of the probe whose ids should be retrieved.
     * @return The IDs of the registered probes belonging to the given category.
     */
    @Nonnull
    List<Long> getProbeIdsForCategory(@Nonnull final ProbeCategory probeCategory);

    /**
     * Retrieves the information of all probes that have been registered, but they may not
     * be currently connected. (i.e. the mediation component of the probe may be stopped.)
     *
     * @return The map of probeId -> {@link ProbeInfo} provided by the probe at registration.
     */
    Map<Long, ProbeInfo> getProbes();

    /**
     * Updates stored information about probes with a different map of information about probes.
     * The old map is cleared and only the entries from the new map are present.
     * Any probes already registered remain registered, but no additional probes are registered.
     *
     * This method is used for restoring probe info from diags, and should not be used in
     * normal operations.
     *
     * @param probeInfoMap the new probe information to store
     */
    void overwriteProbeInfo(@Nonnull final Map<Long, ProbeInfo> probeInfoMap);

    /**
     * Add a listener for probe store events.
     *
     * @param listener The listener to add.
     */
    void addListener(@Nonnull ProbeStoreListener listener);

    /**
     * Remove a listener for probe store events.
     *
     * @param listener The listener to remove.
     * @return True if the listener was successfully removed, false otherwise.
     */
    boolean removeListener(@Nonnull ProbeStoreListener listener);

    /**
     * Returns a boolean to indicate whether any transport with the given probe ID is connected.
     * Note that this does not take the communication binding channel into consideration.  All
     * connected transports of the given probe id will be counted as true.
     *
     * @param probeId Probe ID
     * @return true if probe is connected, false otherwise.
     */
    boolean isProbeConnected(@Nonnull Long probeId);

    /**
     * Returns a boolean to indicate whether the probe for the given target has any transport
     * connected.  If the target has an associated communication binding channel, then that will be
     * taken into consideration to match the transports.
     *
     * @param target the target
     * @return true if there is any matched transport is connected, false otherwise.
     */
    boolean isAnyTransportConnectedForTarget(@Nonnull Target target);

    /**
     * Get the probe ordering needed for stitching
     * @return {@link ProbeOrdering} for this ProbeStore
     */
    ProbeOrdering getProbeOrdering();

    /**
     * Update the existing probe with the new probe info.
     * @param updatedProbeInfo New probeInfo for the probe.
     *
     */
    void updateProbeInfo(@Nonnull ProbeInfo updatedProbeInfo) throws ProbeException;

    /**
     * Returns all probe registrations.
     *
     * @return all probe registrations.
     */
    @Nonnull
    Collection<ProbeRegistrationDescription> getAllProbeRegistrations();

    /**
     * Returns probe registrations relevant to the given target.
     *
     * @return the probe registrations relevant to the target.
     */
    @Nonnull
    Collection<ProbeRegistrationDescription> getProbeRegistrationsForTarget(@Nonnull Target target);

    /**
     * Returns the probe registration by id wrapped by Optional.  If no probe registration exists
     * with the id, an Optional.empty() is returned.s
     *
     * @param id the id of the probe registration
     * @return the found probe registration wrapped by Optional; or Optional.empty() if not found.
     */
    @Nonnull
    Optional<ProbeRegistrationDescription> getProbeRegistrationById(final long id);
}
