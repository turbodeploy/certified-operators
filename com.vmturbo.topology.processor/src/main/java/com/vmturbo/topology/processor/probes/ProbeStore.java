package com.vmturbo.topology.processor.probes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;

/**
 * Interface for registering probes and the transports used to talk to them.
 */
public interface ProbeStore extends RequiresDataInitialization {
    /**
     * Key prefix of values stored in the key/value store.
     */
    String PROBE_KV_STORE_PREFIX = "probes/";

    /**
     * Method registers a new probe with a specified transport.
     *
     * @param probeInfo Probe to register
     * @param transport Transport to use for this probe
     * @return Whether a new probe has been registered (<code>true</code>) or this is just a new
     *         transport for existing probe (<code>false</code>).
     * @throws ProbeException If new probe info differs from the existing probe info
     * @throws NullPointerException If <code>probeInfo</code> is null
     */
    boolean registerNewProbe(@Nonnull ProbeInfo probeInfo,
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
     * Returns a boolean to indicate whether the probe with the given probe ID is connected.
     *
     * @param probeId Probe ID
     * @return true if probe is connected, false otherwise.
     */
    boolean isProbeConnected(@Nonnull Long probeId);

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
}
