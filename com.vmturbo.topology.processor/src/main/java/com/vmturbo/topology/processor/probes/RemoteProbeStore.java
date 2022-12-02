package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.HealthCheck.HealthState;

import com.vmturbo.clustermgr.api.ClusterMgrClient;
import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.probes.ProbeVersionFactory.ProbeVersionCompareResult;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Store for remote probe information. It publishes several convenient methods to operate with
 * probes.
 */
@ThreadSafe
public class RemoteProbeStore implements ProbeStore {

    @GuardedBy("dataLock")
    private final KeyValueStore keyValueStore;

    private final Logger logger = LogManager.getLogger();
    /**
     * Lock to use for accessing all the probe information. Most of actions are performed as
     * double-staged, writing or reading into multiple maps. This lock is used to make the
     * operations atomic.
     */
    private final Object dataLock = new Object();
    /**
     * Multimap of probe id -> transport.
     */
    @GuardedBy("dataLock")
    private final Multimap<Long, ITransport<MediationServerMessage, MediationClientMessage>> probes =
                    HashMultimap.create();
    /**
     * Map of registered probe types. Only probes, which have active transports are stored in this
     * map.
     */
    @GuardedBy("dataLock")
    private final Map<Long, ProbeInfo> probeInfos;

    /**
     * List of registered listeners to ProbeStore events.
     */
    @GuardedBy("dataLock")
    private final List<ProbeStoreListener> listeners = new ArrayList<>();

    private final IdentityProvider identityProvider_;

    private final StitchingOperationStore stitchingOperationStore;

    private final ProbeOrdering probeOrdering;

    private final Map<ITransport<MediationServerMessage, MediationClientMessage>, String>
            transportToChannel = new ConcurrentHashMap<>();

    private final Map<ITransport<MediationServerMessage, MediationClientMessage>,
            Collection<ProbeRegistrationDescription>> transportToProbeRegistrations = new ConcurrentHashMap<>();

    /**
     * The action merge policy/specs conversion repository.
     */
    private final ActionMergeSpecsRepository actionMergeSpecsRepository;

    public RemoteProbeStore(@Nonnull final KeyValueStore keyValueStore,
                            @Nonnull IdentityProvider identityProvider,
                            @Nonnull final StitchingOperationStore stitchingOperationStore,
                            final @Nonnull ActionMergeSpecsRepository actionSpecsRepository) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        identityProvider_ = Objects.requireNonNull(identityProvider);
        this.stitchingOperationStore = Objects.requireNonNull(stitchingOperationStore);
        this.actionMergeSpecsRepository = actionSpecsRepository;

        this.probeInfos = new HashMap<>();
        this.probeOrdering = new StandardProbeOrdering(this);
    }

    @Override
    public void initialize() {
        final MutableInt probesLoaded = new MutableInt(0);
        final MutableInt duplicatesLoaded = new MutableInt(0);

        synchronized (dataLock) {
            this.probeInfos.clear();
            // Load ProbeInfo persisted in Consul.
            Map<String, String> persistedProbeInfos = this.keyValueStore.getByPrefix(PROBE_KV_STORE_PREFIX);
            persistedProbeInfos.values().stream()
                .map(probeInfoJson -> {
                    try {
                        final ProbeInfo.Builder probeInfoBuilder = ProbeInfo.newBuilder();
                        JsonFormat.parser().merge(probeInfoJson, probeInfoBuilder);
                        return probeInfoBuilder.build();
                    } catch (InvalidProtocolBufferException e) {
                        logger.error("Failed to load probe info from Consul: {}", probeInfoJson, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .forEach(probeInfo -> {
                    final long probeId;
                    try {
                        probeId = identityProvider_.getProbeId(probeInfo);
                    } catch (IdentityProviderException e) {
                        logger.error("Failed to assign ID to saved probe info. Error: {}", e.getMessage());
                        return;
                    }
                    final ProbeInfo existing = this.probeInfos.putIfAbsent(probeId, probeInfo);
                    if (existing != null) {
                        logger.error("Probe with same id ({}) loaded more than once from KV store!" +
                            " Keeping first. Dropping: {}", probeId, probeInfo);
                        duplicatesLoaded.increment();
                    } else {
                        probesLoaded.increment();
                    }
                });

            // set stitching operations when restoring probes so stitching will happen even if
            stitchingOperationStore.clearOperations();
            probeInfos.forEach((probeId, probeInfo) -> {
                try {
                    stitchingOperationStore.setOperationsForProbe(probeId, probeInfo, probeOrdering);
                } catch (ProbeException e) {
                    logger.error("Failed to create stitching operations for probe {} due to {}", probeId, e);
                }
            });

            // Handle action merge data for the restored probes.
            actionMergeSpecsRepository.clear();
            probeInfos.forEach(actionMergeSpecsRepository::setPoliciesForProbe);
        }

        logger.info("Loaded info for {} probes successfully and discarded info for {} duplicates.",
            probesLoaded.getValue(), duplicatesLoaded.getValue());
    }

    @Override
    public int priority() {
        // Initialize before the identity store, so that the identity store can access the
        // probes metadata
        return identityProvider_.priority() + 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean registerNewProbe(@Nonnull ProbeInfo probeInfo,
            @Nonnull ContainerInfo containerInfo,
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport)
            throws ProbeException {
        Objects.requireNonNull(probeInfo, "Probe info should not be null");
        Objects.requireNonNull(containerInfo, "Container info should not be null");
        Objects.requireNonNull(transport, "Transport should not be null");

        boolean probeExists;
        long probeId;
        synchronized (dataLock) {
            try {
                probeId = identityProvider_.getProbeId(probeInfo);
            } catch (IdentityProviderException e) {
                throw new ProbeException(
                        "Failed to get ID for probe configuration received from " + transport, e);
            }
            // If we successfully got an ID it means we passed the compatibility check.
            logger.debug("Adding endpoint to probe type map: " + transport + " "
                    + probeInfo.getProbeType());
            probeExists = probes.containsKey(probeId);
            // Update probe info in consul as well as updating stitching operations and action
            // merge policy depending on the version of the connecting probe
            updateProbeInfoIfNeeded(probeId, probeInfo);
            probes.put(probeId, transport);
            // Store the probe registration
            final long probeRegistrationId = identityProvider_.getProbeRegistrationId(probeInfo, transport);
            final ProbeRegistrationDescription probeRegistration = buildProbeRegistrationDescription(
                    probeRegistrationId, probeId, probeInfo, containerInfo);
            final Collection<ProbeRegistrationDescription> probeRegistrations
                    = transportToProbeRegistrations.getOrDefault(transport, new ArrayList<>());
            probeRegistrations.add(probeRegistration);
            transportToProbeRegistrations.put(transport, probeRegistrations);
            if (containerInfo.hasCommunicationBindingChannel()) {
                updateTransportByChannel(containerInfo.getCommunicationBindingChannel(), transport);
            }

            if (probeExists) {
                logger.info("Connected probe " + probeId + " type=" + probeInfo.getProbeType()
                        + " category=" + probeInfo.getProbeCategory());
            } else {
                logger.info("Registered new probe " + probeId + " type=" + probeInfo.getProbeType()
                        + " category=" + probeInfo.getProbeCategory());
            }
        }
        // notify listeners - to avoid deadlocks, this is done without holding a lock
        listeners.forEach(listener -> listener.onProbeRegistered(probeId, probeInfo));

        return probeExists;
    }

    /**
     * Update {@link ProbeInfo} in Consul, and also update stitching operations and action merge
     * policies. This is done if:
     * - the probe of the same type does not exist in the probeInfos map, or
     * - the comparison between the version of the existing probe and the connecting probe cannot
     *   be made, or
     * - the version of the existing probe is no greater than that of the connecting probe,
     * The purpose of this logic is to make the probe info update operation deterministic for
     * multiple remote probes that have the same probe type but different versions. We want to make
     * sure that the registered probe info in Consul as well as the stitching operations and action
     * merge policies of a particular probe type is always the newest. Currently, this only applies
     * to Kubernetes probes.
     *
     * @param probeId the probe ID
     * @param probeInfo the {@link ProbeInfo} of the connecting probe
     * @throws ProbeException probe exception
     */
    @GuardedBy("dataLock")
    private void updateProbeInfoIfNeeded(final long probeId,
                                         @Nonnull final ProbeInfo probeInfo)
            throws ProbeException {
        if (probeInfos.containsKey(probeId)
                && ProbeVersionFactory.compareProbeVersion(probeInfos.get(probeId).getVersion(),
                        probeInfo.getVersion()).equals(ProbeVersionCompareResult.GREATER)) {
            logger.info("The probe with type {} is already registered. It has a version [{}] "
                                + "which is newer than the connecting probe's version [{}]. "
                                + "Skip updating the probe info from the connecting probe.",
                        probeInfo.getProbeType(), probeInfos.get(probeId).getVersion(),
                        probeInfo.getVersion());
            return;
        }
        try {
            // Store the probeInfo in Consul
            storeProbeInfo(probeId, probeInfo);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Invalid probeInfo {}", probeInfo);
            throw new ProbeException(
                    "Failed to persist probe info in Consul. Probe ID: " + probeId);
        }
        // Update the stitching operations store
        stitchingOperationStore.setOperationsForProbe(probeId, probeInfo, probeOrdering);
        // Save the action merge policies
        actionMergeSpecsRepository.setPoliciesForProbe(probeId, probeInfo);
        // Update the probeInfos map
        probeInfos.put(probeId, probeInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void overwriteProbeInfo(@Nonnull final Map<Long, ProbeInfo> probeInfoMap) {
        logger.info("Storing probe info");
        synchronized (dataLock) {
            probeInfos.clear();
            probeInfos.putAll(probeInfoMap);
            // Keep Consul in sync with the internal cache
            keyValueStore.removeKeysWithPrefix(PROBE_KV_STORE_PREFIX);
            probeInfos.forEach((probeId, probeInfo) -> {
                try {
                    storeProbeInfo(probeId, probeInfo);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException("Failed to restore an invalid probeInfo: "
                        + probeInfo, e);
                }
            });
            // set stitching operations when loading diags so stitching will happen
            stitchingOperationStore.clearOperations();
            probeInfos.forEach((probeId, probeInfo) -> {
                try {
                    stitchingOperationStore.setOperationsForProbe(probeId, probeInfo, probeOrdering);
                } catch (ProbeException e) {
                    logger.error("Failed to create stitching operations for probe {} due to {}", probeId, e);
                }
            });

            // Handle action merge data for the restored probes.
            actionMergeSpecsRepository.clear();
            probeInfoMap.forEach(actionMergeSpecsRepository::setPoliciesForProbe);
        }
        logger.info("Stored info from {} probes", probeInfos.size());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransport(
                    long probeId) throws ProbeException {
        synchronized (dataLock) {
            if (!probes.containsKey(probeId)) {
                throw new ProbeException(noTransportMessage(probeId));
            }
            return ImmutableList.copyOf(probes.get(probeId));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransportsForTarget(
            @Nonnull Target target) throws ProbeException {
        final long probeId = target.getProbeId();
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>> transports =
                getTransport(probeId);
        if (target.getSpec() == null || !target.getSpec().hasCommunicationBindingChannel()) {
            return transports;
        }
        final String communicationBindingChannel =
                target.getSpec().getCommunicationBindingChannel();
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>>
                filteredTransports = transports.stream()
                .filter(t -> communicationBindingChannel.equals(transportToChannel.get(t)))
                .collect(Collectors.toList());
        if (filteredTransports.size() == 0) {
            throw new ProbeException(noTransportMessage(probeId, communicationBindingChannel));
        }
        return filteredTransports;
    }

    /**
     * Associate the transport with the given communication binding channel.
     *
     * @param communicationBindingChannel the communication binding channel
     * @param transport the associated transport
     */
    private void updateTransportByChannel(@Nonnull final String communicationBindingChannel,
            @Nonnull final ITransport<MediationServerMessage, MediationClientMessage> transport) {
        transportToChannel.put(transport, communicationBindingChannel);
    }

    @Override
    public Optional<String> getChannel(final ITransport<MediationServerMessage, MediationClientMessage> transport) {
        return Optional.ofNullable(transportToChannel.get(transport));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTransport(ITransport<MediationServerMessage, MediationClientMessage> transport) {
        synchronized (dataLock) {
            transportToProbeRegistrations.remove(transport);
            final Iterator<Entry<Long, ITransport<MediationServerMessage, MediationClientMessage>>> iterator =
                            probes.entries().iterator();
            while (iterator.hasNext()) {
                final Entry<Long, ITransport<MediationServerMessage, MediationClientMessage>> entry =
                                iterator.next();
                final Long probeId = entry.getKey();
                if (entry.getValue().equals(transport)) {
                    iterator.remove();
                    if (!probes.containsKey(probeId)) {
                        // The ProbeInfo object that corresponds to the probeId is not removed
                        // from probeInfos map because there can be targets configured for this
                        // probe type, and information such as probe category and type need to be
                        // cached. When the probe re-connects, ProbeInfo value will be refreshed.

                        stitchingOperationStore.removeOperationsForProbe(probeId);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<ProbeInfo> getProbe(long probeId) {
        synchronized (dataLock) {
            return Optional.ofNullable(probeInfos.get(probeId));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> getProbeIdForType(@Nonnull final String probeTypeName) {
        Objects.requireNonNull(probeTypeName);

        synchronized (dataLock) {
            return probeInfos.entrySet().stream()
                .filter(entry -> entry.getValue().getProbeType().equals(probeTypeName))
                .map(Entry::getKey)
                .findFirst();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public List<Long> getProbeIdsForCategory(@Nonnull final ProbeCategory probeCategory) {
        Objects.requireNonNull(probeCategory);

        synchronized (dataLock) {
            return probeInfos.entrySet().stream()
                .filter(entry -> entry.getValue().getProbeCategory()
                        .equalsIgnoreCase(probeCategory.getCategory()))
                .map(Entry::getKey)
                .collect(Collectors.toList());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, ProbeInfo> getProbes() {
        synchronized (dataLock) {
            return ImmutableMap.copyOf(probeInfos);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<ProbeInfo> getProbeInfoForType(@Nonnull final String probeTypeName) {

        synchronized (dataLock) {
            return probeInfos.entrySet().stream()
                    .filter(entry -> entry.getValue().getProbeType().equals(probeTypeName))
                    .map(Entry::getValue)
                    .findFirst();
        }
    }

    /**
     * {@inheritDoc}
     * This method does a blind update and doesn't do any compatability check
     * with the old one and the new probeInfos.
     */
    @Override
    public void updateProbeInfo(@Nonnull ProbeInfo probeInfo)
        throws ProbeException {

        Objects.requireNonNull(probeInfo, "probeInfo cannot be null");
        Optional<Long> probeIdOpt = getProbeIdForType(probeInfo.getProbeType());
        if (!probeIdOpt.isPresent()) {
            logger.warn("Trying to update a non-existing probeInfo: {}", probeInfo);
            return;
        }
        final long probeId = probeIdOpt.get();

        synchronized (dataLock) {

            try {
                // Store the probeInfo in Consul
                storeProbeInfo(probeId, probeInfo);
            } catch (InvalidProtocolBufferException e) {
                logger.error("Invalid probeInfo {}", probeInfo);
                throw new ProbeException("Failed to persist probe info in Consul. Probe ID: " + probeId);
            }

            // Cache the probeInfo in memory
            probeInfos.put(probeId, probeInfo);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Collection<ProbeRegistrationDescription> getAllProbeRegistrations() {
        return transportToProbeRegistrations.values().stream().flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Nonnull
    @Override
    public Collection<ProbeRegistrationDescription> getProbeRegistrationsForTarget(
            @Nonnull Target target) {
        try {
            return getTransportsForTarget(target).stream()
                    .flatMap(transport -> transportToProbeRegistrations.getOrDefault(transport, Collections.emptyList()).stream())
                    .collect(Collectors.toList());
        } catch (ProbeException e) {
            return Collections.emptyList();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<ProbeRegistrationDescription> getProbeRegistrationById(final long id) {
        return transportToProbeRegistrations.values().stream().flatMap(Collection::stream)
                .filter(i -> i.getId() == id).findAny();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(@Nonnull ProbeStoreListener listener) {
        synchronized (dataLock) {
            listeners.add(Objects.requireNonNull(listener));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeListener(@Nonnull ProbeStoreListener listener) {
        synchronized (dataLock) {
            return listeners.remove(Objects.requireNonNull(listener));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isProbeConnected(@Nonnull final Long probeId) {
        return probes.containsKey(probeId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isAnyTransportConnectedForTarget(@Nonnull Target target) {
        try {
            return getTransportsForTarget(target).size() > 0;
        } catch (ProbeException e) {
            return false;
        }
    }

    @Override
    public ProbeOrdering getProbeOrdering() {
        return probeOrdering;
    }

    private void storeProbeInfo(final Long probeId, final ProbeInfo probeInfo)
        throws InvalidProtocolBufferException {
        // Store the probeInfo in consul, using the probeId as the key
        keyValueStore.put(PROBE_KV_STORE_PREFIX + probeId, JsonFormat.printer().print(probeInfo));
    }

    /**
     * Constructs a {@link ProbeRegistrationDescription} given the list of inputs.
     *
     * @param id the id of the probe registration
     * @param probeId the id of the probe type
     * @param probeInfo the info about this probe
     * @param containerInfo the info about the mediation container
     */
    private ProbeRegistrationDescription buildProbeRegistrationDescription(
            final long id, final long probeId, @Nonnull final ProbeInfo probeInfo,
            @Nonnull ContainerInfo containerInfo) {
        final String communicationBindingChannel = Objects.requireNonNull(containerInfo).getCommunicationBindingChannel();
        final String probeVersion = Objects.requireNonNull(probeInfo).getVersion();
        final long registeredTime = System.currentTimeMillis();
        final String displayName;
        if (Strings.isNullOrEmpty(probeInfo.getDisplayName())) {
            if (Strings.isNullOrEmpty(communicationBindingChannel)) {
                displayName = probeInfo.getProbeType() + " Probe " + id;
            } else {
                displayName = probeInfo.getProbeType() + " Probe " + communicationBindingChannel;
            }
        } else {
            displayName = probeInfo.getDisplayName();
        }
        final String platformVersion = keyValueStore.get(ClusterMgrClient.COMPONENT_VERSION_KEY, "");
        final Pair<HealthState, String> health = ProbeVersionFactory.deduceProbeHealth(probeVersion, platformVersion, probeInfo);

        return new ProbeRegistrationDescription(id, probeId, communicationBindingChannel,
                probeVersion, registeredTime, displayName, health.getFirst(), health.getSecond());
    }
}
