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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
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

    private final Map<String, Collection<ITransport<MediationServerMessage,
            MediationClientMessage>>> channelToTransport = new ConcurrentHashMap<>();

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
        logger.debug("initialize");
        this.probeInfos.clear();
        // Load ProbeInfo persisted in Consul.
        Map<String, String> persistedProbeInfos = this.keyValueStore.getByPrefix(PROBE_KV_STORE_PREFIX);
        persistedProbeInfos.values().stream()
            .map(probeInfoJson -> {
                try {
                    final ProbeInfo.Builder probeInfoBuilder = ProbeInfo.newBuilder();
                    JsonFormat.parser().merge(probeInfoJson, probeInfoBuilder);
                    return probeInfoBuilder.build();
                } catch (InvalidProtocolBufferException e){
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
                }
            });
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
                    @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport)
                    throws ProbeException {
        Objects.requireNonNull(probeInfo, "Probe info should not be null");
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

            logger.debug("Adding endpoint to probe type map: " + transport + " "
                    + probeInfo.getProbeType());
            probeExists = probes.containsKey(probeId);
            try {
                // Store the probeInfo in Consul
                storeProbeInfo(probeId, probeInfo);
            } catch (InvalidProtocolBufferException e) {
                logger.error("Invalid probeInfo {}", probeInfo);
                throw new ProbeException(
                        "Failed to persist probe info in Consul. Probe ID: " + probeId);
            }

            try {
                keyValueStore.put(PROBE_KV_STORE_PREFIX + Long.toString(probeId),
                        JsonFormat.printer().print(probeInfo));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Invalid probeInfo {}", probeInfo);
                throw new ProbeException(
                        "Failed to persist probe info in Consul. Probe ID: " + probeId);
            }

            probes.put(probeId, transport);
            // If we successfully got an ID it means we passed the compatibility check.
            probeInfos.put(probeId, probeInfo);
            stitchingOperationStore.setOperationsForProbe(probeId, probeInfo, probeOrdering);
            // Save the action merge policies
            actionMergeSpecsRepository.setPoliciesForProbe(probeId, probeInfo);

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
            return probes.get(probeId);
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
                transportsWithAssignedChannel = channelToTransport.getOrDefault(
                communicationBindingChannel, Collections.emptyList());
        final Collection<ITransport<MediationServerMessage, MediationClientMessage>>
                filteredTransports = transports.stream().filter(
                transportsWithAssignedChannel::contains).collect(Collectors.toList());
        if (filteredTransports.size() == 0) {
            throw new ProbeException(noTransportMessage(probeId, communicationBindingChannel));
        }
        return filteredTransports;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTransportByChannel(@Nonnull final String communicationBindingChannel,
            @Nonnull final ITransport<MediationServerMessage, MediationClientMessage> transport) {
        channelToTransport.computeIfAbsent(communicationBindingChannel, x -> new ArrayList()).add(transport);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTransport(ITransport<MediationServerMessage, MediationClientMessage> transport) {
        synchronized (dataLock) {
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
}
