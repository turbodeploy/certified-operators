package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.communication.ITransport;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;

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

    private final ProbeInfoCompatibilityChecker compatibilityChecker;

    private final ProbeOrdering probeOrdering;

    public RemoteProbeStore(@Nonnull final KeyValueStore keyValueStore,
                            @Nonnull IdentityProvider identityProvider,
                            @Nonnull final StitchingOperationStore stitchingOperationStore,
                            @Nonnull final ProbeInfoCompatibilityChecker compatibilityChecker) {
        this.keyValueStore = Objects.requireNonNull(keyValueStore);
        identityProvider_ = Objects.requireNonNull(identityProvider);
        this.stitchingOperationStore = Objects.requireNonNull(stitchingOperationStore);
        this.compatibilityChecker = Objects.requireNonNull(compatibilityChecker);

        // Load ProbeInfo persisted in Consul.
        Map<String, String> persistedProbeInfos = this.keyValueStore.getByPrefix(PROBE_KV_STORE_PREFIX);

        this.probeInfos = persistedProbeInfos.values().stream()
                .map(probeInfoJson -> {
                    try {
                        final ProbeInfo.Builder probeInfoBuilder = ProbeInfo.newBuilder();
                        JsonFormat.parser().merge(probeInfoJson, probeInfoBuilder);
                        return probeInfoBuilder.build();
                    } catch (InvalidProtocolBufferException e){
                        logger.error("Failed to load probe info from Consul.");
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toConcurrentMap(
                    identityProvider_::getProbeId, Function.identity()));
        this.probeOrdering = new StandardProbeOrdering(this);
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

        synchronized (dataLock) {
            long probeId = identityProvider_.getProbeId(probeInfo);
            final ProbeInfo existing = probeInfos.get(probeId);
            if (existing != null && !compatibilityChecker.areCompatible(existing, probeInfo)) {
                throw new ProbeException("Probe configuration " + probeInfo
                                + ", received from " + transport + " is incompatible "
                                + "with already registered probe with the same probe type: "
                                + existing);
            } else {
                logger.debug("Adding endpoint to probe type map: " + transport + " " + probeInfo.getProbeType());
                final boolean probeExists = probes.containsKey(probeId);

                try {
                    keyValueStore.put(PROBE_KV_STORE_PREFIX + Long.toString(probeId),
                            JsonFormat.printer().print(probeInfo));
                } catch (InvalidProtocolBufferException e) {
                    logger.error("Invalid probeInfo {}", probeInfo);
                    throw new ProbeException("Failed to persist probe info in Consul. Probe ID: " + probeId);
                }

                probes.put(probeId, transport);
                // If we passed the compatibility check, it is safe to replace the old registration
                // information in the store.
                probeInfos.put(probeId, probeInfo);
                stitchingOperationStore.setOperationsForProbe(probeId, probeInfo, probeOrdering);

                if (probeExists) {
                    logger.info("Connected probe " + probeId +
                            " type=" + probeInfo.getProbeType() +
                            " category=" + probeInfo.getProbeCategory()
                    );
                } else {
                    logger.info("Registered new probe " + probeId +
                            " type=" + probeInfo.getProbeType() +
                            " category=" + probeInfo.getProbeCategory()
                    );
                }

                // notify listeners
                listeners.forEach(listener -> listener.onProbeRegistered(probeId, probeInfo));

                return probeExists;
            }
        }
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
            // set stitching operations when loading diags so stitching will happen
            stitchingOperationStore.clearOperations();
            probeInfos.forEach((probeId, probeInfo) -> {
                try {
                    stitchingOperationStore.setOperationsForProbe(probeId, probeInfo, probeOrdering);
                } catch (ProbeException e) {
                    logger.error("Failed to create stitching operations for probe {} due to {}", probeId, e);
                }
            });
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
                throw new ProbeException("Probe for requested id is not registered: " + probeId);
            }
            return probes.get(probeId);
        }
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
        Optional<Long> probeId = getProbeIdForType(probeInfo.getProbeType());
        if (!probeId.isPresent()) {
            logger.warn("Trying to update a non-existing probeInfo: {}", probeInfo);
            return;
        }

        synchronized (dataLock) {

            try {
                keyValueStore.put(PROBE_KV_STORE_PREFIX + Long.toString(probeId.get()),
                        JsonFormat.printer().print(probeInfo));
            } catch (InvalidProtocolBufferException e) {
                logger.error("Invalid probeInfo {}", probeInfo);
                throw new ProbeException("Failed to persist probe info in Consul. Probe ID: " + probeId);
            }

            probeInfos.put(probeId.get(), probeInfo);
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

    @Override
    public ProbeOrdering getProbeOrdering() {
        return probeOrdering;
    }
}
