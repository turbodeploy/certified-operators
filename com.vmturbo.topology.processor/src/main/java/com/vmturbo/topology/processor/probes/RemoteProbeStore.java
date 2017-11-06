package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.sdk.server.common.ProbeInfoComparator;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;

/**
 * Store for remote probe information. It publishes several convenient methods to operate with
 * probes.
 */
@ThreadSafe
public class RemoteProbeStore implements ProbeStore {

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
    private final Map<Long, ProbeInfo> probeInfos = new HashMap<>();

    /**
     * List of registered listeners to ProbeStore events.
     */
    @GuardedBy("dataLock")
    private final List<ProbeStoreListener> listeners = new ArrayList<>();

    private final IdentityProvider identityProvider_;

    private final StitchingOperationStore stitchingOperationStore;

    public RemoteProbeStore(@Nonnull IdentityProvider identityProvider,
                            @Nonnull final StitchingOperationStore stitchingOperationStore) {
        Objects.requireNonNull(identityProvider);
        identityProvider_ = identityProvider;
        this.stitchingOperationStore = stitchingOperationStore;
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
            if (existing != null && !ProbeInfoComparator.equals(existing, probeInfo) &&
                    probes.containsKey(probeId)) {
                throw new ProbeException("Probe configuration " + probeInfo
                                + ", received from " + transport + " differs "
                                + "from already registered probe with the same probe type: "
                                + existing);
            } else {
                logger.debug("Adding endpoint to probe type map: " + transport + " " + probeInfo.getProbeType());
                final boolean probeExists = probes.containsKey(probeId);
                stitchingOperationStore.setOperationsForProbe(probeId, probeInfo);

                probes.put(probeId, transport);
                if (!probeExists) {
                    probeInfos.put(probeId, probeInfo);
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
    public Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransport(
                    long probeId) throws ProbeException {
        synchronized (dataLock) {
            if (!probeInfos.containsKey(probeId)) {
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
                        probeInfos.remove(probeId);
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
    public Map<Long, ProbeInfo> getRegisteredProbes() {
        synchronized (dataLock) {
            return ImmutableMap.copyOf(probeInfos);
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
}
