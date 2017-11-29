package com.vmturbo.topology.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;

/**
 * Dead-simple probe store for target tests.
 *
 * Need this instead of a mock because it has to persist
 * registered probes.
 */
public class TestProbeStore implements ProbeStore {
    private final Map<Long, ProbeInfo> probeInfos = new HashMap<>();
    private final IdentityProvider identityProvider;
    private final List<ProbeStoreListener> listeners = new ArrayList<>();

    private final Multimap<Long, ITransport<MediationServerMessage, MediationClientMessage>> probes =
        HashMultimap.create();

    public TestProbeStore(@Nonnull IdentityProvider identityProvider) {
        Objects.requireNonNull(identityProvider);
        this.identityProvider = identityProvider;
    }

    @Override
    public boolean registerNewProbe(@Nonnull ProbeInfo probeInfo, ITransport<MediationServerMessage, MediationClientMessage> transport) throws ProbeException {
        long probeId = identityProvider.getProbeId(probeInfo);
        probeInfos.put(probeId, probeInfo);
        probes.put(probeId, transport);
        listeners.forEach(listener -> listener.onProbeRegistered(probeId, probeInfo));

        return true;
    }

    public void removeProbe(ProbeInfo probeInfo) {
        probeInfos.remove(identityProvider.getProbeId(probeInfo));
    }

    @Override
    public Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransport(long probeId) throws ProbeException {
        if (!probeInfos.containsKey(probeId)) {
            throw new ProbeException("Probe for requested type is not registered: " + probeId);
        }
        return probes.get(probeId);
    }

    @Override
    public void removeTransport(ITransport<MediationServerMessage, MediationClientMessage> transport) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ProbeInfo> getProbe(long probeId) {
        return Optional.ofNullable(probeInfos.get(probeId));
    }

    @Override
    public Optional<Long> getProbeIdForType(@Nonnull final String probeTypeName) {
        return probeInfos.entrySet().stream()
            .filter(entry -> entry.getValue().getProbeType().equals(probeTypeName))
            .map(Entry::getKey)
            .findFirst();
    }

    @Override
    public Map<Long, ProbeInfo> getRegisteredProbes() {
        return probeInfos;
    }

    @Override
    public void addListener(@Nonnull ProbeStoreListener listener) {
        listeners.add(listener);
    }

    @Override
    public boolean removeListener(@Nonnull ProbeStoreListener listener) {
        return listeners.remove(listener);
    }
}
