package com.vmturbo.topology.processor;

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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeOrdering;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.ProbeStoreListener;
import com.vmturbo.topology.processor.probes.StandardProbeOrdering;
import com.vmturbo.topology.processor.targets.Target;

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

    private final Map<String, Collection<ITransport<MediationServerMessage,
            MediationClientMessage>>> channelToTransport = new ConcurrentHashMap<>();

    public TestProbeStore(@Nonnull IdentityProvider identityProvider) {
        Objects.requireNonNull(identityProvider);
        this.identityProvider = identityProvider;
    }

    @Override
    public boolean registerNewProbe(@Nonnull ProbeInfo probeInfo,
            @Nonnull ContainerInfo containerInfo, @Nonnull ITransport<MediationServerMessage,
            MediationClientMessage> transport) throws ProbeException {
        final long probeId;
        try {
            probeId = identityProvider.getProbeId(probeInfo);
        } catch (IdentityProviderException e) {
            throw new ProbeException("Failed to get ID!", e);
        }
        probeInfos.put(probeId, probeInfo);
        probes.put(probeId, transport);
        listeners.forEach(listener -> listener.onProbeRegistered(probeId, probeInfo));
        if (containerInfo.hasCommunicationBindingChannel()) {
            channelToTransport.computeIfAbsent(containerInfo.getCommunicationBindingChannel(),
                    x -> new ArrayList()).add(transport);
        }

        return true;
    }

    @Override
    public void overwriteProbeInfo(@Nonnull final Map<Long, ProbeInfo> probeInfoMap) {
        probeInfos.clear();
        probeInfos.putAll(probeInfoMap);
    }

    public void removeProbe(ProbeInfo probeInfo) {
        try {
            probeInfos.remove(identityProvider.getProbeId(probeInfo));
        } catch (IdentityProviderException e) {
            // No problem.
        }
    }

    @Override
    public Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransport(long probeId) throws ProbeException {
        if (!probes.containsKey(probeId)) {
            throw new ProbeException(String.format("Probe %s is not registered", probeId));
        }
        return probes.get(probeId);
    }

    @Nonnull
    @Override
    public Collection<ITransport<MediationServerMessage, MediationClientMessage>> getTransportsForTarget(
            @Nonnull Target target) throws ProbeException {
        // Same implementation as in RemoteProbeStore
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

    @Override
    public void removeTransport(ITransport<MediationServerMessage, MediationClientMessage> transport) {
        final Iterator<Entry<Long, ITransport<MediationServerMessage, MediationClientMessage>>>
                iterator =
                probes.entries().iterator();
        while (iterator.hasNext()) {
            final Entry<Long, ITransport<MediationServerMessage, MediationClientMessage>> entry =
                    iterator.next();
            if (entry.getValue().equals(transport)) {
                iterator.remove();
           }
        }
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
    public Optional<ProbeInfo> getProbeInfoForType(@Nonnull final String probeTypeName) {
        return probeInfos.entrySet().stream()
                .filter(entry -> entry.getValue().getProbeType().equals(probeTypeName))
                .map(Entry::getValue)
                .findFirst();
    }

    @Override
    @Nonnull
    public List<Long> getProbeIdsForCategory(@Nonnull final ProbeCategory probeCategory) {
        return probeInfos.entrySet().stream()
            .filter(entry -> entry.getValue().getProbeCategory().equalsIgnoreCase(probeCategory.name()))
            .map(Entry::getKey)
            .collect(Collectors.toList());
    }

    @Override
    public Map<Long, ProbeInfo> getProbes() {
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

    @Override
    public boolean isProbeConnected(@Nonnull final Long probeId) {
        return probes.containsKey(probeId);
    }

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
        return new StandardProbeOrdering(this);
    }

    @Override
    public void updateProbeInfo(ProbeInfo newProbeInfo) {
        Optional<Long> probeId = getProbeIdForType(newProbeInfo.getProbeType());
        if (!probeId.isPresent()) {
            return;
        }
        probeInfos.put(probeId.get(), newProbeInfo);
    }

    @NotNull
    @Override
    public Collection<ProbeRegistrationDescription> getAllProbeRegistrations() {
        return null;
    }

    @NotNull
    @Override
    public Optional<ProbeRegistrationDescription> getProbeRegistrationById(long id) {
        return Optional.empty();
    }

    @Override
    public void initialize() throws InitializationException {
    }
}
