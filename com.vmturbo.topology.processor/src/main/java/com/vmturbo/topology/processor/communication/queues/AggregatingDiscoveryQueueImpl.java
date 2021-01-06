package com.vmturbo.topology.processor.communication.queues;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Implementation of AggregatingDiscoveryQueue.  This queue is shared by RemoteMediationServer and
 * OperationManager.  OperationManager queues discoveries here and per-transport, per-DiscoveryType
 * workers created by the RemoteMediationServer service the queue.
 */
@ThreadSafe
public class AggregatingDiscoveryQueueImpl implements AggregatingDiscoveryQueue {

    private static final Logger logger = LogManager.getLogger(AggregatingDiscoveryQueueImpl.class);

    private final ProbeStore probeStore;

    private final Map<DiscoveryType, Map<Long, DiscoveryQueue>> discoveryQueueByProbeId =
            Maps.newHashMap();

    private final Map<ITransport<MediationServerMessage, MediationClientMessage>,
            Map<DiscoveryType, DiscoveryQueue>> discoveryQueueByTransport = Maps.newHashMap();

    private final Map<Pair<String, String>, ITransport<MediationServerMessage, MediationClientMessage>>
            transportByTargetId = Maps.newHashMap();

    /**
     * Map from channel to discovery queue indexed both by discovery type and probe id. This is
     * used to get, given a channel, the corresponding discovery queue for a given discovery type
     * and probe type.
     */
    private final Map<String, Map<Long, Map<DiscoveryType, DiscoveryQueue>>>
        discoveryQueueByChannel = Maps.newHashMap();

    private final Map<ITransport<MediationServerMessage,
        MediationClientMessage>, String> transportToChannel = new ConcurrentHashMap<>();

    /**
     * Create an instance of {@link AggregatingDiscoveryQueueImpl}.
     * @param probeStore {@link ProbeStore} containing probe information relevant to this queue.
     */
    public AggregatingDiscoveryQueueImpl(@Nonnull ProbeStore probeStore) {
        this.probeStore = probeStore;
    }

    @Override
    public synchronized IDiscoveryQueueElement offerDiscovery(@Nonnull Target target,
            @Nonnull DiscoveryType discoveryType,
            @Nonnull Function<Runnable, DiscoveryBundle> prepareDiscoveryInformation,
            @Nonnull BiConsumer<Discovery, Exception> errorHandler,
            boolean runImmediately) throws ProbeException {

        validateTransportsWithChannels(target);
        try {
            final IDiscoveryQueueElement element = new DiscoveryQueueElement(target, discoveryType,
                    prepareDiscoveryInformation, errorHandler, runImmediately);

            final Pair<String, String> lookupKey = new Pair<>(target.getProbeInfo().getProbeType(),
                    target.getSerializedIdentifyingFields());
            final ITransport<MediationServerMessage, MediationClientMessage> existingTransport =
                    transportByTargetId.get(lookupKey);
            if (existingTransport != null && !targetHasUpdatedChannel(target, existingTransport)) {
                final IDiscoveryQueueElement retVal = discoveryQueueByTransport
                    .computeIfAbsent(existingTransport,
                        key -> Maps.newHashMap()).computeIfAbsent(discoveryType,
                        key -> new DiscoveryQueue(target.getProbeId(), discoveryType)).add(element);
                logger.debug("Added element to queue {}", retVal);
                return retVal;
            } else if (target.getSpec().hasCommunicationBindingChannel()) {
                final IDiscoveryQueueElement retVal =
                    discoveryQueueByChannel.computeIfAbsent(target.getSpec().getCommunicationBindingChannel(),
                        key -> Maps.newHashMap()).computeIfAbsent(target.getProbeId(),
                        k -> Maps.newHashMap()).computeIfAbsent(discoveryType,
                        e -> new DiscoveryQueue(discoveryType)).add(element);
                logger.debug("Added element to queue {}", retVal);
                return retVal;
            } else {
                final IDiscoveryQueueElement retVal =
                        discoveryQueueByProbeId.computeIfAbsent(discoveryType,
                                key -> Maps.newHashMap())
                        .computeIfAbsent(target.getProbeId(),
                                probeId -> new DiscoveryQueue(probeId, discoveryType))
                        .add(element);
                logger.debug("Added element to queue {}", retVal);
                return retVal;
            }
        } catch (DiscoveryQueueException e) {
            // this will never happen
            logger.error("Failed to queue discovery for target {} and discovery type {}",
                    target.getId(), discoveryType, e);
            return null;
        } finally {
            notifyAll();
        }
    }

    /**
     * Return true if both the channel for the target and the channel assigned to the transport
     * are the same, or are both unset. In the other case return false
     *
     * @param target with the channel
     * @param existingTransport associated with a channel
     * @return whether the target of the channel and the one of the transport are the same
     */
    private boolean targetHasUpdatedChannel(Target target, ITransport<MediationServerMessage,
        MediationClientMessage> existingTransport) {
        if (!target.getSpec().hasCommunicationBindingChannel()) {
            return transportToChannel.containsKey(existingTransport);
        } else {
            return !target.getSpec().getCommunicationBindingChannel().equals(transportToChannel.get(existingTransport));
        }
    }

    /**
     * Validate that given a target with a channel, there is at least one transport with that
     * channel. Otherwise, throw an exception. In addition, checks that there's a registered
     * probe that can discover the target.
     *
     * @param target to check
     * @throws ProbeException if no transports for the target channel exist
     */
    private void validateTransportsWithChannels(Target target) throws ProbeException {
        if (target.getSpec().hasCommunicationBindingChannel()) {
            String channel = target.getSpec().getCommunicationBindingChannel();
            Optional<String> availableTransport =
                transportToChannel.values().stream().filter(c -> c.equals(channel)).findAny();
            if (!availableTransport.isPresent()) {
                throw new ProbeException("There are no transports with the following channel: "
                    + target.getSpec().getCommunicationBindingChannel());
            }
        }
        long probeId = target.getProbeId();
        probeStore.getProbe(probeId).orElseThrow(() -> new ProbeException(String.format(
            "Probe %s is not registered", probeId)));
    }

    private synchronized Optional<IDiscoveryQueueElement> pollNextQueuedDiscovery(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull Collection<Long> probeTypes, @Nonnull DiscoveryType discoveryType) {
        final Map<Long, DiscoveryQueue> queuesByProbeId = discoveryQueueByProbeId
            .getOrDefault(discoveryType, Maps.newHashMap());
        Stream<DiscoveryQueue> queuesByProbeType = probeTypes.stream()
            .map(queuesByProbeId::get)
            .filter(Objects::nonNull);
        DiscoveryQueue queueByTransport = discoveryQueueByTransport.getOrDefault(transport, Maps.newHashMap())
            .get(discoveryType);
        Map<Long, Map<DiscoveryType, DiscoveryQueue>> probeTypeToDiscoveryQueue =
            discoveryQueueByChannel.getOrDefault(transportToChannel.get(transport), Maps.newHashMap());
        Stream<DiscoveryQueue> queuesByChannel =
            probeTypes.stream().map(probeTypeToDiscoveryQueue::get).filter(Objects::nonNull).map(key -> key.get(discoveryType));

        Optional<DiscoveryQueue> queueToUse =
            Stream.of(queuesByProbeType, Stream.of(queueByTransport), queuesByChannel)
                .flatMap(s -> s)
                .filter(Objects::nonNull)
                .filter(queue -> !queue.isEmpty())
                .sorted(Comparator.comparing(queue ->
                        queue.peek().get()))
                .findFirst();
        logger.debug("Returning queued discovery {}", () -> queueToUse.isPresent()
                ? queueToUse.get().peek().get()
                : "no discovery found");
        return queueToUse.flatMap(queue -> queue.remove());
    }

    @Override
    public synchronized Optional<IDiscoveryQueueElement> takeNextQueuedDiscovery(
            @NotNull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @NotNull Collection<Long> probeTypes, @NotNull DiscoveryType discoveryType)
            throws InterruptedException {
        Optional<IDiscoveryQueueElement> optElement = pollNextQueuedDiscovery(transport, probeTypes,
                discoveryType);
        while (!optElement.isPresent()) {
            wait();
            optElement = pollNextQueuedDiscovery(transport, probeTypes,
                    discoveryType);
        }
        logger.debug("takeNextQueuedDiscovery returning {}", optElement.map(element -> element.toString()));
        return optElement;
    }

    /**
     * Parse information from a {@link ContainerInfo} and the corresponding {@link ITransport}.
     *
     * @param containerInfo to parse
     * @param serverEndpoint that is connected to the {@link ContainerInfo}
     */
    @Override
    public synchronized void parseContainerInfoWithTransport(ContainerInfo containerInfo,
                                                             ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint) {
        if (containerInfo.hasCommunicationBindingChannel()) {
            assignChannelToTransport(containerInfo.getCommunicationBindingChannel(), serverEndpoint);
        }
        containerInfo.getPersistentTargetIdMapMap()
            .forEach((probeType, targetIdSet) -> targetIdSet.getTargetIdList()
                .forEach(targetId -> assignTargetToTransport(
                    serverEndpoint, probeType, targetId)));
    }

    @Override
    public synchronized void assignTargetToTransport(
        @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
        @Nonnull Target target) {

        if (!target.getProbeInfo().hasIncrementalRediscoveryIntervalSeconds()) {
            return;
        }
        final String probeType = target.getProbeInfo().getProbeType();
        final String targetId = target.getSerializedIdentifyingFields();
        if (target.getSpec().hasCommunicationBindingChannel()) {
            String channel = target.getSpec().getCommunicationBindingChannel();
            long probeId = target.getProbeId();
            if (discoveryQueueByChannel.containsKey(channel) && discoveryQueueByChannel.get(channel).containsKey(probeId)) {
                discoveryQueueByChannel.remove(target.getSpec().getCommunicationBindingChannel()).get(target.getProbeId());
            }
        }
        assignTargetToTransport(transport, probeType, targetId);
    }


    private synchronized void assignTargetToTransport(
        @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
        @Nonnull String probeType,
        @Nonnull String targetId) {
        final Pair<String, String> key = new Pair<>(probeType, targetId);
        final ITransport<MediationServerMessage, MediationClientMessage> transportFromMap =
            transportByTargetId.get(key);
        if (transportFromMap != null && transport != transportFromMap) {
            logger.info("Moving target {} of probe type {} from transport {} to transport {}",
                targetId, probeType, transportFromMap, transport);
        }
        logger.debug("Assigning target {} of probe type {} to transport {}",
            targetId, probeType, transport);
        transportByTargetId.put(key, transport);
    }

    @Override
    public synchronized void handleTargetRemoval(long probeId, long targetId) {
        discoveryQueueByProbeId.values().stream()
                .map(m -> m.get(probeId))
                .filter(Objects::nonNull)
                .forEach(discoveryQueue -> discoveryQueue.handleTargetRemoval(targetId));

        discoveryQueueByTransport.values().stream()
                .flatMap(map -> map.values().stream())
                .forEach(discoveryQueue -> discoveryQueue.handleTargetRemoval(targetId));

        discoveryQueueByChannel.values().stream()
            .flatMap(map -> map.values().stream())
            .flatMap(map -> map.values().stream())
            .forEach(discoveryQueue -> discoveryQueue.handleTargetRemoval(targetId));
    }

    /**
     * Take any elements on the passed in DiscoveryQueue and remove them and add them to the proper
     * ProbeType associated queues. Re-sort any queues that are impacted as the newly added
     * elements may not belong at the end of the queues.
     *
     * @param discoveryQueue {@link DiscoveryQueue} that is being flushed.
     * @param type {@link DiscoveryType} handled by this queue.
     * @param transport that got removed and caused the reassignment
     */
    private void reassignQueueContents(@Nonnull DiscoveryQueue discoveryQueue,
            @Nonnull DiscoveryType type, @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport) {
        Set<Long> impactedProbeIds = Sets.newHashSet();
        Set<String> impactedChannels = Sets.newHashSet();

        while (!discoveryQueue.isEmpty()) {
            Optional<IDiscoveryQueueElement> next = discoveryQueue.remove();
            next.ifPresent(element -> {
                try {
                    if (transportToChannel.containsKey(transport)) {
                        String channel = transportToChannel.get(transport);
                        discoveryQueueByChannel.computeIfAbsent(channel, key -> Maps.newHashMap())
                            .computeIfAbsent(element.getTarget().getProbeId(),
                            key -> Maps.newHashMap()).computeIfAbsent(type,
                        e -> new DiscoveryQueue(type)).add(element);
                        impactedChannels.add(channel);
                    } else {
                        discoveryQueueByProbeId.computeIfAbsent(type, key -> Maps.newHashMap())
                            .computeIfAbsent(element.getTarget().getProbeId(),
                                probeId -> new DiscoveryQueue(probeId, type))
                            .add(element);
                        impactedProbeIds.add(element.getTarget().getProbeId());
                    }
                } catch (DiscoveryQueueException e) {
                    // this will never happen
                    e.printStackTrace();
                }
            });
        }
        impactedProbeIds.forEach(probeId -> discoveryQueueByProbeId.get(type).get(probeId)
                .sort());
        impactedChannels.forEach(channel -> discoveryQueueByChannel.get(channel).values().stream()
            .map(Map::values).flatMap(Collection::stream).forEach(DiscoveryQueue::sort));
    }

    /**
     * Assign a communication channel to a transport.
     *
     * @param communicationChannel the channel
     * @param transport to use
     */
    private void assignChannelToTransport(@Nonnull String communicationChannel,
                                         @Nonnull ITransport<MediationServerMessage,
                                           MediationClientMessage> transport) {
        transportToChannel.put(transport, communicationChannel);
    }

    @Override
    public synchronized void handleTransportRemoval(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport) {
        transportByTargetId.values().removeIf(transport::equals);
        discoveryQueueByTransport.getOrDefault(transport, Collections.emptyMap())
                .entrySet()
                .stream()
                .forEach(entry -> reassignQueueContents(entry.getValue(), entry.getKey(), transport));
        transportToChannel.keySet().removeIf(transport::equals);
        notifyAll();
    }
}
