package com.vmturbo.topology.processor.communication.queues;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
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
            boolean runImmediately) {
        try {
            final IDiscoveryQueueElement element = new DiscoveryQueueElement(target, discoveryType,
                    prepareDiscoveryInformation, errorHandler, runImmediately);

            final Pair<String, String> lookupKey = new Pair<>(target.getProbeInfo().getProbeType(),
                    target.getSerializedIdentifyingFields());
            final ITransport<MediationServerMessage, MediationClientMessage> existingTransport =
                    transportByTargetId.get(lookupKey);
            if (existingTransport != null) {
                final IDiscoveryQueueElement retVal = discoveryQueueByTransport
                        .computeIfAbsent(existingTransport,
                        key -> Maps.newHashMap()).computeIfAbsent(discoveryType,
                        key -> new DiscoveryQueue(target.getProbeId(), discoveryType)).add(element);
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

    private synchronized Optional<IDiscoveryQueueElement> pollNextQueuedDiscovery(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull Collection<Long> probeTypes, @Nonnull DiscoveryType discoveryType) {

        final Map<Long, DiscoveryQueue> queuesByProbeId = discoveryQueueByProbeId
                .getOrDefault(discoveryType, Maps.newHashMap());
        Optional<DiscoveryQueue> queueToUse = Stream.concat(probeTypes.stream()
                .map(queuesByProbeId::get)
                .filter(Objects::nonNull),
                Stream.of(discoveryQueueByTransport.getOrDefault(transport, Maps.newHashMap())
                        .get(discoveryType)))
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

    @Override
    public synchronized void assignTargetToTransport(
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
    }

    /**
     * Take any elements on the passed in DiscoveryQueue and remove them and add them to the proper
     * ProbeType associated queues. Re-sort any queues that are impacted as the newly added
     * elements may not belong at the end of the queues.
     *
     * @param discoveryQueue {@link DiscoveryQueue} that is being flushed.
     * @param type {@link DiscoveryType} handled by this queue.
     */
    private void reassignQueueContents(@Nonnull DiscoveryQueue discoveryQueue,
            @Nonnull DiscoveryType type) {
        Set<Long> queuesImpacted = Sets.newHashSet();
        while (!discoveryQueue.isEmpty()) {
            Optional<IDiscoveryQueueElement> next = discoveryQueue.remove();
            next.ifPresent(element -> {
                try {
                    discoveryQueueByProbeId.computeIfAbsent(type, key -> Maps.newHashMap())
                        .computeIfAbsent(element.getTarget().getProbeId(),
                                probeId -> new DiscoveryQueue(probeId, type))
                        .add(element);
                } catch (DiscoveryQueueException e) {
                    // this will never happen
                    e.printStackTrace();
                }
                queuesImpacted.add(element.getTarget().getProbeId());
            });
        }
        queuesImpacted.forEach(probeId -> discoveryQueueByProbeId.get(type).get(probeId)
                .sort());
    }

    @Override
    public synchronized void handleTransportRemoval(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport) {
        transportByTargetId.values().removeIf(transport::equals);
        discoveryQueueByTransport.getOrDefault(transport, Collections.emptyMap())
                .entrySet()
                .stream()
                .forEach(entry -> reassignQueueContents(entry.getValue(), entry.getKey()));
        notifyAll();
    }
}
