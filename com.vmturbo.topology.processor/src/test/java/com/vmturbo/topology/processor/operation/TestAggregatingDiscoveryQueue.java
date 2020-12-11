package com.vmturbo.topology.processor.operation;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.communication.queues.DiscoveryQueueElement;
import com.vmturbo.topology.processor.communication.queues.IDiscoveryQueueElement;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Utility class to simulate an AggregatingDiscoveryQueue for test purposes.
 */
public class TestAggregatingDiscoveryQueue implements AggregatingDiscoveryQueue {

    private static final Logger logger = LogManager.getLogger(TestAggregatingDiscoveryQueue.class);

    private final Map<DiscoveryType, Queue<IDiscoveryQueueElement>> queueMap = Maps.newHashMap();
    private final Set<Long> targetSet = Sets.newHashSet();

    private final ITransport<MediationServerMessage, MediationClientMessage> endpoint;

    private final ExecutorService executorService;

    /**
     * Create an instance of TestAggregatingDiscoveryQueue.
     *
     * @param transport Transport to use for immediately running queued discoveries. If null, we
     * don't execute discoveries off the queue but expect the test code using this class to pull
     * things off the queue and run them.
     */
    public TestAggregatingDiscoveryQueue(
            @Nullable ITransport<MediationServerMessage, MediationClientMessage> transport) {
        endpoint = transport;
        executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public synchronized IDiscoveryQueueElement offerDiscovery(@Nonnull Target target,
            @Nonnull DiscoveryType discoveryType,
            @Nonnull Function<Runnable, DiscoveryBundle> prepareDiscoveryInformation,
            @Nonnull BiConsumer<Discovery, Exception> errorHandler,
            boolean runImmediately) {
        IDiscoveryQueueElement retVal;
        logger.debug("Queueing discovery {}({})", target.getId(), discoveryType);
        if (!targetSet.contains(target.getId())) {
            targetSet.add(target.getId());
            IDiscoveryQueueElement element = new DiscoveryQueueElement(target, discoveryType,
                    prepareDiscoveryInformation, errorHandler);
            queueMap.computeIfAbsent(discoveryType, key -> Queues.newArrayDeque())
                    .add(element);
            retVal = element;
        } else {
            logger.debug("Discovery already exists in queue. Retrieving it.");
            retVal = queueMap.get(discoveryType).stream()
                    .filter(element -> element.getTarget().getId() == target.getId())
                    .findFirst()
                    .get();
        }
        if (endpoint != null) {
            logger.debug("Submitting discovery for execution.");
            executorService.submit(() -> {
                logger.info("About to run discovery...");
                retVal.performDiscovery((bundle) -> bundle.getDiscovery(), () -> callback());
            });
        }
        notifyAll();
        return retVal;
    }

    /**
     * Get the next available queue element without waiting if none is available.
     *
     * @param transport the transport related to the caller.
     * @param probeTypes the types of probes the caller can service.
     * @param discoveryType the discovery type the caller can service.
     * @return an Optional with the next element in the queue or Optional.empty if the queue is
     * empty.
     */
    public Optional<IDiscoveryQueueElement> pollNextQueuedDiscovery(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull Collection<Long> probeTypes, @Nonnull DiscoveryType discoveryType) {
        Queue<IDiscoveryQueueElement> queue = queueMap.get(discoveryType);
        if (queue != null && !queue.isEmpty()) {
            IDiscoveryQueueElement element = queue.remove();
            targetSet.remove(element.getTarget().getId());
            return Optional.of(element);
        }
        return Optional.empty();
    }

    @Override
    public void assignTargetToTransport(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull String targetIdentifiers) {

    }

    @Override
    public void handleTargetRemoval(long probeId, long targetId) {
        logger.debug("Handling target removal for target {} of probe {}", targetId, probeId);
        if (targetSet.contains(targetId)) {
            queueMap.values().forEach(queue -> queue.removeIf(element ->
                    element.getTarget().getId() == targetId));
        }
    }

    @Override
    public void handleTransportRemoval(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport) {

    }

    /**
     * Get the ITransport associated with this queue.
     *
     * @return mocked ITransport object.
     */
    public ITransport<MediationServerMessage, MediationClientMessage> getTransport() {
        return endpoint;
    }

    /**
     * Callback method that should be called when discovery completes.
     *
     * @return always returns 1.
     */
    public int callback() {
        logger.debug("Callback called.");
        return 1;
    }

    /**
     * Return the number of discoveries queued for the discoveryType.
     *
     * @param discoveryType the DiscoveryType whose queue size we want.
     * @return number of elements in the queue.
     */
    public synchronized int size(DiscoveryType discoveryType) {
        Queue<IDiscoveryQueueElement> queue = queueMap.get(discoveryType);
        if (queue == null) {
            return 0;
        }
        return queueMap.get(discoveryType).size();
    }

    @Override
    public synchronized Optional<IDiscoveryQueueElement> takeNextQueuedDiscovery(
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
            @Nonnull Collection<Long> probeTypes, @Nonnull DiscoveryType discoveryType)
            throws InterruptedException {
        Optional<IDiscoveryQueueElement> element = pollNextQueuedDiscovery(transport, probeTypes,
                discoveryType);
        while (!element.isPresent()) {
            wait();
            element = pollNextQueuedDiscovery(transport, probeTypes,
                    discoveryType);
        }
        return element;
    }
}
