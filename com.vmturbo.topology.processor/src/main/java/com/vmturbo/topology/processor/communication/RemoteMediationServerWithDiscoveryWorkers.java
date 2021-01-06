package com.vmturbo.topology.processor.communication;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.TargetUpdateRequest;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.communication.queues.IDiscoveryQueueElement;
import com.vmturbo.topology.processor.operation.IOperationMessageHandler;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.discovery.DiscoveryBundle;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;

/**
 * This subclass of {@link RemoteMediationServer} moves permit control from
 * {@link com.vmturbo.topology.processor.operation.OperationManager} where it is controlled at
 * the ProbeType level down to the remote mediation level where it can be properly controlled
 * at the container level. Now, instead of allowing a fixed number of discoveries per probe type,
 * we allow a fixed number per probe container, which makes much more sense and allows us to
 * add discovery capacity simply by adding probe containers.  Where previously
 * RemoteMediationServer.sendDiscoveryRequest was called by OperationManager, now OperationManager
 * simply queues discoveries in an {@link AggregatingDiscoveryQueue} and this class has worker
 * threads that service the queue and feed discoveries to the appropriate containers' transports.
 *
 */
public class RemoteMediationServerWithDiscoveryWorkers extends RemoteMediationServer {

    private final Logger logger = LogManager.getLogger();

    /**
     * Map of transport to map of discovery type to transport worker.
     */
    private final Map<ITransport<MediationServerMessage, MediationClientMessage>,
            Map<DiscoveryType, TransportDiscoveryWorker>> transportToTransportWorker =
                Maps.newHashMap();

    private final AggregatingDiscoveryQueue discoveryQueue;

    private final int maxConcurrentTargetDiscoveriesPerContainerCount;

    private final int maxConcurrentTargetIncrementalDiscoveriesPerContainerCount;

    /**
     * Construct the instance.
     *  @param probeStore probes registry
     * @param probePropertyStore probe and target-specific properties registry
     * @param containerChooser it will route the requests to the right transport
     * @param discoveryQueue AggregatingDiscoveryQueue where target discoveries are queued for
     * service by TransportDiscoveryWorkers.
     * @param maxConcurrentTargetDiscoveriesPerContainerCount the number of full discoveries that
     * can be carried out in parallel per probe container.
     * @param maxConcurrentTargetIncrementalDiscoveriesPerContainerCount the number of incremental
     * discoveries that can be carried out in parallel per probe container.
     */
    public RemoteMediationServerWithDiscoveryWorkers(@Nonnull final ProbeStore probeStore,
            @Nonnull ProbePropertyStore probePropertyStore,
            @Nonnull ProbeContainerChooser containerChooser,
            @Nonnull AggregatingDiscoveryQueue discoveryQueue,
            int maxConcurrentTargetDiscoveriesPerContainerCount,
            int maxConcurrentTargetIncrementalDiscoveriesPerContainerCount) {
        super(probeStore, probePropertyStore, containerChooser);
        this.discoveryQueue = discoveryQueue;
        this.maxConcurrentTargetDiscoveriesPerContainerCount =
                maxConcurrentTargetDiscoveriesPerContainerCount;
        this.maxConcurrentTargetIncrementalDiscoveriesPerContainerCount =
                maxConcurrentTargetIncrementalDiscoveriesPerContainerCount;
    }

    private boolean supportsIncrementalDiscovery(ContainerInfo containerInfo) {
        return containerInfo.getProbesList().stream()
                .filter(ProbeInfo::hasIncrementalRediscoveryIntervalSeconds)
                .findAny()
                .isPresent();
    }

    @Override
    public void registerTransport(ContainerInfo containerInfo,
                    ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint) {
        super.registerTransport(containerInfo, serverEndpoint);
        containerInfo.getPersistentTargetIdMapMap()
                .forEach((probeType, targetIdSet) -> targetIdSet.getTargetIdList()
                        .forEach(targetId -> discoveryQueue.assignTargetToTransport(
                                serverEndpoint, probeType, targetId)));

        synchronized (transportToTransportWorker) {
            transportToTransportWorker.computeIfAbsent(serverEndpoint, key -> Maps.newHashMap())
                    .put(DiscoveryType.FULL,
                            new TransportDiscoveryWorker(serverEndpoint, containerInfo, probeStore,
                                    discoveryQueue, maxConcurrentTargetDiscoveriesPerContainerCount,
                                    DiscoveryType.FULL));
            if (supportsIncrementalDiscovery(containerInfo)) {
                transportToTransportWorker.get(serverEndpoint).put(DiscoveryType.INCREMENTAL,
                        new TransportDiscoveryWorker(serverEndpoint, containerInfo, probeStore,
                                discoveryQueue,
                                maxConcurrentTargetIncrementalDiscoveriesPerContainerCount,
                                DiscoveryType.INCREMENTAL));
            }
            transportToTransportWorker.getOrDefault(serverEndpoint, Collections.emptyMap()).values()
                    .forEach(Thread::start);
        }
    }

    /**
     * When a container is closed, remove all probe types for that container from the probe type
     * map.
     *
     * @param endpoint endpoint, representing communication link with the closed container.
     */
    protected void processContainerClose(
                    ITransport<MediationServerMessage, MediationClientMessage> endpoint) {
        discoveryQueue.handleTransportRemoval(endpoint);
        synchronized (transportToTransportWorker) {
            if (transportToTransportWorker.containsKey(endpoint)) {
                transportToTransportWorker.remove(endpoint).values()
                        .forEach(transportDiscoveryWorker -> {
                            transportDiscoveryWorker.containerClose();
                            transportDiscoveryWorker.interrupt();
                        });
            }
        }
        super.processContainerClose(endpoint);
    }

    private int sendMessageViaTransport(@Nonnull MediationServerMessage message,
            @Nonnull Target target,
            @Nonnull IOperationMessageHandler<?> responseHandler,
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport)
            throws CommunicationException, InterruptedException {
        boolean success = false;
        try {
            // Register the handler before sending the message so there is no gap where there is
            // no registered handler for an outgoing message. Of course this means cleanup is
            // necessary!
            if (responseHandler != null) {
                messageHandlers.put(
                        message.getMessageID(),
                        new MessageAnticipator(transport, responseHandler));
            }
            getLogger().trace("Sending message to {} through {}", target::getNoSecretDto,
                    () -> transport);
            transport.send(message);
            success = true;
        } finally {
            if (!success) {
                messageHandlers.remove(message.getMessageID());
            }
        }
        return message.getMessageID();
    }

    @Override
    public int sendDiscoveryRequest(final Target target,
                                     @Nonnull final DiscoveryRequest discoveryRequest,
                                     @Nonnull final IOperationMessageHandler<Discovery>
                                             responseHandler)
        throws ProbeException, CommunicationException, InterruptedException {

        throw new UnsupportedOperationException("Old style sendDiscoveryRequest called in "
                + "new RemoteMediationServer");
    }

    private int sendDiscoveryRequest(@Nonnull Target target,
            @Nonnull DiscoveryRequest discoveryRequest,
            @Nonnull IOperationMessageHandler<Discovery> responseHandler,
            @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport)
            throws CommunicationException, InterruptedException {

        final int messageId = nextMessageId();
        final MediationServerMessage message = MediationServerMessage.newBuilder()
                .setMessageID(messageId)
                .setDiscoveryRequest(discoveryRequest).build();

        // if this target is related to a persistent probe register the association between the
        // target and the transport with the discoveryQueue
        probeStore.getProbe(target.getProbeId()).ifPresent(probeInfo -> {
                if (probeInfo.hasIncrementalRediscoveryIntervalSeconds()) {
                    discoveryQueue.assignTargetToTransport(transport,
                            target.getProbeInfo().getProbeType(),
                            target.getSerializedIdentifyingFields());
                }
        });
        return sendMessageViaTransport(message, target, responseHandler, transport);
    }

    @Override
    public void handleTargetRemoval(long probeId, long targetId,
                                    @Nonnull TargetUpdateRequest request)
                    throws CommunicationException, InterruptedException, ProbeException {
        discoveryQueue.handleTargetRemoval(probeId, targetId);
        super.handleTargetRemoval(probeId, targetId, request);
    }

    /**
     * Worker thread associated with a particular transport that runs discoveries pulled from a shared
     * queue subject to a limit of concurrent discoveries.
     */
    private class TransportDiscoveryWorker extends Thread {

        private static final String THREAD_NAME_PREFIX = "TransportDiscoveryWorker";

        private static final String THREAD_NAME_SEPARATOR = "-";

        private AtomicBoolean transportClosed = new AtomicBoolean(false);

        private final ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint;

        private final Set<Long> probesSupported = Sets.newHashSet();

        private final AggregatingDiscoveryQueue discoveryQueue;

        private final int maxPermits;

        private final AtomicInteger numPermits;

        private final DiscoveryType discoveryType;

        /**
         * Create a TransportDiscoveryWorker.
         *
         * @param serverEndpoint {@link ITransport} associated with this worker.
         * @param containerInfo {@link ContainerInfo} providing information on probe types supported
         * and dedicated targets.
         * @param probeStore {@link ProbeStore} used for processing container info.
         * @param discoveryQueue {@link AggregatingDiscoveryQueue} from which to pull discoveries.
         * @param permits number of simultaneous discoveries to support.
         * @param discoveryType DiscoveryType handled by this worker.
         */
        TransportDiscoveryWorker(
                @Nonnull ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint,
                @Nonnull ContainerInfo containerInfo,
                @Nonnull ProbeStore probeStore,
                @Nonnull AggregatingDiscoveryQueue discoveryQueue,
                int permits,
                @Nonnull DiscoveryType discoveryType) {
            this.serverEndpoint = serverEndpoint;
            this.discoveryQueue = discoveryQueue;
            this.maxPermits = permits;
            this.numPermits = new AtomicInteger(permits);
            this.discoveryType = discoveryType;
            logger.info("Creating transport worker for probe types {} with {} permits.",
                    containerInfo.getProbesList().stream().map(ProbeInfo::getProbeType).collect(
                            Collectors.joining(", ")), permits);
            processContainerInfo(containerInfo, probeStore);
            setName(createThreadName(containerInfo));
        }

        private String createThreadName(@Nonnull ContainerInfo containerInfo) {
            return THREAD_NAME_PREFIX + THREAD_NAME_SEPARATOR + containerInfo.getProbesList()
                    .stream()
                    .map(ProbeInfo::getProbeType)
                    .collect(Collectors.joining(THREAD_NAME_SEPARATOR))
                    + THREAD_NAME_SEPARATOR + discoveryType + THREAD_NAME_SEPARATOR
                    + super.getName();
        }

        private void processContainerInfo(@Nonnull ContainerInfo containerInfo,
                @Nonnull ProbeStore probeStore) {
            // If this worker supports full discovery, populate the list with all probe types in
            // containerinfo. If it supports incremental, then only get the ids of probes that
            // have incremental discoveries.
            containerInfo.getProbesList().stream()
                    .filter(probeInfo -> discoveryType == DiscoveryType.INCREMENTAL
                            ? probeInfo.hasIncrementalRediscoveryIntervalSeconds() : true)
                    .map(ProbeInfo::getProbeType)
                    .map(probeStore::getProbeIdForType)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(probesSupported::add);
        }

        /**
         * The Transport associated with this worker is gone, so set transportDeleted to true so that
         * run method will end.
         */
        public void containerClose() {
            transportClosed.set(true);
        }

        private int returnPermit(long targetId) {
            final int permits = numPermits.incrementAndGet();
            logger.info("Number of permits after discovery completed for target {}({}): {}",
                    targetId, discoveryType, permits);
            synchronized (numPermits) {
                numPermits.notifyAll();
            }
            return permits;
        }

        private void logAndRecordException(@Nonnull Exception e, @Nonnull DiscoveryBundle bundle) {
            final long targetId = bundle.getDiscovery().getTargetId();
            logger.error("Exception while trying to execute discovery for "
                    + "target {}", targetId, e);
            returnPermit(targetId);
            bundle.setException(e);
        }

        @Override
        public void run() {
            logger.info("Starting transport worker for DiscoveryType {} and probe IDs {}",
                    discoveryType, probesSupported);
            while (!transportClosed.get()) {
                if (numPermits.getAndDecrement() > 0) {
                    try {
                        // wait to get the next available discovery
                        logger.trace("About to call takeNextQueuedDiscovery...");
                        Optional<IDiscoveryQueueElement> optDiscoveryElement =
                                discoveryQueue.takeNextQueuedDiscovery(serverEndpoint,
                                        probesSupported, discoveryType);
                        logger.trace("Called takeNextQueuedDiscovery and got back {}",
                                optDiscoveryElement.isPresent()
                                        ? optDiscoveryElement.get()
                                        : "empty");
                        // If we got a discovery, process it.  If not, we will continue looping as
                        // long as transportDeleted is not set to true.
                        optDiscoveryElement.ifPresent(discoveryElement -> {
                            logger.debug("Acquired discovery for target {}",
                                    discoveryElement.getTarget().getId());
                            final Target target = discoveryElement.getTarget();
                            final long targetId = target.getId();
                            logger.info("Beginning discovery of target {}({}) leaving {} of {} "
                                            + "permits available.", target.getId(), discoveryType,
                                    numPermits.get(), maxPermits);
                            discoveryElement.performDiscovery((bundle) -> {
                                try {
                                    if (bundle.getDiscoveryRequest() == null) {
                                        // There was already a running discovery. We shouldn't run
                                        // another one.
                                        returnPermit(targetId);
                                    } else {
                                        final int messageId = sendDiscoveryRequest(target,
                                                bundle.getDiscoveryRequest(),
                                                bundle.getDiscoveryMessageHandler(),
                                                serverEndpoint);
                                        bundle.getDiscovery().setMediationMessageId(messageId);
                                        logger.info("Beginning {}", bundle.getDiscovery());
                                    }
                                } catch (InterruptedException e) {
                                    logAndRecordException(e, bundle);
                                    Thread.currentThread().interrupt();
                                } catch (CommunicationException | RuntimeException e) {
                                    logAndRecordException(e, bundle);
                                }
                                logger.debug("Returning discovery {} for target {}({})",
                                        bundle.getDiscovery(),
                                        discoveryElement.getTarget().getId(), discoveryType);
                                return bundle.getDiscovery();
                            }, () -> returnPermit(targetId));
                        });
                    } catch (InterruptedException e) {
                        // interrupt() is called when transport has been closed
                        logger.warn("Interrupted while waiting to acquire target information. "
                                + "Transport is {}. TransportClosed value is {}", serverEndpoint,
                                transportClosed.get(), e);
                        numPermits.incrementAndGet();
                    }
                } else { // no permits available, wait for a permit to get returned
                    synchronized (numPermits) {
                        try {
                            // give back the permit we took in if statement
                            numPermits.incrementAndGet();
                            numPermits.wait();
                        } catch (InterruptedException e) {
                            // interrupt() is called when transport has been closed
                            logger.warn("Interrupted while waiting for permit to be released. "
                                    + "Transport is {}", serverEndpoint, e);
                        }
                    }
                }
            }
        }
    }
}
