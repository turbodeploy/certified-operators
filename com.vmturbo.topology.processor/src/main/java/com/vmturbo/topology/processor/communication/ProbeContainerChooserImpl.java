package com.vmturbo.topology.processor.communication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Class for choosing a ITransport to use to communicate with a probe when multiple probes of the
 * same type are running. For probes that supports incremental discoveries This
 * {@link ProbeContainerChooser} chooses an ITransport if it is already assigned to a targetId.
 * If not, it chooses the next ITransport and assigns it to that targetId. For full discoveries,
 * validation and action executions, this chooser just select a transport using round robin,
 * without assigning it to the target.
 */
public class ProbeContainerChooserImpl implements ProbeContainerChooser {

    private final Logger logger = LogManager.getLogger();
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final ScheduledExecutorService scheduledExecutorService;
    private final long targetRebalanceDelaySec;

    private final Map<String, ScheduledFuture<?>> rebalanceTaskByProbeType = new ConcurrentHashMap<>();

    private final Map<Pair<String, String>, ITransport<MediationServerMessage,
            MediationClientMessage>> targetIdToTransport = new ConcurrentHashMap<>();

    /**
     * The index of the last used transport from the transport collection of a given type of probe.
     * We increment by one each time the getTransportUsingRoundRobin method is called and then mod
     * it by the size of the collection.
     */
    private final Map<String, Integer> roundRobinIndexByProbeType = new HashMap<>();

    private static final Comparator<TransportInfo> TARGET_REBALANCE_COMPARATOR =
            Comparator.comparingInt(TransportInfo::targetCount);

    private static final Comparator<TransportInfo> TARGET_REBALANCE_COMPARATOR_REVERSE =
            TARGET_REBALANCE_COMPARATOR.reversed();

    /**
     * The operation to perform when rebalance is done.
     */
    private Consumer<List<TransportReassignment>> rebalanceCallback;

    /**
     * Initialize a new ProbeContainerChooserImp.
     *
     * @param probeStore containing the probes
     * @param targetStore the store for managing targets
     * @param scheduledExecutorService service for scheduling task
     * @param targetRebalanceDelaySec the seconds to wait before rebalancing targets after transport is registered or removed
     */
    public ProbeContainerChooserImpl(@Nonnull final ProbeStore probeStore,
                                     @Nonnull final TargetStore targetStore,
                                     @Nonnull final ScheduledExecutorService scheduledExecutorService,
                                     long targetRebalanceDelaySec) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.scheduledExecutorService = scheduledExecutorService;
        this.targetRebalanceDelaySec = targetRebalanceDelaySec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ITransport<MediationServerMessage, MediationClientMessage> choose( @Nonnull final Target target,
        @Nonnull final MediationServerMessage message) throws ProbeException {
        Collection<ITransport<MediationServerMessage, MediationClientMessage>> transportCollection =
                probeStore.getTransportsForTarget(target);

        if (!message.hasDiscoveryRequest()) {
            logger.debug("Choosing transport from {} options for target {}.",
                    transportCollection.size(), target.getDisplayName());
            // even for persistent probe, we are round-robin non-discovery requests like: validation/action/...
            return getTransportUsingRoundRobin(target.getProbeInfo().getProbeType(), transportCollection);
        }
        return choose(target);
    }

    /**
     * Chooses the transport for a target.
     *
     * @param target the target to which the message is being sent
     * @return the assigned ITransport
     * @throws ProbeException if probe can't be found
     */
    private ITransport<MediationServerMessage, MediationClientMessage> choose(@Nonnull final Target target) throws ProbeException {
        Collection<ITransport<MediationServerMessage, MediationClientMessage>> transportCollection =
                probeStore.getTransportsForTarget(target);
        logger.debug("Choosing transport from {} options for target {}.",
                transportCollection.size(), target.getDisplayName());

        ProbeInfo probeInfo = probeStore.getProbe(target.getProbeId()).orElseThrow(() -> new ProbeException(
                String.format(ProbeStore.NO_TRANSPORTS_MESSAGE + " (probe id %s)", target.getProbeId())));

        if (!probeSupportsPersistentConnections(probeInfo)) {
            return getTransportUsingRoundRobin(probeInfo.getProbeType(), transportCollection);
        }
        final String targetIdentifyingValues = target.getSerializedIdentifyingFields();
        final Pair<String, String> lookupKey = new Pair<>(probeInfo.getProbeType(),
                targetIdentifyingValues);
        synchronized (targetIdToTransport) {
            ITransport<MediationServerMessage, MediationClientMessage> targetTransport =
                    targetIdToTransport.get(lookupKey);
            // Check that the transport that we have cached is still among the available transports
            // of the probe
            if (targetTransport != null) {
                if (!transportCollection.contains(targetTransport)) {
                    targetIdToTransport.remove(lookupKey);
                }
            } else {
                if (transportCollection.size() == 0) {
                    throw new ProbeException(ProbeStore.NO_TRANSPORTS_MESSAGE);
                }
                final Map<ITransport<MediationServerMessage, MediationClientMessage>, List<Pair<String, String>>>
                        targetsByTransport = reverse(targetIdToTransport);
                // find transport with the least number of targets
                targetTransport = Collections.min(transportCollection, Comparator.comparingInt(t ->
                        targetsByTransport.containsKey(t) ? targetsByTransport.get(t).size() : 0));
                targetIdToTransport.put(lookupKey, targetTransport);
                logger.info("Chose transport {} for target {}", targetTransport, lookupKey);
            }
            return targetTransport;
        }
    }

    private static boolean probeSupportsPersistentConnections(@Nonnull ProbeInfo probeInfo) {
        return probeInfo.hasIncrementalRediscoveryIntervalSeconds();
    }

    @Nonnull
    private ITransport<MediationServerMessage, MediationClientMessage> getTransportUsingRoundRobin(
            @Nonnull String probeType, @Nonnull Collection<ITransport<MediationServerMessage,
            MediationClientMessage>> transportCollection) throws ProbeException {
        if (transportCollection.size() == 0) {
            throw new ProbeException(ProbeStore.NO_TRANSPORTS_MESSAGE);
        }
        final int newIndex;
        synchronized (roundRobinIndexByProbeType) {
            roundRobinIndexByProbeType.putIfAbsent(probeType, -1);
            newIndex = (roundRobinIndexByProbeType.get(probeType) + 1) % transportCollection.size();
            roundRobinIndexByProbeType.put(probeType, newIndex);
        }
        logger.debug("Choosing container at index {}", newIndex);
        return transportCollection.stream().skip(newIndex).findAny().get();
    }

    /**
     * Assign the transport to a target.
     * @param  transport the transport to assign of the probe
     * @param probeType the probe type
     * @param targetIdentifyingValues the serialized identifying field of a target
     */
    private void assignTargetToTransport(
        @Nonnull ITransport<MediationServerMessage, MediationClientMessage> transport,
        @Nonnull String probeType,
        @Nonnull String targetIdentifyingValues) {
        final Pair<String, String> lookupKey = new Pair<>(probeType,
            targetIdentifyingValues);

        logger.debug("Assigning target {} to transport {}", lookupKey, transport);
        ITransport<MediationServerMessage, MediationClientMessage> oldTransport =
                        targetIdToTransport.put(lookupKey, transport);
        if (oldTransport != null && !transport.equals(oldTransport)) {
            logger.warn("Transport {} for target {} being replaced with transport {}",
                targetIdToTransport.get(lookupKey), targetIdentifyingValues,
                transport);
        }
    }

    @Override
    public void onTransportRegistered(@Nonnull final ContainerInfo containerInfo,
            @Nonnull final ITransport<MediationServerMessage, MediationClientMessage> transport) {
        if (containerInfo.getPersistentTargetIdMapMap().isEmpty()) {
            // if the map is empty, it means this is a new transport
            containerInfo.getProbesList().forEach(probeInfo ->
                    scheduleRebalanceTask(probeInfo.getProbeType()));
        } else {
            // if map is not empty, it means this is a register after TP restarts,
            // then this was already rebalanced and no need to rebalance again
            containerInfo.getPersistentTargetIdMapMap()
                    .forEach((probeType, targetIdSet) -> targetIdSet.getTargetIdList()
                            .forEach(targetId -> assignTargetToTransport(
                                    transport, probeType, targetId)));
        }
    }

    @Override
    public void onTransportRemoved(ITransport<MediationServerMessage, MediationClientMessage> transport) {
        final Set<String> probeTypes = new HashSet<>();
        final List<Pair<String, String>> removed = new ArrayList<>();
        synchronized (targetIdToTransport) {
            // remove targets assigned to this transport
            final Iterator<Entry<Pair<String, String>, ITransport<MediationServerMessage, MediationClientMessage>>>
                    iterator = targetIdToTransport.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<Pair<String, String>, ITransport<MediationServerMessage, MediationClientMessage>>
                        next = iterator.next();
                if (next.getValue() == transport) {
                    iterator.remove();
                    probeTypes.add(next.getKey().getFirst());
                    removed.add(next.getKey());
                }
            }
        }
        if (!removed.isEmpty()) {
            logger.info("Removed following targets from transport {}: {}", transport, removed);
        }
        // schedule rebalance
        probeTypes.forEach(this::scheduleRebalanceTask);
    }

    @Override
    public ITransport<MediationServerMessage, MediationClientMessage> getTransportByTargetId(Pair<String, String> targetId) {
        return targetIdToTransport.get(targetId);
    }

    @Override
    public void onTargetAdded(Target target) throws ProbeException {
        choose(target);
    }

    @Override
    public void onTargetRemoved(Target target) throws ProbeException {
        // todo: do we need to trigger rebalance here? yeah!
        targetIdToTransport.remove(new Pair<>(target.getProbeInfo().getProbeType(),
                target.getSerializedIdentifyingFields()));
    }

    @Override
    public void registerRebalanceCallback(Consumer<List<TransportReassignment>> rebalanceCallback) {
        this.rebalanceCallback = rebalanceCallback;
    }

    private void scheduleRebalanceTask(String probeType) {
        // no need to rebalance for non-persistent probes, since they always use round robin
        Optional<ProbeInfo> probeInfoForType = probeStore.getProbeInfoForType(probeType);
        if (!probeInfoForType.isPresent() || !probeSupportsPersistentConnections(probeInfoForType.get())) {
            return;
        }

        // only rebalance whenever a new transport registered and 30 seconds have passed without
        // new transport registered, we cancel previous scheduled task (wait for it to finish if
        // it's still in progress)
        synchronized (rebalanceTaskByProbeType) {
            ScheduledFuture<?> removed = rebalanceTaskByProbeType.remove(probeType);
            if (removed != null) {
                removed.cancel(false);
            }
            ScheduledFuture<?> task = scheduledExecutorService.schedule(
                    () -> rebalance(probeType), targetRebalanceDelaySec,
                    TimeUnit.SECONDS);
            rebalanceTaskByProbeType.put(probeType, task);
        }
    }

    /**
     * Rebalance all the targets for the given probe type among available transports. This is
     * triggered when a transport is registered or removed.
     *
     * @param probeType the probe type whose targets should be rebalanced
     */
    @VisibleForTesting
    public synchronized void rebalance(String probeType) {
        // get list of transports for this probe
        Optional<Long> probeId = probeStore.getProbeIdForType(probeType);
        if (!probeId.isPresent()) {
            // should not happen unless the probe is removed from probestore immediately after register
            logger.warn("Skipping rebalancing targets for probe type {} since probe id is not available", probeType);
            return;
        }

        Collection<ITransport<MediationServerMessage, MediationClientMessage>> allTransportsForProbe;
        try {
            allTransportsForProbe = probeStore.getTransport(probeId.get());
        } catch (ProbeException e) {
            logger.error("Error when retrieving transports for probe {}", probeType, e);
            allTransportsForProbe = Collections.emptyList();
        }

        final Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Starting rebalancing targets for probe type {}, current mapping: {}",
                probeType, reverse(targetIdToTransport));
        final List<Target> allTargets = targetStore.getProbeTargets(probeId.get());
        final Map<Pair<String, String>, Target> targetById = allTargets.stream()
                .collect(Collectors.toMap(t -> new Pair<>(probeType,
                        t.getSerializedIdentifyingFields()), t -> t));
        final Map<ITransport<MediationServerMessage, MediationClientMessage>, Set<Target>>
                existingTargetsByTransport = targetIdToTransport.entrySet().stream()
                .filter(e -> targetById.get(e.getKey()) != null)
                .collect(Collectors.groupingBy(Entry::getValue,
                        Collectors.mapping(e -> targetById.get(e.getKey()), Collectors.toSet())));

        // 1. first assign all targets with channel to transport with same channel
        final Map<String, List<Target>> targetsByChannel = allTargets.stream()
                .filter(t -> t.getSpec().hasCommunicationBindingChannel())
                .collect(Collectors.groupingBy(t -> t.getSpec().getCommunicationBindingChannel()));

        final Map<String, List<ITransport<MediationServerMessage, MediationClientMessage>>>
                transportsByChannel = new HashMap<>();
        final List<ITransport<MediationServerMessage, MediationClientMessage>>
                transportsWithoutChannel = new ArrayList<>();
        allTransportsForProbe.forEach(t -> {
            Optional<String> channel = probeStore.getChannel(t);
            if (channel.isPresent()) {
                transportsByChannel.computeIfAbsent(channel.get(), c -> new ArrayList<>()).add(t);
            } else {
                transportsWithoutChannel.add(t);
            }
        });

        final MinMaxPriorityQueue<TransportInfo> minMaxHeapGlobal = MinMaxPriorityQueue
                .orderedBy(TARGET_REBALANCE_COMPARATOR)
                .create();

        targetsByChannel.forEach((channel, targets) -> {
            final List<ITransport<MediationServerMessage, MediationClientMessage>> transports =
                    transportsByChannel.get(channel);
            if (transports != null) {
                MinMaxPriorityQueue<TransportInfo> minMaxHeap = balance(transports, targets,
                        existingTargetsByTransport);
                minMaxHeapGlobal.addAll(minMaxHeap);
            }
        });

        // 2. then rebalance other non-assigned targets without channel on transport without channel
        List<Target> targetsWithoutChannelAndNoTransport = allTargets.stream()
                .filter(t -> !t.getSpec().hasCommunicationBindingChannel())
                .filter(t -> existingTargetsByTransport.values().stream().noneMatch(set -> set.contains(t)))
                .collect(Collectors.toList());
        // allTransportsForProbe
        if (!transportsWithoutChannel.isEmpty()) {
            MinMaxPriorityQueue<TransportInfo> minMaxHeap = balance(transportsWithoutChannel,
                    targetsWithoutChannelAndNoTransport, existingTargetsByTransport);
            minMaxHeapGlobal.addAll(minMaxHeap);

            // 3. try to rebalance again if needed, since target in transport with channel can go
            // to transport without channel
            PriorityQueue<TransportInfo> maxHeap = new PriorityQueue<>(TARGET_REBALANCE_COMPARATOR_REVERSE);
            // only contain the transports without channel
            maxHeap.addAll(minMaxHeap);
            // use minMaxHeapGlobal contain all transports (with channel & without channel)

            if (!maxHeap.isEmpty() && !minMaxHeapGlobal.isEmpty()) {
                while (maxHeap.peek().targetCount() > minMaxHeapGlobal.peek().targetCount() + 1) {
                    TransportInfo max = maxHeap.poll();
                    TransportInfo min = minMaxHeapGlobal.poll();

                    Target removed = max.pollTarget();
                    min.addTarget(removed);

                    minMaxHeapGlobal.offer(min);
                    maxHeap.offer(max);

                    // re-heap max again since its value changed
                    minMaxHeapGlobal.remove(max);
                    minMaxHeapGlobal.offer(max);
                }
            }
        }

        // build the target to transport map
        final Map<Pair<String, String>, ITransport<MediationServerMessage, MediationClientMessage>>
                newTargetIdToTransport = new HashMap<>();
        minMaxHeapGlobal.forEach(transportInfo -> {
            transportInfo.getTargets().forEach(target -> {
                newTargetIdToTransport.put(new Pair<>(probeType,
                        target.getSerializedIdentifyingFields()), transportInfo.transport);
            });
        });

        // find old/new transport changes and reassign discovery queue contents
        if (rebalanceCallback != null) {
            List<TransportReassignment> changes = new ArrayList<>();
            newTargetIdToTransport.forEach((targetId, newTransport) -> {
                ITransport<MediationServerMessage, MediationClientMessage> oldTransport =
                        targetIdToTransport.get(targetId);
                if (oldTransport != null && newTransport != oldTransport) {
                    Target target = targetById.get(targetId);
                    if (target != null) {
                        changes.add(new TransportReassignment(target, oldTransport, newTransport));
                    }
                }
            });
            if (!changes.isEmpty()) {
                rebalanceCallback.accept(changes);
            }
        }

        // update mapping
        targetIdToTransport.entrySet().removeIf(entry -> entry.getKey().getFirst().equals(probeType));
        targetIdToTransport.putAll(newTargetIdToTransport);

        logger.info("Done rebalancing targets for probe type {} in {} ms, new mapping: {}",
                probeType, stopwatch.elapsed(TimeUnit.MILLISECONDS), reverse(targetIdToTransport));
    }

    private Map<ITransport<MediationServerMessage, MediationClientMessage>, List<Pair<String, String>>> reverse(
            Map<Pair<String, String>, ITransport<MediationServerMessage, MediationClientMessage>> targetIdToTransport) {
        return targetIdToTransport.entrySet().stream().collect(
                Collectors.groupingBy(Entry::getValue,
                        Collectors.mapping(Entry::getKey, Collectors.toList())));
    }

    /**
     * Balance the given targets on the given transports. If transports is empty, then the targets
     * can't be assigned to any transport, thus it will return an empty heap. If the targets to
     * assign transports is empty, then it will return the transports with existing assigned targets.
     *
     * @param transports the transports to balance targets on
     * @param targets the targets to assign transport
     * @param existingTargetsByTransport existing assigned targets for each transport
     * @return heap containing assigned targets for each transport
     */
    private static MinMaxPriorityQueue<TransportInfo> balance(
            List<ITransport<MediationServerMessage, MediationClientMessage>> transports,
            List<Target> targets,
            Map<ITransport<MediationServerMessage, MediationClientMessage>, Set<Target>> existingTargetsByTransport) {
        final MinMaxPriorityQueue<TransportInfo> minMaxHeap = MinMaxPriorityQueue
                .orderedBy(Comparator.comparingInt(TransportInfo::targetCount))
                .create();

        // sanity check
        if (transports.isEmpty()) {
            return minMaxHeap;
        }

        transports.forEach(transport -> {
            TransportInfo transportInfo = new TransportInfo(transport);
            existingTargetsByTransport.getOrDefault(transport, Collections.emptySet())
                    .forEach(existingTarget -> {
                        transportInfo.addTarget(existingTarget);
                        // if the target is already assigned to a transport, then remove it from
                        // the targets, so we don't change existing assignment at this point
                        targets.remove(existingTarget);
                    });
            minMaxHeap.offer(transportInfo);
        });

        // assign all targets not assigned yet
        Iterator<Target> iterator = targets.iterator();
        while (iterator.hasNext()) {
            Target next = iterator.next();
            // find the transport with least number of targets, and assign target there
            TransportInfo transportInfo = minMaxHeap.pollFirst();
            transportInfo.addTarget(next);
            // update existingTargetsByTransport
            existingTargetsByTransport.computeIfAbsent(transportInfo.transport, k -> new HashSet<>()).add(next);
            // add back
            minMaxHeap.offer(transportInfo);
            // remove this target
            iterator.remove();
        }

        // ensure the targets are balanced among transports: target count between min/max
        // difference is no more than 1, so in the end the number of targets for all transports
        // will be either n or n + 1
        while (minMaxHeap.peekLast().targetCount() > minMaxHeap.peekFirst().targetCount() + 1) {
            TransportInfo min = minMaxHeap.pollFirst();
            TransportInfo max = minMaxHeap.pollLast();

            Target removed = max.pollTarget();
            min.addTarget(removed);
            // update existingTargetsByTransport
            existingTargetsByTransport.computeIfAbsent(max.transport, k -> new HashSet<>()).remove(removed);
            existingTargetsByTransport.computeIfAbsent(min.transport, k -> new HashSet<>()).add(removed);

            minMaxHeap.offer(min);
            minMaxHeap.offer(max);
        }

        return minMaxHeap;
    }

    /**
     * Class for temporarily keeping track of transport and assigned targets.
     */
    static class TransportInfo {
        private final ITransport<MediationServerMessage, MediationClientMessage> transport;
        private final List<Target> targets = new ArrayList<>();

        TransportInfo(ITransport<MediationServerMessage, MediationClientMessage> transport) {
            this.transport = transport;
        }

        List<Target> getTargets() {
            return targets;
        }

        void addTarget(Target target) {
            this.targets.add(target);
        }

        Target pollTarget() {
            return this.targets.remove(targets.size() - 1);
        }

        int targetCount() {
            return targets.size();
        }
    }

    /**
     * Class describing the target whose transport was reassigned.
     */
    public static class TransportReassignment {
        private final Target target;
        private final ITransport<MediationServerMessage, MediationClientMessage> oldTransport;
        private final ITransport<MediationServerMessage, MediationClientMessage> newTransport;

        /**
         * Constructor.
         *
         * @param target the target whose transport was changed
         * @param oldTransport old transport which was used for discovering the target
         * @param newTransport new transport which is going to be used for discovering the target
         */
        public TransportReassignment(Target target,
                ITransport<MediationServerMessage, MediationClientMessage> oldTransport,
                ITransport<MediationServerMessage, MediationClientMessage> newTransport) {
            this.target = target;
            this.oldTransport = oldTransport;
            this.newTransport = newTransport;
        }

        public Target getTarget() {
            return target;
        }

        public ITransport<MediationServerMessage, MediationClientMessage> getOldTransport() {
            return oldTransport;
        }

        public ITransport<MediationServerMessage, MediationClientMessage> getNewTransport() {
            return newTransport;
        }
    }
}
