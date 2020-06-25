package com.vmturbo.topology.processor.topology.pipeline.blocking;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.RemoteProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Unblocks the {@link TopologyPipelineExecutorService} when all targets have completed discovery.
 */
public class DiscoveryBasedUnblock implements PipelineUnblock {
    private static final Logger logger = LogManager.getLogger();
    private static final long SLEEP_BETWEEN_CYCLES_MS = 10_000;
    private final TopologyPipelineExecutorService pipelineExecutorService;
    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final Scheduler scheduler;
    private final IOperationManager operationManager;
    private final Clock clock;
    private final TargetShortCircuitSpec targetShortCircuitSpec;
    private final long maxDiscoveryWaitMs;
    private final long maxProbeRegistrationWaitPeriodMs;

    private final Map<Long, TargetDiscoveryInfo> targetDiscoveryInfoMap = new HashMap<>();
    private final long startMillis;
    private final long endMillis;
    private final IdentityProvider identityProvider;
    private BinaryDiscoveryDumper binaryDiscoveryDumper;
    private boolean enableDiscoveryResponsesCaching;

    DiscoveryBasedUnblock(@Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
            @Nonnull final TargetStore targetStore,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final Scheduler scheduler,
            @Nonnull final IOperationManager operationManager,
            TargetShortCircuitSpec targetShortCircuitSpec,
            final long maxDiscoveryWaitPeriod,
            final long maxProbeRegistrationWaitPeriod,
            @Nonnull final TimeUnit timeUnit,
            @Nonnull final Clock clock,
            @Nonnull final IdentityProvider identityProvider,
            BinaryDiscoveryDumper binaryDiscoveryDumper,
            boolean enableDiscoveryResponsesCaching) {
        this.pipelineExecutorService = pipelineExecutorService;
        this.targetStore = targetStore;
        this.probeStore = probeStore;
        this.scheduler = scheduler;
        this.operationManager = operationManager;
        this.clock = clock;
        this.targetShortCircuitSpec = targetShortCircuitSpec;
        this.maxDiscoveryWaitMs = timeUnit.toMillis(maxDiscoveryWaitPeriod);
        startMillis = clock.millis();
        endMillis = clock.millis() + this.maxDiscoveryWaitMs;
        this.maxProbeRegistrationWaitPeriodMs = timeUnit.toMillis(maxProbeRegistrationWaitPeriod);
        this.identityProvider = identityProvider;
        this.binaryDiscoveryDumper = binaryDiscoveryDumper;
        this.enableDiscoveryResponsesCaching = enableDiscoveryResponsesCaching;
    }

    /**
     * The status of a particular target w.r.t. unblocking broadcasts.
     */
    enum TargetWaitingStatus {
        /**
         * The target has not had a successful discovery yet.
         */
        WAITING,

        /**
         * The target has had a successful discovery.
         */
        SUCCESS,

        /**
         * The target has failed more than the threshold number of times. We won't wait for it
         * anymore.
         */
        FAILURE_EXCEED_THRESHOLD,

        /**
         * The target's probe did not register within the configured threshold. We do not wait for
         * targets with this status to be discovered before unblocking discovery. However, if the
         * target's probe DOES register while we're waiting for other targets, we transition back
         * to the "WAITING" status. This is why this state is not terminal.
         */
        PROBE_NOT_REGISTERED,

        /**
         * The target has correctly been loaded from a binary file persisted on disk.
         */
        RESTORED_FROM_DISK;
    }

    @Nonnull
    private List<Target> refreshExistingTargets() {
        // We get all existing targets here, since some discoveries can add derived
        // targets.
        final List<Target> existingTargets = targetStore.getAll();
        // If the user deleted some targets we no longer want to wait for them to be
        // discovered.
        targetDiscoveryInfoMap.keySet().retainAll(existingTargets.stream()
                .map(Target::getId)
                .collect(Collectors.toSet()));

        return existingTargets;
    }

    boolean runIteration() {
        final List<Target> existingTargets = refreshExistingTargets();

        existingTargets.forEach(target -> {
            final TargetDiscoveryInfo info = targetDiscoveryInfoMap.computeIfAbsent(target.getId(),
                    targetId -> new TargetDiscoveryInfo(targetId,
                            scheduler,
                            target.getDisplayName(),
                            targetShortCircuitSpec,
                            // The "threshold" is "maxProbeRegistrationWaitPeriodMs" after we first see the target.
                            clock.millis() + maxProbeRegistrationWaitPeriodMs));
            // Check to make sure that the target's probe is registered.
            boolean probeRegistered;
            try {
                probeRegistered = !probeStore.getTransport(target.getProbeId()).isEmpty();
            } catch (ProbeException e) {
                // The probe is not registered.
                logger.debug("Probe {} for target {} does not exist.", target.getProbeId(), target.getId());
                probeRegistered = false;
            }

            // First check if we already have a completed discovery for the target.
            Optional<Discovery> discovery = operationManager.getLastDiscoveryForTarget(target.getId(), DiscoveryType.FULL);
            info.updateDiscovery(discovery, clock, probeRegistered);
        });

        final Map<TargetWaitingStatus, List<TargetDiscoveryInfo>> byStatus = targetDiscoveryInfoMap.values().stream()
                .collect(Collectors.groupingBy(TargetDiscoveryInfo::getStatus));
        List<TargetDiscoveryInfo> waitingCnt = byStatus.get(TargetWaitingStatus.WAITING);
        if (CollectionUtils.isEmpty(waitingCnt)) {
            return true;
        } else {
            logger.info("{}/{} are still not successfully discovered."
                            + " Waiting for remaining discoveries before allowing broadcasts.",
                    waitingCnt.size(), existingTargets.size());
            logger.debug("Targets by status:\n{}", byStatus.entrySet().stream()
                    .map(e -> e.getKey() + " : [\n" + e.getValue().stream()
                            .map(TargetDiscoveryInfo::toString)
                            .collect(Collectors.joining("\n")) + "\n]")
                    .collect(Collectors.joining("\n")));
            final long now = clock.millis();
            if (now < endMillis) {
                // Not done waiting.
                return false;
            } else {
                logger.warn("Not all discoveries complete after waiting for {}ms. Timed out.",
                        now - startMillis);
                // Timed out, so we are done waiting.
                return true;
            }
        }
    }

    private void logResult() {
        final List<Target> existingTargets = refreshExistingTargets();

        final Map<TargetWaitingStatus, List<TargetDiscoveryInfo>> byStatus = targetDiscoveryInfoMap.values().stream()
                .collect(Collectors.groupingBy(TargetDiscoveryInfo::getStatus));

        final List<TargetDiscoveryInfo> exceedThreshold =
                byStatus.getOrDefault(TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD, Collections.emptyList());
        final List<TargetDiscoveryInfo> waiting =
                byStatus.getOrDefault(TargetWaitingStatus.WAITING, Collections.emptyList());
        final List<TargetDiscoveryInfo> probeNotRegistered =
                byStatus.getOrDefault(TargetWaitingStatus.PROBE_NOT_REGISTERED, Collections.emptyList());
        if (exceedThreshold.isEmpty() && waiting.isEmpty() && probeNotRegistered.isEmpty()) {
            logger.info("All {} targets have finished discovery. Unblocking broadcasts.",
                    existingTargets.size());
            logger.info("Target counts by status: {}", byStatus.entrySet().stream()
                    .map(e -> e.getKey() + " : " + e.getValue().size())
                    .collect(Collectors.joining(",")));
        } else {
            final int problematicCnt = exceedThreshold.size() + waiting.size() + probeNotRegistered.size();
            final StringJoiner targetsByStatus = new StringJoiner("\n");
            byStatus.forEach((status, targets) -> {
                targetsByStatus.add(status.name() + ":");
                targets.forEach(target -> targetsByStatus.add("    " + target.toString()));
            });
            logger.warn("{}/{} targets do not have results. Targets by status:\n{}",
                    problematicCnt,
                    existingTargets.size(),
                    targetsByStatus.toString());
        }
    }

    @Override
    public void run() {
        try {
            if (enableDiscoveryResponsesCaching) {
                try {
                    binaryDiscoveryDumper.restoreDiscoveryResponses(targetStore).forEach((key, discoveryResponse) -> {
                        long targetId = key;
                        Optional<Target> target = targetStore.getTarget(targetId);
                        if (target.isPresent()) {
                            // TODO: (MarcoBerlot 5/26/20) do not fake the discovery object
                            long probeId = target.get().getProbeId();
                            Discovery operation = new Discovery(probeId, targetId,
                                    identityProvider);

                            try {
                                operationManager.notifyDiscoveryResult(operation,
                                    discoveryResponse).get(5, TimeUnit.MINUTES);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                logger.error("Error in notifying the discovery result for target "
                                    + "id:{} and probe id: {}", targetId, probeId);
                                throw new RuntimeException(e);
                            }

                            TargetDiscoveryInfo info = targetDiscoveryInfoMap.computeIfAbsent(targetId,
                                    k -> new TargetDiscoveryInfo(targetId, scheduler,
                                            target.get().getDisplayName(), targetShortCircuitSpec,
                                            maxProbeRegistrationWaitPeriodMs));
                            // This target should be considered successfully discovered.
                            info.changeStatus(TargetWaitingStatus.RESTORED_FROM_DISK);
                        }
                    });
                } catch (RuntimeException e) {
                    logger.error("Failed to restore discovery responses from disk.", e);
                }
            }
            while (pipelineExecutorService.areBroadcastsBlocked()) {
                try {
                    if (runIteration()) {
                        break;
                    } else {
                        final long now = clock.millis();
                        // If there are less than 10 seconds until the expiry time, wait
                        // less.
                        Thread.sleep(Math.min(SLEEP_BETWEEN_CYCLES_MS, endMillis - now));
                    }
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting.", e);
                    break;
                }
            }
        } catch (RuntimeException e) {
            logger.error("Unexpected exception. Unblocking broadcasts early.", e);
        } finally {
            // Unblock the pipeline when done, even if we exit the loop due to some kind of exception.
            pipelineExecutorService.unblockBroadcasts();
            // Log the results, regardless of how we finished.
            logResult();
        }
    }

    /**
     * Per-target information, mainly to keep track of which targets have been discovered, and which
     * targets are failing to be discovered.
     */
    private static class TargetDiscoveryInfo {
        private final long targetId;
        private final String displayName;
        private long lastDiscoveryId = 0;
        private int numFailedDiscoveries = 0;
        private final TargetShortCircuitSpec targetShortCircuitSpec;
        private final Scheduler scheduler;

        /**
         * If we get to this threshold and there is no discovery for the target, stop waiting for it.
         */
        private final long probeRegistrationThresholdMs;

        private TargetWaitingStatus status = TargetWaitingStatus.WAITING;

        TargetDiscoveryInfo(final long targetId,
                            @Nonnull final Scheduler scheduler,
                            final String displayName,
                            final TargetShortCircuitSpec targetShortCircuitSpec,
                            final long probeRegistrationThresholdMs) {
            this.targetId = targetId;
            this.scheduler = scheduler;
            this.displayName = displayName;
            this.targetShortCircuitSpec = targetShortCircuitSpec;
            this.probeRegistrationThresholdMs = probeRegistrationThresholdMs;
        }

        void updateDiscovery(Optional<Discovery> discoveryOpt,
                Clock clock,
                final boolean probeRegistered) {
            // Early return if we already had a successful discovery, a loaded discovery from disk,
            // or  had the "fatal" number of failed discoveries.
            if (status == TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD
                    || status == TargetWaitingStatus.RESTORED_FROM_DISK
                    || status == TargetWaitingStatus.SUCCESS ) {
                return;
            }

            if (!discoveryOpt.isPresent()) {
                processProbeNotRegistered(clock);
            } else {
                processLastDiscovery(discoveryOpt.get(), clock);
            }
        }

        private void processProbeNotRegistered(Clock clock) {
            // If we've never had a discovery complete, the probe is still not registered,
            // and we've reached the probe registration threshold...
            if (lastDiscoveryId == 0 && clock.millis() >= probeRegistrationThresholdMs) {
                changeStatus(TargetWaitingStatus.PROBE_NOT_REGISTERED);
            }
        }

        private void processLastDiscovery(@Nonnull final Discovery discovery, @Nonnull final Clock clock) {
            if (discovery.getStatus() == Status.SUCCESS) {
                changeStatus(TargetWaitingStatus.SUCCESS);
                status = TargetWaitingStatus.SUCCESS;
            } else if (discovery.getStatus() == Status.FAILED && discovery.getId() != lastDiscoveryId) {
                if (discovery.getErrorString().contains(RemoteProbeStore.TRANSPORT_NOT_REGISTERED_PREFIX)) {
                    // This is kind of a corner case, but if the discovery fails because the probe
                    // for this target does not have a registered transport, we should count this
                    // as a "probe not registered" case, and not as a true failed discovery.
                    //
                    // Discoveries that fail due to probes not being registered get immediately
                    // re-scheduled when the probe registers.
                    //
                    // TODO (roman, Jun 8 2020: If possible, it would be good to keep the ProbeException
                    // in the discovery. That way we can check for the exception that caused the
                    // issue, instead of relying on the string messages.
                    processProbeNotRegistered(clock);
                    // Again, kind of a corner case, but we don't want to count this discovery as
                    // a real "last discovery", so we return early.
                    return;
                } else {
                    numFailedDiscoveries++;
                    final int shortCircuitThreshold = targetShortCircuitSpec.getFailureThreshold(targetId, scheduler);
                    logger.debug(
                            "{} new failed discovery detected. Total failed discovery count: {}. Short-circuit threshold: {}",
                            this, numFailedDiscoveries, shortCircuitThreshold);
                    if (numFailedDiscoveries >= shortCircuitThreshold) {
                        changeStatus(TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD);
                    }
                }
            } else if (status == TargetWaitingStatus.PROBE_NOT_REGISTERED) {
                // If this target's probe was not registered within the threshold,
                // but registered while we were waiting for other targets to be discovered,
                // transition the probe back to "WAITING", since we can now wait for the
                // discovery to fail or succeed.
                changeStatus(TargetWaitingStatus.WAITING);
            }
            lastDiscoveryId = discovery.getId();
        }

        private void changeStatus(TargetWaitingStatus newStatus) {
            if (newStatus != status) {
                logger.info("{} transitioning from {} to {}", this, status, newStatus);
                status = newStatus;
            }
        }

        TargetWaitingStatus getStatus() {
            return status;
        }

        @Override
        public String toString() {
            return displayName + "{id: " + targetId + " , lastDiscovery : " + lastDiscoveryId
                + " , numFailedDiscoveries : " + numFailedDiscoveries + "}";
        }
    }
}
