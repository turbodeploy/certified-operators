package com.vmturbo.topology.processor.topology.pipeline.blocking;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
    private final long targetShortCircuitMs;
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
            final long targetShortCircuit,
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
        this.targetShortCircuitMs = timeUnit.toMillis(targetShortCircuit);
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

    boolean runIteration() {
        // We get all existing targets here, since some discoveries can add derived
        // targets.
        final List<Target> existingTargets = targetStore.getAll();
        // If the user deleted some targets we no longer want to wait for them to be
        // discovered.
        targetDiscoveryInfoMap.keySet().retainAll(existingTargets.stream()
                .map(Target::getId)
                .collect(Collectors.toSet()));

        existingTargets.forEach(target -> {
            final TargetDiscoveryInfo info = targetDiscoveryInfoMap.computeIfAbsent(target.getId(),
                    targetId -> new TargetDiscoveryInfo(targetId,
                            scheduler,
                            target.getDisplayName(),
                            targetShortCircuitMs,
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
            List<TargetDiscoveryInfo> exceedThreshold = byStatus.get(TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD);
            if (!CollectionUtils.isEmpty(exceedThreshold)) {
                logger.warn("{}/{} targets exceeded failure threshold. Unblocking broadcasts."
                                + " Failing targets:\n{}", exceedThreshold.size(),
                        existingTargets.size(),
                        exceedThreshold.stream()
                                .map(TargetDiscoveryInfo::toString)
                                .collect(Collectors.joining("\n")));
            } else {
                logger.info("All {} targets have finished discovery. Unblocking broadcasts.",
                        existingTargets.size());
            }
            logger.info("Target counts by status: {}", byStatus.entrySet().stream()
                    .map(e -> e.getKey() + " : " + e.getValue().size())
                    .collect(Collectors.joining(",")));
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
                logger.info("Not all discoveries complete after waiting for {}ms. Timed out.",
                        now - startMillis);
                // Timed out, so we are done waiting.
                return true;
            }
        }
    }

    @Override
    public void run() {
        try {
            if (enableDiscoveryResponsesCaching) {
                binaryDiscoveryDumper.restoreDiscoveryResponses(targetStore).forEach((key, discoveryResponse) -> {
                    long targetId = key;
                    Optional<Target> target = targetStore.getTarget(targetId);
                    if (target.isPresent()) {
                        // TODO: (MarcoBerlot 5/26/20) do not fake the discovery object
                        Discovery operation = new Discovery(target.get().getProbeId(),
                            targetId, identityProvider);
                        operationManager.notifyDiscoveryResult(operation, discoveryResponse);
                        TargetDiscoveryInfo info = targetDiscoveryInfoMap.computeIfAbsent(targetId,
                            k -> new TargetDiscoveryInfo(targetId, scheduler, target.get().getDisplayName(),
                                    targetShortCircuitMs, maxProbeRegistrationWaitPeriodMs));
                        // This target should be considered successfully discovered.
                        info.changeStatus(TargetWaitingStatus.RESTORED_FROM_DISK);
                    }
                });
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
        private final long targetShortCircuitMs;
        private final Scheduler scheduler;

        /**
         * If we get to this threshold and there is no discovery for the target, stop waiting for it.
         */
        private final long probeRegistrationThresholdMs;

        private TargetWaitingStatus status = TargetWaitingStatus.WAITING;

        TargetDiscoveryInfo(final long targetId,
                            @Nonnull final Scheduler scheduler,
                            final String displayName,
                            final long targetShortCircuitMs,
                            final long probeRegistrationThresholdMs) {
            this.targetId = targetId;
            this.scheduler = scheduler;
            this.displayName = displayName;
            this.targetShortCircuitMs = targetShortCircuitMs;
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
                // If we've never had a discovery complete, the probe is still not registered,
                // and we've reached the probe registration threshold...
                if (lastDiscoveryId == 0 && !probeRegistered
                        && clock.millis() >= probeRegistrationThresholdMs) {
                    changeStatus(TargetWaitingStatus.PROBE_NOT_REGISTERED);
                }
            } else {
                processLastDiscovery(discoveryOpt.get());
            }
        }

        private void processLastDiscovery(@Nonnull final Discovery discovery) {
            if (discovery.getStatus() == Status.SUCCESS) {
                changeStatus(TargetWaitingStatus.SUCCESS);
                status = TargetWaitingStatus.SUCCESS;
            } else if (discovery.getStatus() == Status.FAILED && discovery.getId() != lastDiscoveryId) {
                numFailedDiscoveries++;
                final int shortCircuitThreshold = scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL)
                        // The short circuit threshold is however many re-discovery intervals fit within
                        // the targetShortCircuitMs timeframe.
                        //
                        // e.g. if "targetShortCircuitMs" is 30 minutes and the target gets
                        // gets rediscovered every 10 minutes, the threshold should be 3.
                        .map(schedule -> Math.max(1, (int)(targetShortCircuitMs / schedule.getScheduleData().getScheduleIntervalMillis())))
                        // This should not happen, because in order to have had a discovery we must
                        // have a schedule for the discovery.
                        .orElseGet(() -> {
                            logger.warn("Discovered target {} has no associated schedule", this);
                            return 1;
                        });
                logger.debug("{} new failed discovery detected. Total failed discovery count: {}. Short-circuit threshold: {}",
                        this, numFailedDiscoveries, shortCircuitThreshold);
                if (numFailedDiscoveries >= shortCircuitThreshold) {
                    changeStatus(TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD);
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
                logger.trace("{} transitioning from {} to {}", this, status, newStatus);
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
