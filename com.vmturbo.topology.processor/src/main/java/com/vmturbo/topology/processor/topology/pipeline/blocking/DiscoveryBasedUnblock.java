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
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.blocking.PipelineUnblockFactory.PipelineUnblock;

/**
 * Unblocks the {@link TopologyPipelineExecutorService} when all targets have completed discovery.
 */
public class DiscoveryBasedUnblock implements PipelineUnblock {
    private static final Logger logger = LogManager.getLogger();
    private static final long SLEEP_BETWEEN_CYCLES_MS = 10_000;
    private final TopologyPipelineExecutorService pipelineExecutorService;
    private final TargetStore targetStore;
    private final IOperationManager operationManager;
    private final Clock clock;
    private final int targetShortCircuitCount;
    private final long maxDiscoveryWaitMs;
    private final long maxProbeRegistrationWaitPeriodMs;

    private final Map<Long, TargetDiscoveryInfo> targetDiscoveryInfoMap = new HashMap<>();
    private final long startMillis;
    private final long endMillis;

    // Use the factory.
    private DiscoveryBasedUnblock(@Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
            @Nonnull final TargetStore targetStore,
            @Nonnull final IOperationManager operationManager,
            final int targetShortCircuitCount,
            @Nonnull final Clock clock,
            final long maxDiscoveryWaitPeriodMs,
            final long maxProbeRegistrationWaitPeriodMs) {
        this.pipelineExecutorService = pipelineExecutorService;
        this.targetStore = targetStore;
        this.operationManager = operationManager;
        this.clock = clock;
        this.targetShortCircuitCount = targetShortCircuitCount;
        this.maxDiscoveryWaitMs = maxDiscoveryWaitPeriodMs;
        startMillis = clock.millis();
        endMillis = clock.millis() + maxDiscoveryWaitPeriodMs;
        this.maxProbeRegistrationWaitPeriodMs = maxProbeRegistrationWaitPeriodMs;
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
        PROBE_NOT_REGISTERED;
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
                            targetShortCircuitCount,
                            // The "threshold" is "maxProbeRegistrationWaitPeriodMs" after we first see the target.
                            clock.millis() + maxProbeRegistrationWaitPeriodMs));
            Optional<Discovery> discovery = operationManager.getLastDiscoveryForTarget(target.getId(), DiscoveryType.FULL);
            info.updateDiscovery(discovery, clock);
        });

        final Map<TargetWaitingStatus, List<TargetDiscoveryInfo>> byStatus = targetDiscoveryInfoMap.values().stream()
                .collect(Collectors.groupingBy(TargetDiscoveryInfo::getStatus));
        List<TargetDiscoveryInfo> waitingCnt = byStatus.get(TargetWaitingStatus.WAITING);
        if (CollectionUtils.isEmpty(waitingCnt)) {
            List<TargetDiscoveryInfo> exceedThreshold = byStatus.get(TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD);
            if (!CollectionUtils.isEmpty(exceedThreshold)) {
                logger.warn("{}/{} targets exceeded {} failures. Unblocking broadcasts."
                                + " Failing targets:\n{}", exceedThreshold.size(),
                        existingTargets.size(),
                        targetShortCircuitCount,
                        exceedThreshold.stream()
                                .map(t -> t.toString())
                                .collect(Collectors.joining(", ")));
            } else {
                logger.info("All {} targets have finished discovery. Unblocking broadcasts.",
                        existingTargets.size());
            }
            return true;
        } else {
            logger.info("{}/{} are still not successfully discovered."
                            + " Waiting for remaining discoveries before allowing broadcasts.",
                    waitingCnt.size(), existingTargets.size());
            logger.debug("Targets by status:\n{}", () -> byStatus.entrySet().stream()
                    .map(e -> e.getKey() + " : [\n" + e.getValue().stream()
                            .map(targetDiscoveryInfo -> targetDiscoveryInfo.toString())
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
        private long lastDiscoveryId = 0;
        private int numFailedDiscoveries = 0;
        private final int shortCircuitThreshold;

        /**
         * If we get to this threshold and there is no discovery for the target, stop waiting for it.
         */
        private final long probeRegistrationThresholdMs;

        private TargetWaitingStatus status = TargetWaitingStatus.WAITING;

        TargetDiscoveryInfo(final long targetId,
                            final int shortCircuitThreshold,
                            final long probeRegistrationThresholdMs) {
            this.targetId = targetId;
            this.shortCircuitThreshold = shortCircuitThreshold;
            this.probeRegistrationThresholdMs = probeRegistrationThresholdMs;
        }

        void updateDiscovery(Optional<Discovery> discoveryOpt, Clock clock) {
            // Early return if we already had a successful discovery, or already had the "fatal"
            // number of failed discoveries.
            if (status == TargetWaitingStatus.FAILURE_EXCEED_THRESHOLD
                    || status == TargetWaitingStatus.SUCCESS) {
                return;
            }

            if (!discoveryOpt.isPresent()) {
                // If we've never had a discovery yet, and we've reached the probe registration
                // threshold.
                if (lastDiscoveryId == 0 && clock.millis() >= probeRegistrationThresholdMs) {
                    changeStatus(TargetWaitingStatus.PROBE_NOT_REGISTERED);
                }
            } else {
                final Discovery discovery = discoveryOpt.get();
                if (discovery.getStatus() == Status.SUCCESS) {
                    changeStatus(TargetWaitingStatus.SUCCESS);
                    status = TargetWaitingStatus.SUCCESS;
                } else if (discovery.getStatus() == Status.FAILED && discovery.getId() != lastDiscoveryId) {
                    numFailedDiscoveries++;
                    logger.trace("{} new failed discovery detected. Total failed discovery count: {}",
                         this, numFailedDiscoveries);
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
            return targetId + "(lastDiscovery : " + lastDiscoveryId
                + " , numFailedDiscoveries : " + numFailedDiscoveries + ")";
        }
    }

    /**
     * Factory class for {@link DiscoveryBasedUnblock} operations.
     */
    public static class DiscoveryBasedUnblockFactory implements PipelineUnblockFactory {
        private final TargetStore targetStore;
        private final IOperationManager operationManager;
        private final Clock clock;
        private final int targetShortCircuitCount;
        private final long maxDiscoveryWaitMs;
        private final long maxProbeRegistrationWaitMs;

        /**
         * Create a new instance of the factory.
         *
         * @param targetStore Target store to retrieve target information from.
         * @param operationManager For information about ongoing discoveries.
         * @param clock System clock.
         * @param targetShortCircuitCount Maximum number of tolerated failed discoveries before
         *                                we stop waiting for the "bad" target.
         * @param maxDiscoveryWait Maximum time to wait for all targets to be discovered.
         * @param maxProbeRegistrationWait Maximum time to wait for probes to be registered.
         * @param maxDiscoveryWaitTimeUnit Time unit for the max wait time.
         */
        public DiscoveryBasedUnblockFactory(@Nonnull final TargetStore targetStore,
                @Nonnull final IOperationManager operationManager,
                @Nonnull final Clock clock,
                final int targetShortCircuitCount,
                final long maxDiscoveryWait,
                final long maxProbeRegistrationWait,
                TimeUnit maxDiscoveryWaitTimeUnit) {
            this.targetStore = targetStore;
            this.operationManager = operationManager;
            this.clock = clock;
            this.targetShortCircuitCount = targetShortCircuitCount;
            this.maxProbeRegistrationWaitMs = maxDiscoveryWaitTimeUnit.toMillis(maxProbeRegistrationWait);
            this.maxDiscoveryWaitMs = maxDiscoveryWaitTimeUnit.toMillis(maxDiscoveryWait);
        }

        @Override
        public DiscoveryBasedUnblock newUnblockOperation(@Nonnull final TopologyPipelineExecutorService pipelineExecutorService) {
            return new DiscoveryBasedUnblock(pipelineExecutorService, targetStore,
                    operationManager, targetShortCircuitCount, clock,
                    maxDiscoveryWaitMs, maxProbeRegistrationWaitMs);
        }

    }
}
