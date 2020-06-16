package com.vmturbo.topology.processor.topology.pipeline.blocking;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;

/**
 * Contains configuration relevant to the target short-circuiting logic for the
 * {@link DiscoveryBasedUnblock}.
 */
public class TargetShortCircuitSpec {
    private static final Logger logger = LogManager.getLogger();

    private final int fastRediscoveryThreshold;
    private final int slowRediscoveryThreshold;
    private final long fastSlowBoundaryMs;

    private TargetShortCircuitSpec(int fastRediscoveryThreshold,
            int slowRediscoveryThreshold,
            long fastSlowBoundaryMs) {
        this.fastRediscoveryThreshold = fastRediscoveryThreshold;
        this.slowRediscoveryThreshold = slowRediscoveryThreshold;
        this.fastSlowBoundaryMs = fastSlowBoundaryMs;
    }

    @Nonnull
    static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder class for the {@link TargetShortCircuitSpec}.
     */
    public static class Builder {
        private int fastRediscoveryThreshold = 3;
        private int slowRediscoveryThreshold = 1;
        private long fastSlowBoundaryMs = TimeUnit.MINUTES.toMillis(15);

        Builder setSlowRediscoveryThreshold(int slowRediscoveryThreshold) {
            this.slowRediscoveryThreshold = slowRediscoveryThreshold;
            return this;
        }

        Builder setFastSlowBoundary(long fastSlowBoundary, TimeUnit timeUnit) {
            this.fastSlowBoundaryMs = timeUnit.toMillis(fastSlowBoundary);
            return this;
        }

        Builder setFastRediscoveryThreshold(int fastRediscoveryThreshold) {
            this.fastRediscoveryThreshold = fastRediscoveryThreshold;
            return this;
        }

        TargetShortCircuitSpec build() {
            return new TargetShortCircuitSpec(fastRediscoveryThreshold, slowRediscoveryThreshold, fastSlowBoundaryMs);
        }
    }

    /**
     * Get the number of acceptable failures for a target. Once a target has this many discoveries
     * fail we should no longer wait for it to be discovered to unblock broadcasts.
     *
     * @param targetId The id of the target.
     * @param scheduler The scheduler.
     * @return The failure threshold for the target.
     */
    public int getFailureThreshold(final long targetId, @Nonnull final Scheduler scheduler) {
        Optional<TargetDiscoverySchedule> targetSchedule =
                scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL);
        if (targetSchedule.isPresent()) {
            final long scheduleIntervalMs = targetSchedule.get().getScheduleData().getScheduleIntervalMillis();
            if (scheduleIntervalMs > fastSlowBoundaryMs) {
                return slowRediscoveryThreshold;
            } else {
                return fastRediscoveryThreshold;
            }
        } else {
            // This should not happen, because in order to have had a discovery we must
            // have a schedule for the discovery.
            logger.warn("Discovered target {} has no associated schedule", this);
            return 1;
        }
    }
}
