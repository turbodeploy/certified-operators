package com.vmturbo.topology.processor.scheduling;

import java.util.concurrent.ScheduledFuture;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * The discovery schedule for a target.
 * Used when discovering a target at fixed intervals.
 *
 * A {@link TargetDiscoverySchedule} may be synched to the broadcast schedule for topology broadcast.
 * These synched schedules should be updated when the topology broadcast schedule is updated.
 */
public class TargetDiscoverySchedule extends Schedule {
    private final long targetId;
    private final boolean synchedToBroadcast;

    /**
     * Create a new TargetDiscoverySchedule, specifying whether the discovery schedule should be
     * synched with the broadcast schedule..
     *
     * @param scheduledTask The task that actually executes the scheduled discovery
     *                      at fixed intervals.
     * @param targetId The ID of the target to discover.
     * @param discoveryIntervalMillis The interval at which to discover the target in milliseconds.
     * @param synchedToBroadcastSchedule Indicates that the discovery schedule for this target should be
     *                                   synched to the broadcast schedule for service entities. If true,
     *                                   when the broadcast schedule is changed, the schedule here should
     *                                   be changed as well.
     */
    TargetDiscoverySchedule(@Nonnull ScheduledFuture<?> scheduledTask, long targetId,
                            long discoveryIntervalMillis, boolean synchedToBroadcastSchedule) {
        super(scheduledTask, discoveryIntervalMillis);
        this.targetId = targetId;
        this.synchedToBroadcast = synchedToBroadcastSchedule;
    }

    /**
     * The ID of the target to which this schedule applies.
     *
     * @return The ID of the target to which this schedule applies.
     */
    public long getTargetId() {
        return targetId;
    }

    /**
     * Check if this discovery schedule is synched to the overall broadcast schedule.
     *
     * @return If this discovery schedule is synched to the overall broadcast schedule.
     */
    public boolean isSynchedToBroadcast() {
        return synchedToBroadcast;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduleData getScheduleData() {
        return new TargetDiscoveryScheduleData(scheduleIntervalMillis, synchedToBroadcast);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DiscoverySchedule for target " + targetId +
            ". Discovers every " + scheduleIntervalMillis + " ms synched=" + synchedToBroadcast;
    }

    @Immutable
    public static class TargetDiscoveryScheduleData extends ScheduleData {
        // this only applies to full discovery, since incremental discovery is designed to happen
        // much more frequently than broadcast
        private final boolean synchedToBroadcast;

        // incremental discovery interval, may be null if target does not support incremental
        // discovery, or -1 if incremental discovery is disabled by user
        private final Long incrementalIntervalMillis;

        public TargetDiscoveryScheduleData(long scheduleIntervalMillis, boolean synchedToBroadcast) {
            super(scheduleIntervalMillis);
            this.incrementalIntervalMillis = null;
            this.synchedToBroadcast = synchedToBroadcast;
        }

        public TargetDiscoveryScheduleData(long scheduleIntervalMillis,
                long incrementalIntervalMillis, boolean synchedToBroadcast) {
            super(scheduleIntervalMillis);
            this.incrementalIntervalMillis = incrementalIntervalMillis;
            this.synchedToBroadcast = synchedToBroadcast;
        }

        public long getFullIntervalMillis() {
            return super.getScheduleIntervalMillis();
        }

        public boolean isSynchedToBroadcast() {
            return synchedToBroadcast;
        }

        public boolean hasIncrementalIntervalMillis() {
            return incrementalIntervalMillis != null && incrementalIntervalMillis != -1;
        }

        public boolean isIncrementalDiscoveryDisabled() {
            return incrementalIntervalMillis != null && incrementalIntervalMillis == -1;
        }

        public Long getIncrementalIntervalMillis() {
            return incrementalIntervalMillis;
        }

        public static class Builder {
            private boolean synchedToBroadcast;
            private long fullIntervalMillis;
            // may be null if target does not support incremental discovery
            private Long incrementalIntervalMillis;

            public Builder setFullIntervalMillis(long fullIntervalMillis) {
                this.fullIntervalMillis = fullIntervalMillis;
                return this;
            }

            public Builder setIncrementalIntervalMillis(Long incrementalIntervalMillis) {
                this.incrementalIntervalMillis = incrementalIntervalMillis;
                return this;
            }

            public Builder clearIncrementalIntervalMillis() {
                this.incrementalIntervalMillis = null;
                return this;
            }

            public Builder setSynchedToBroadcast(boolean synchedToBroadcast) {
                this.synchedToBroadcast = synchedToBroadcast;
                return this;
            }

            public TargetDiscoveryScheduleData build() {
                if (incrementalIntervalMillis == null) {
                    return new TargetDiscoveryScheduleData(fullIntervalMillis, synchedToBroadcast);
                } else {
                    return new TargetDiscoveryScheduleData(fullIntervalMillis,
                        incrementalIntervalMillis, synchedToBroadcast);
                }
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(TargetDiscoveryScheduleData targetDiscoveryScheduleData) {
            return new Builder()
                .setFullIntervalMillis(targetDiscoveryScheduleData.getFullIntervalMillis())
                .setIncrementalIntervalMillis(targetDiscoveryScheduleData.getIncrementalIntervalMillis())
                .setSynchedToBroadcast(targetDiscoveryScheduleData.isSynchedToBroadcast());
        }
    }
}
