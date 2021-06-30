package com.vmturbo.topology.event.library.uptime;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;

/**
 * The uptime calculation of an entity. Uptime represents the time the entity was powered on over
 * some analyzed window of time.
 */
@HiddenImmutableImplementation
@Immutable
public interface EntityUptime {

    /**
     * Represents an unknown amount of uptime (indicated by zero {@link #totalTime()}), which may
     * occur in cases where an entity is newly launched or in cases where there are no topology events
     * for older VMs.
     */
    EntityUptime UNKNOWN_DEFAULT_TO_ALWAYS_ON = EntityUptime.builder()
            .uptime(Duration.ZERO)
            .totalTime(Duration.ZERO)
            .uptimePercentage(100.0)
            .build();

    /**
     * The uptime duration.
     * @return The uptime duration.
     */
    @Nonnull
    Duration uptime();

    /**
     * The total duration of the analyzed uptime window.
     * @return The total duration of the analyzed uptime window.
     */
    @Nonnull
    Duration totalTime();

    /**
     * The creation time of the entity. This will only be set when the creation time falls within
     * the analyzed uptime window.
     * @return The creation time of the entity.
     */
    @Nonnull
    Optional<Instant> creationTime();

    /**
     * The uptime percentage. This value is not directly derived from {@link #uptime()} and {@link #totalTime()},
     * in the case where the state of the entity over the analyzed uptime window is unknown.
     * @return The uptime percentage. This will be a value between 0.0 and 100.0.
     */
    double uptimePercentage();

    /**
     * Converts this {@link EntityUptime} instance to a protobuf message.
     * @return The converted {@link EntityUptimeDTO} protobuf message.
     */
    @Nonnull
    default EntityUptimeDTO toProtobuf() {

        final EntityUptimeDTO.Builder entityUptime = EntityUptimeDTO.newBuilder()
                .setUptimeDurationMs(uptime().toMillis())
                .setTotalDurationMs(totalTime().toMillis())
                .setUptimePercentage(uptimePercentage());

        creationTime().ifPresent(creationTime ->
                entityUptime.setCreationTimeMs(creationTime.toEpochMilli()));

        return entityUptime.build();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link EntityUptime} instances.
     */
    class Builder extends ImmutableEntityUptime.Builder {}
}
