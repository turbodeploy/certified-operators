package com.vmturbo.topology.event.library.uptime;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Modifiable;
import org.immutables.value.Value.Style;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;

/**
 * A cached (precomputed) set of data used to extend an entity uptime calculation. The intent of
 * storing the precomputed data is to avoid having to load and process all topology events within
 * the full uptime window on every progression of the window. Rather the set of cached data allows
 * for uptime on the front-end (earliest) to be dropped, while events occurring after the
 * {@link CachedEntityUptime#calculationWindow()} can be appended to {@link CachedEntityUptime#uptimeToLatestEvent()}.
 */
@Style(typeModifiable = "Mutable*")
@Modifiable
@HiddenImmutableImplementation
@Immutable
public interface CachedEntityUptime {

    /**
     * An empty precomputed uptime.
     */
    CachedEntityUptime EMPTY_UPTIME = CachedEntityUptime.builder()
            .calculationWindow(TimeInterval.EPOCH)
            .build();

    /**
     * The calculation window this instance was calculated over.
     * @return The calculation window this instance was calculated over.
     */
    @Nonnull
    TimeInterval calculationWindow();

    /**
     * The state of the entity at the start of {@link #calculationWindow()}.
     * @return The state of the entity at the start of {@link #calculationWindow()}.
     */
    @Default
    @Nonnull
    default EntityState startState() {
        return EntityState.UNKNOWN;
    }

    /**
     * The earliest power state event within {@link #calculationWindow()}.
     * @return The earliest power state event within {@link #calculationWindow()}.
     */
    @Nonnull
    Optional<TopologyEvent> earliestPowerEvent();

    /**
     * The latest power state event within {@link #calculationWindow()}. This may be the same
     * event as {@link #earliestPowerEvent()}.
     * @return The latest power state event within {@link #calculationWindow()}.
     */
    @Nonnull
    Optional<TopologyEvent> latestPowerEvent();

    /**
     * The entity uptime within {@link #calculationWindow()}, up until {@link #latestPowerEvent()}.
     * If there are no power state events within {@link #calculationWindow()}, this will be the uptime
     * until the end of {@link #calculationWindow()}.
     * @return The entity uptime until {@link #latestPowerEvent()}.
     */
    @Default
    @Nonnull
    default Duration uptimeToLatestEvent() {
        return Duration.ZERO;
    }

    /**
     * Converts this {@link CachedEntityUptime} to a {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    default Builder toBuilder() {
        return CachedEntityUptime.builder().from(this);
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
     * A builder class for constructing {@link CachedEntityUptime} instances.
     */
    class Builder extends ImmutableCachedEntityUptime.Builder {}
}
