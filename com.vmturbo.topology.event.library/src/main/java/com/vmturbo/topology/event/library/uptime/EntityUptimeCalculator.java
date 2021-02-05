package com.vmturbo.topology.event.library.uptime;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.data.MutableDuration;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

/**
 * Calculates {@link EntityUptime} based on a set of topology events. Entity uptime represents the powered
 * on time of a workload (billed time) over an analyzed window.
 */
public class EntityUptimeCalculator {

    private static final Set<TopologyEventType> POWER_STATE_EVENT_TYPES = Sets.immutableEnumSet(
            TopologyEventType.RESOURCE_CREATION,
            TopologyEventType.STATE_CHANGE,
            TopologyEventType.RESOURCE_DELETION);

    private final UptimeCalculationRecorder calculationRecorder;

    private final boolean uptimeFromCreation;

    /**
     * Constructs a new calculator instance.
     * @param calculationRecorder The {@link UptimeCalculationRecorder}, used to record the discrete
     *                            steps in calculating entity uptime for selected entities.
     * @param uptimeFromCreation If true, uptime percentage is calculated based on the creation time
     *                           of the entity, if the creation time falls within the analyzed window.
     *                           If this flag is false or the entity was created prior to the analyzed
     *                           window, the uptime percentage will be based on a denominator of the
     *                           analyzed window.
     */
    public EntityUptimeCalculator(@Nonnull UptimeCalculationRecorder calculationRecorder,
                                  boolean uptimeFromCreation) {
        this.calculationRecorder = Objects.requireNonNull(calculationRecorder);
        this.uptimeFromCreation = uptimeFromCreation;
    }

    /**
     * Calculates the entity uptime, based on power state events contained within {@code eventLedger}
     * and the provided {@code uptimeWindow}.
     * @param eventLedger The event ledger, containing all power state events to be considered
     *                    within the {@code uptimeWindow}. Events prior to the start of the uptime
     *                    window will be used to infer the starting power state of the entity.
     * @param uptimeWindow The time window over which uptime should be analyzed.
     * @return The {@link EntityUptimeCalculation}.
     */
    @Nonnull
    public EntityUptimeCalculation calculateUptime(@Nonnull TopologyEventLedger eventLedger,
                                                   @Nonnull TimeInterval uptimeWindow) {

        calculationRecorder.recordCalculationStart(eventLedger, uptimeWindow);

        final CachedEntityUptime cachedUptime = extendCachedUptime(
                eventLedger.entityOid(),
                CachedEntityUptime.EMPTY_UPTIME,
                eventLedger,
                uptimeWindow);

        return finalizeUptimeCalculation(eventLedger.entityOid(), cachedUptime, uptimeWindow);
    }

    /**
     * Progresses the start time of the {@code cachedUptime}, returning a new {@link CachedEntityUptime}
     * instance representing {@code cachedUptime} with the new start time. The {@link CachedEntityUptime#startState()}
     * is used to determine how progressing the start time will affect {@link CachedEntityUptime#uptimeToLatestEvent()}.
     * If the new start time falls after the {@link CachedEntityUptime#earliestPowerEvent()}, the calculator
     * cannot know the impact on the uptime, given it does not know the power state events between the
     * earliest and latest power state events within {@link CachedEntityUptime}. In this case, the cache is
     * invalid and an exception will be thrown.
     * @param entityOid The entity OID.
     * @param cachedUptime The cached entity uptime to progress (move start time later).
     * @param newStartTime The new start time. Must be later than the current start time of
     *                     {@code cachedUptime}.
     * @return A new immutable instance of {@link CachedEntityUptime}, with uptime based on the
     * {@code newStartTime}.
     * @throws CacheInvalidationException Thrown if the {@code newStartTime} either falls after the first
     * known power state event within {@code cachedUptime} or if the {@code newStartTime} is prior to
     * the current start time of {@code cachedUptime}.
     */
    @Nonnull
    public EntityUptimeCalculation progressStartTime(long entityOid,
                                                     @Nonnull CachedEntityUptime cachedUptime,
                                                     @Nonnull Instant newStartTime) throws CacheInvalidationException {

        final TimeInterval cachedUptimeWindow = cachedUptime.calculationWindow();
        if (newStartTime.isBefore(cachedUptimeWindow.startTime())) {
            throw new CacheInvalidationException();
        } else {
            final boolean withinCollapsedEvents = cachedUptime.earliestPowerEvent()
                    .map(e -> Instant.ofEpochMilli(e.getEventTimestamp()))
                    .map(earliestEventTime -> !newStartTime.isBefore(earliestEventTime))
                    .orElse(false);
            final boolean afterUptimeWindow = newStartTime.isAfter(cachedUptime.calculationWindow().endTime());

            if (withinCollapsedEvents || afterUptimeWindow) {
                throw new CacheInvalidationException();
            } else {

                final Duration uptimeToLatestEvent;
                if (cachedUptime.startState() == EntityState.POWERED_ON) {
                    uptimeToLatestEvent = cachedUptime.uptimeToLatestEvent()
                            .minus(Duration.between(cachedUptimeWindow.startTime(), newStartTime));
                } else {
                    uptimeToLatestEvent = cachedUptime.uptimeToLatestEvent();
                }

                final TimeInterval newUptimeWindow = cachedUptimeWindow.toBuilder()
                        .startTime(newStartTime)
                        .build();
                final CachedEntityUptime updatedCachedUptime = cachedUptime.toBuilder()
                        .calculationWindow(newUptimeWindow)
                        .uptimeToLatestEvent(uptimeToLatestEvent)
                        .build();

                return finalizeUptimeCalculation(entityOid, updatedCachedUptime, newUptimeWindow);
            }
        }
    }

    /**
     * Appends the {@code eventLedger} events to {@code baseUptime}, up until {@code newEndTime}.
     * @param baseUptime The base cached uptime to extend.
     * @param eventLedger The event ledger, containing all events to be appended to the {@code baseUptime}.
     *                    Any events contained within the ledger occurring prior to the latest event
     *                    within {@code baseUptime} will be ignored.
     * @param newEndTime The new end time for the uptime window.
     * @return A new {@link EntityUptimeCalculation}, representing {@code baseUptime} extended by
     * {@code eventLedger}.
     */
    @Nonnull
    public EntityUptimeCalculation extendUptimeCalculation(@Nonnull CachedEntityUptime baseUptime,
                                                           @Nonnull TopologyEventLedger eventLedger,
                                                           @Nonnull Instant newEndTime) {

        final TimeInterval baseWindow = baseUptime.calculationWindow();
        final boolean invalidEndTime = baseUptime.latestPowerEvent()
                .map(e -> Instant.ofEpochMilli(e.getEventTimestamp()))
                .map(latestEventTime -> newEndTime.isBefore(latestEventTime))
                .orElse(false);
        if (invalidEndTime) {
            throw new IllegalArgumentException();
        }

        final TimeInterval extendedUptimeWindow = baseWindow.toBuilder()
                .endTime(newEndTime)
                .build();
        calculationRecorder.recordCalculationStart(baseUptime, eventLedger, extendedUptimeWindow);

        final CachedEntityUptime updatedCachedUptime = extendCachedUptime(
                eventLedger.entityOid(),
                baseUptime,
                eventLedger,
                extendedUptimeWindow);

        return finalizeUptimeCalculation(eventLedger.entityOid(), updatedCachedUptime, extendedUptimeWindow);
    }

    private Optional<Instant> determineCreationTime(@Nonnull CachedEntityUptime cachedUptime) {

        return cachedUptime.earliestPowerEvent()
                .filter(e -> e.getType() == TopologyEventType.RESOURCE_CREATION)
                .map(TopologyEvent::getEventTimestamp)
                .map(Instant::ofEpochMilli);
    }

    private CachedEntityUptime extendCachedUptime(long entityOid,
                                                  @Nonnull CachedEntityUptime baseUptime,
                                                  @Nonnull TopologyEventLedger eventLedger,
                                                  @Nonnull TimeInterval uptimeWindow) {

        final Instant earliestEventTime = baseUptime.latestPowerEvent()
                .map(e -> Instant.ofEpochMilli(e.getEventTimestamp()))
                .orElse(Instant.EPOCH);
        final MutableDuration totalUptime = MutableDuration.fromDuration(baseUptime.uptimeToLatestEvent());
        final MutableCachedEntityUptime cachedUptime = MutableCachedEntityUptime.create()
                .from(baseUptime)
                .setCalculationWindow(uptimeWindow);

        eventLedger.events(POWER_STATE_EVENT_TYPES).forEach(topologyEvent -> {
            final Instant eventTime = Instant.ofEpochMilli(topologyEvent.getEventTimestamp());
            final TopologyEventInfo eventInfo = topologyEvent.getEventInfo();

            if (eventTime.isBefore(earliestEventTime)) {
                calculationRecorder.recordSkippedEvent(entityOid, topologyEvent);
            } else if (eventTime.isBefore(uptimeWindow.startTime())) {

                switch (topologyEvent.getType()) {

                    case RESOURCE_CREATION:
                        cachedUptime.setStartState(EntityState.POWERED_OFF);
                    case STATE_CHANGE:
                        cachedUptime.setStartState(eventInfo.getStateChange().getDestinationState());
                        break;
                    case RESOURCE_DELETION:
                        cachedUptime.setStartState(EntityState.MAINTENANCE);
                        break;
                }

            calculationRecorder.recordPriorEvent(entityOid, topologyEvent, cachedUptime.startState());

            } else if (uptimeWindow.contains(eventTime)) {

                recordUptimeFromLastEvent(cachedUptime, totalUptime, eventTime);

                if (!cachedUptime.earliestPowerEvent().isPresent()) {
                    cachedUptime.setEarliestPowerEvent(topologyEvent);
                }

                cachedUptime.setLatestPowerEvent(topologyEvent);
            }
        });

        // If there are no power events within the uptime window, we set uptime to latest event
        // as the entire uptime window
        if (!cachedUptime.latestPowerEvent().isPresent()
                && cachedUptime.startState() == EntityState.POWERED_ON) {
            totalUptime.add(uptimeWindow.duration());
        }

        return cachedUptime.setUptimeToLatestEvent(totalUptime.toDuration()).toImmutable();
    }

    @Nonnull
    private EntityUptimeCalculation finalizeUptimeCalculation(long entityOid,
                                                              @Nonnull CachedEntityUptime cachedUptime,
                                                              @Nonnull TimeInterval uptimeWindow) {

        final MutableDuration totalUptime = MutableDuration.fromDuration(cachedUptime.uptimeToLatestEvent());

        // calculate latest event to the end of the uptime window
        if (cachedUptime.latestPowerEvent().isPresent()) {
            recordUptimeFromLastEvent(cachedUptime, totalUptime, uptimeWindow.endTime());
        }

        final Optional<Instant> creationTime = determineCreationTime(cachedUptime);
        final Instant startTime = uptimeFromCreation
                ? creationTime.orElseGet(uptimeWindow::startTime)
                : uptimeWindow.startTime();
        final Duration totalDuration = Duration.between(startTime, uptimeWindow.endTime());
        return EntityUptimeCalculation.builder()
                .entityOid(entityOid)
                .entityUptime(EntityUptime.builder()
                        .uptime(totalUptime.toDuration())
                        .totalTime(totalDuration)
                        .creationTime(creationTime)
                        .uptimePercentage((double)totalUptime.toMillis() / totalDuration.toMillis() * 100)
                        .build())
                .cachedUptime(cachedUptime)
                .build();
    }

    private void recordUptimeFromLastEvent(@Nonnull CachedEntityUptime cachedUptime,
                                           @Nonnull MutableDuration uptimeDuration,
                                           @Nonnull Instant currentTime) {


        final boolean isTransitionFromPoweredOn = cachedUptime.latestPowerEvent()
                .map(e -> e.getType() == TopologyEventType.STATE_CHANGE
                        && e.getEventInfo().getStateChange().getDestinationState() == EntityState.POWERED_ON)
                .orElseGet(() -> cachedUptime.startState() == EntityState.POWERED_ON);

        if (isTransitionFromPoweredOn) {
            final Instant priorEventTime = cachedUptime.latestPowerEvent()
                    .map(e -> Instant.ofEpochMilli(e.getEventTimestamp()))
                    .orElseGet(() -> cachedUptime.calculationWindow().startTime());

            final Duration uptime = Duration.between(priorEventTime, currentTime);

            uptimeDuration.add(uptime);
            calculationRecorder.recordUptime(cachedUptime, uptime);
        }
    }

    /**
     * Contains the output of an entity uptime calculation, including the calculated entity uptime
     * data and cached/precomputed uptime metadata.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface EntityUptimeCalculation {

        /**
         * The entity OID.
         * @return The entity OID.
         */
        long entityOid();

        /**
         * The {@link EntityUptime}.
         * @return The {@link EntityUptime}.
         */
        @Nonnull
        EntityUptime entityUptime();

        /**
         * The {@link CachedEntityUptime}.
         * @return The {@link CachedEntityUptime}.
         */
        @Nonnull
        CachedEntityUptime cachedUptime();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link EntityUptimeCalculation} instances.
         */
        class Builder extends ImmutableEntityUptimeCalculation.Builder {}
    }

    /**
     * An exception indicating invalidation of a {@link CachedEntityUptime} instance. This exception
     * indicates a full entity uptime calculation is required in order to correctly calculate the
     * uptime for the target entity.
     */
    public static class CacheInvalidationException extends Exception {}
}
