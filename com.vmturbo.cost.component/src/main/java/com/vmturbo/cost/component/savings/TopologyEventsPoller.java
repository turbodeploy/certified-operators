package com.vmturbo.cost.component.savings;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.TopologyEvent.EventType;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.topology.event.library.TopologyEventProvider;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventFilter;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

/**
 * Listens for topology event changes and inserts events into the internal savings event log.
 */
public class TopologyEventsPoller {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    /**
     * Topology Event Provider.
     */
    private TopologyEventProvider topologyEventProvider;

    /**
     * The In Memory Events Journal.
     */
    private final EntityEventsJournal entityEventsJournal;

    /**
     * Topology Info Tracker.
     */
    private final TopologyInfoTracker topologyInfoTracker;

    /**
     * Constructor.
     * @param tep The Topology Event Provider.
     * @param topoInfoTracker The topology Info Tracker.
     * @param entityEventsInMemoryJournal The Entity Events Journal.
     */
    TopologyEventsPoller(@Nonnull final TopologyEventProvider tep,
            @Nonnull final TopologyInfoTracker topoInfoTracker,
            @Nonnull final EntityEventsJournal entityEventsInMemoryJournal) {
        topologyEventProvider = Objects.requireNonNull(tep);
        topologyInfoTracker = Objects.requireNonNull(topoInfoTracker);
        entityEventsJournal = Objects.requireNonNull(entityEventsInMemoryJournal);
    }

    /**
     * The poll method retrieves topology events in an event window.
     *
     * @param start Start of events polling window in LocalDateTime.
     * @param end End of events polling window in LocalDateTime.
     */
    void poll(@Nonnull final LocalDateTime start, @Nonnull final LocalDateTime end) {
        logger.debug("Topology event poller checking readiness to get TEP events.");
        final Instant startTime = start.toInstant(ZoneOffset.UTC);
        final Instant endTime = end.toInstant(ZoneOffset.UTC);
        processTopologyEventsIfReady(startTime, endTime);
    }

    /**
     * Check if a topology broadcast happened after the end time of event polling window.
     *
     * <p>Determines readiness for polling and processing of topology events.  Called by EntitySavingsProcessor,
     * which may also adjust the time range to a previous time-period, in case the current event window is missing a
     * latest topology with a creation time greater than the end time of an event polling window
     * @param end a particular polling end time being checked.
     * @return true if topology is ready, false otherwise.
     */
    public boolean isTopologyBroadcasted(@Nonnull final LocalDateTime end) {
        // Make sure that the tracked topology status is ready before we start polling.
        // Latest topology creation time should be higher that polling event window end time.
        final Instant endTime = end.toInstant(ZoneOffset.UTC);
        final Optional<TopologyInfo> latestTopologyInfo = topologyInfoTracker
                                                        .getLatestTopologyInfo();
        final LocalDateTime localEndDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(endTime.toEpochMilli()),
                        ZoneId.of("UTC"));
        if (latestTopologyInfo.isPresent()) {
            final long topologyCreationTime = latestTopologyInfo.get().getCreationTime();
            if (topologyCreationTime > endTime.toEpochMilli()) {
                final LocalDateTime localCreationDateTime =
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(topologyCreationTime),
                                ZoneId.of("UTC"));
                logger.info("Topology created at {} is ready for processing of topology related"
                                + " savings events in event window with end time {}.",
                        localCreationDateTime, localEndDateTime);
                return true;
            }
        }
        logger.info("Topology not yet ready for processing of topology related savings events in"
                        + " event window with end time {}.", localEndDateTime);
        return false;
    }

    /**
     * Process Topology Events retrieved for an event window and create Savings events and
     * add them to the Entity Events journal.
     *
     * <p>Return the Topology events polled in the event window for the purposes of testing.
     * @param startTimeInstant Start instant of events polling window.
     * @param endTimeInstant End instant of events polling window.
     */
    @VisibleForTesting
    void processTopologyEventsIfReady(@Nonnull final Instant startTimeInstant,
                                      @Nonnull final Instant endTimeInstant) {
        final TimeInterval eventWindow = TimeInterval.builder()
                        .endTime(endTimeInstant)
                        .startTime(startTimeInstant)
                        .build();
        final TopologyEvents topologyEvents = topologyEventProvider
                        .getTopologyEvents(eventWindow,
                                           TopologyEventFilter.ALL);

        final Map<Long, TopologyEventLedger> topologyEventLedgers = topologyEvents.ledgers();
        final List<SavingsEvent> events = new ArrayList<>();
        topologyEventLedgers.forEach((key, entityTopologyEventLedger) -> {
            final long entityId = key;
            for (TopologyEvent entityEvent : entityTopologyEventLedger.events()) {
                Optional<SavingsEvent> savingsEvent = createSavingsEvent(entityId, entityEvent);
                if (savingsEvent.isPresent()) {
                    events.add(savingsEvent.get());
                    logger.debug("Added a topology event for {}, of type {}", entityId,
                            entityEvent.getType());
                } else {
                    logger.debug("Dropping topology event for {}, of type {}", entityId,
                            entityEvent.getType());
                }
            }
        });
        final Stopwatch topologyWatch = Stopwatch.createStarted();
        entityEventsJournal.addEvents(events);
        logger.info("Addition of {} topology events took {} ms.",
                events.size(), topologyWatch.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Convert a TEP event into a TEM events, then wrap it in a Savings Event.  This also filters
     * out power state and provider change TEP events that transition to unknown values.  These
     * are not processed by the savings calculator.
     *
     * @param entityId The target entity ID.
     * @param topologyEvent The TEP opologyEvent from which to create the SavingsEvent.
     * @return The SavingsEvent.
     */
    protected static Optional<SavingsEvent> createSavingsEvent(final Long entityId,
            @Nonnull TopologyEvent topologyEvent) {
        final long eventTimestamp = topologyEvent.getEventTimestamp();
        // Convert the TEP event to a TEM event
        Boolean poweredOn = null;
        Long newProvider = null;
        Boolean deleted = null;
        if (TopologyEventType.RESOURCE_DELETION.equals(topologyEvent.getType())) {
            deleted = true;
        } else if (!topologyEvent.hasEventInfo()) {
            return Optional.empty();
        }
        TopologyEventInfo eventInfo = topologyEvent.getEventInfo();
        if (TopologyEventType.STATE_CHANGE.equals(topologyEvent.getType())) {
            /*
             * Only generate the event if one of the src/dest states is POWERED_ON. For example,
             * don't generate transitions from SUSPENDED -> MAINTENANCE.
             */
            TopologyDTO.EntityState destState = eventInfo.getStateChange().getDestinationState();
            TopologyDTO.EntityState sourceState = eventInfo.getStateChange().getSourceState();
            boolean newStateIsOn = TopologyDTO.EntityState.POWERED_ON.equals(destState);
            if (newStateIsOn || TopologyDTO.EntityState.POWERED_ON.equals(sourceState)) {
                poweredOn = newStateIsOn;
            }
        }
        if (TopologyEventType.PROVIDER_CHANGE.equals(topologyEvent.getType())) {
            ProviderChangeDetails providerChange = eventInfo.getProviderChange();
            // Don't set the new provider to unknown.
            if (providerChange.hasDestinationProviderOid()) {
                newProvider = providerChange.getDestinationProviderOid();
            }
        }
        if (poweredOn == null && newProvider == null && deleted == null) {
            // Drop the TEP event in order to filter out transitions to unknown providers
            return Optional.empty();
        }
        com.vmturbo.cost.component.savings.TopologyEvent.Builder temEvent =
                new com.vmturbo.cost.component.savings.TopologyEvent.Builder()
                        .timestamp(eventTimestamp)
                        .entityOid(entityId);
        /*
         * Note that TEP events support a single event type, but TEM events support multiple
         * event types.  In the unlikely event that the TEP event contains both, we will favor
         * the power event over the provider change event.
         */
        if (deleted != null) {
            temEvent.eventType(EventType.ENTITY_REMOVED.getValue()).entityRemoved(true);
        } else if (poweredOn != null) {
            temEvent.eventType(EventType.STATE_CHANGE.getValue()).poweredOn(poweredOn);
        } else if (newProvider != null) {
            temEvent.eventType(EventType.PROVIDER_CHANGE.getValue()).providerOid(newProvider);
        }

        return Optional.of(new SavingsEvent.Builder()
                .topologyEvent(temEvent.build())
                .entityId(entityId)
                .timestamp(eventTimestamp)
                .build());
    }
}
