package com.vmturbo.cost.component.savings;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
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
     * Constructor.
     *
     * @param tep The Topology Event Provider.
     * @param entityEventsInMemoryJournal The Entity Events Journal.
     */
    TopologyEventsPoller(@Nonnull final TopologyEventProvider tep,
                         @Nonnull final EntityEventsJournal entityEventsInMemoryJournal) {
        topologyEventProvider = Objects.requireNonNull(tep);
        entityEventsJournal = Objects.requireNonNull(entityEventsInMemoryJournal);
    }

    /**
     * The poll method retrieves topology events in an event window.
     */
    void poll() {
        logger.debug("Topology event poller getting TEP events.");

        final Instant endTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
        final TimeInterval eventWindow = TimeInterval.builder()
                        .endTime(endTime)
                        .startTime(endTime.minus(1, ChronoUnit.HOURS))
                        .build();
        final TopologyEvents topologyEvents =
                        topologyEventProvider.getTopologyEvents(eventWindow, TopologyEventFilter.ALL);

        processTopologyEvents(topologyEvents);
    }

    /**
     * Process Topology Events retrieved for an event window and create Savings events and
     * add them to the Entity Events journal.
     *
     * @param topologyEvents The TopologyEvents
     */
    @VisibleForTesting
    protected void processTopologyEvents(@Nonnull final TopologyEvents topologyEvents) {
        final Map<Long, TopologyEventLedger> topologyEventLedgers = topologyEvents.ledgers();
        topologyEventLedgers.entrySet().forEach(entry -> {
            final long entityId = entry.getKey();
            final TopologyEventLedger entityTopologyEventLedger = entry.getValue();
            for (TopologyEvent entityEvent : entityTopologyEventLedger.events()) {
                    final SavingsEvent savingsEvent = createSavingsEvent(entityId, entityEvent);
                    entityEventsJournal.addEvent(savingsEvent);
                    logger.debug("Added a topology event for {}, of type {}", entityId, entityEvent.getType());
            }
        });
    }

    /**
     * Create a Savings Event on receiving an Action Event.
     *
     * @param entityId The target entity ID.
     * @param topologyEvent The TopologyEvent from which to create the SavingsEvent.
     * @return The SavingsEvent.
     */
    protected static SavingsEvent
            createSavingsEvent(final Long entityId, @Nonnull TopologyEvent topologyEvent) {
        final long eventTimestamp = topologyEvent.getEventTimestamp();
        return new SavingsEvent.Builder()
                        .topologyEvent(topologyEvent)
                        .entityId(entityId)
                        .timestamp(eventTimestamp)
                        .build();
    }
}
