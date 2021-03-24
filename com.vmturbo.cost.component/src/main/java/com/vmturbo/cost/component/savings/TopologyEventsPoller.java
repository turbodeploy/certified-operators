package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
     * Flag to enable or disable the polling/processing of topology events.
     */
    private final boolean isEnabledTopologyEventsPolling;

    /**
     * Constructor.
     *
     * @param tep The Topology Event Provider.
     * @param entityEventsInMemoryJournal The Entity Events Journal.
     * @param isEnabled boolean that specifies if polling of topology events is enabled.
     */
    TopologyEventsPoller(@Nonnull final TopologyEventProvider tep,
                         @Nonnull final EntityEventsJournal entityEventsInMemoryJournal,
                         final boolean isEnabled) {
        topologyEventProvider = Objects.requireNonNull(tep);
        entityEventsJournal = Objects.requireNonNull(entityEventsInMemoryJournal);
        isEnabledTopologyEventsPolling = isEnabled;
    }

    /**
     * The poll method retrieves topology events in an event window.
     * @param startTime start time
     * @param endTime end time
     */
    void poll(LocalDateTime startTime, LocalDateTime endTime) {
        logger.debug("Topology event poller getting TEP events.");

        final TimeInterval eventWindow = TimeInterval.builder()
                        .endTime(endTime.toInstant(ZoneOffset.UTC))
                        .startTime(startTime.toInstant(ZoneOffset.UTC))
                        .build();
        final TopologyEvents topologyEvents =
                        topologyEventProvider.getTopologyEvents(eventWindow, TopologyEventFilter.ALL);

        if (isEnabledTopologyEventsPolling) {
            processTopologyEvents(topologyEvents);
        }
    }

    /**
     * Check if a topology broadcast happen after the given time.
     *
     * @param time a time instant
     * @return true if the latest broadcast happened after the given time.
     */
    boolean isTopologyBroadcasted(LocalDateTime time) {
        // TODO implementation will be provided by task OM-68006.
        return true;
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
