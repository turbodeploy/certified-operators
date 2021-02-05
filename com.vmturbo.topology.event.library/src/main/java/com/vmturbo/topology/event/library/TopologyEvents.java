package com.vmturbo.topology.event.library;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;

/**
 * Represents the set of {@link TopologyEvent}s for a topology through {@link TopologyEventLedger}.
 * The ledger provides event ordering on a per-entity basis.
 */
public class TopologyEvents {

    /**
     * A static ordering of topology events, if two events occur at the same time. For example,
     * if a workload is created and powered on at the same time, the creation event will be first.
     */
    public static final List<TopologyEventType> EVENT_TYPE_ORDER = ImmutableList.of(
            TopologyEventType.RESOURCE_CREATION,
            TopologyEventType.STATE_CHANGE,
            TopologyEventType.PROVIDER_CHANGE,
            TopologyEventType.RESOURCE_DELETION);

    /**
     * The comparator for sorting {@link TopologyEvent} instances.
     */
    public static final Comparator<TopologyEvent> TOPOLOGY_EVENT_TIME_COMPARATOR =
            Comparator.comparing(TopologyEvent::getEventTimestamp)
                    // put supported events types in EVENT_TYPE_ORDER first
                    .thenComparing(e -> !EVENT_TYPE_ORDER.contains(e.getType()))
                    .thenComparing(e -> EVENT_TYPE_ORDER.indexOf(e.getType()))
                    .thenComparing(event -> event.getEventInfo().hashCode());

    private static final Logger logger = LogManager.getLogger();

    private final Map<Long, TopologyEventLedger> eventLedgers;

    private TopologyEvents(@Nonnull Map<Long, TopologyEventLedger> topologyEventLedgers) {
        this.eventLedgers = ImmutableMap.copyOf(topologyEventLedgers);
    }

    /**
     * An immutable map of {@link TopologyEventLedger} by entity OID.
     * @return An immutable map of {@link TopologyEventLedger} by entity OID.
     */
    @Nonnull
    public Map<Long, TopologyEventLedger> ledgers() {
        return eventLedgers;
    }

    /**
     * Creates and returns a collector of {@link EntityEvents} protobuf messages to a {@link TopologyEvents}
     * instance.
     * @return The newly constructed collector.
     */
    @Nonnull
    public static Collector<EntityEvents, ?, TopologyEvents> toTopologyEvents() {

        return Collector.<EntityEvents, List<EntityEvents>, TopologyEvents>of(
                Lists::newArrayList,
                List::add,
                ListUtils::union,
                TopologyEvents::fromEntityEvents);
    }

    /**
     * Converts a collection of {@link EntityEvents} to a {@link TopologyEvents} instance.
     * @param eventsCollection The collection of {@link EntityEvents} protobuf messages to convert.
     * @return The new {@link TopologyEvents} instance.
     */
    @Nonnull
    public static TopologyEvents fromEntityEvents(@Nonnull Collection<EntityEvents> eventsCollection) {

        final Map<Long, TopologyEventLedger> topologyEventLedgers = eventsCollection.stream()
                .map(entityEvents -> TopologyEventLedger.builder()
                        .entityOid(entityEvents.getEntityOid())
                        .events(ImmutableSortedSet.orderedBy(TOPOLOGY_EVENT_TIME_COMPARATOR)
                                .addAll(entityEvents.getEventsList())
                                .build())
                        .build())
                .collect(ImmutableMap.toImmutableMap(
                        TopologyEventLedger::entityOid,
                        Function.identity(),
                        (ledgerA, ledgerB) -> {
                            logger.warn("Multiple ledgers found for entity '{}'. Merging ledgers", ledgerA::entityOid);
                            return TopologyEvents.mergeLedgers(ledgerA, ledgerB);
                        }));

        return new TopologyEvents(topologyEventLedgers);
    }

    /**
     * Constructs a {@link TopologyEvents} instance from the provided {@link TopologyEventLedger} collection.
     * @param ledgers The ledgers used to populate the
     * @return The new {@link TopologyEvents} instance.
     */
    @Nonnull
    public static TopologyEvents fromEventLedgers(@Nonnull TopologyEventLedger... ledgers) {
        return new TopologyEvents(
                Stream.of(ledgers)
                        .collect(ImmutableMap.toImmutableMap(
                                TopologyEventLedger::entityOid,
                                Function.identity(),
                                TopologyEvents::mergeLedgers)));
    }

    private static TopologyEventLedger mergeLedgers(@Nonnull TopologyEventLedger eventLedgerA,
                                                    @Nonnull TopologyEventLedger eventLedgerB) {

        Preconditions.checkArgument(eventLedgerA.entityOid() == eventLedgerB.entityOid(),
                "Entity OIDs must match to merge ledgers");

        return TopologyEventLedger.builder()
                .entityOid(eventLedgerA.entityOid())
                .events(ImmutableSortedSet.orderedBy(TOPOLOGY_EVENT_TIME_COMPARATOR)
                        .addAll(eventLedgerA.events())
                        .addAll(eventLedgerB.events())
                        .build())
                .build();
    }

    /**
     * An enhanced version of {@link EntityEvents} to provide guaranteed ordering of events by timestamp
     * and utility methods.
     */
    @HiddenImmutableImplementation
    @Immutable
    public interface TopologyEventLedger {

        /**
         * The entity OID.
         * @return The entity OID.
         */
        long entityOid();

        /**
         * The set of topology events for the entity, sorted by {@link #TOPOLOGY_EVENT_TIME_COMPARATOR}.
         * @return An immutable sorted set of entity events, sorted by {@link #TOPOLOGY_EVENT_TIME_COMPARATOR}.
         */
        @Default
        @Nonnull
        default SortedSet<TopologyEvent> events() {
            return Collections.emptySortedSet();
        }

        /**
         * Filters events by {@code eventTypes} and returns a stream of events meeting the requested
         * types.
         * @param eventTypes The requested event types.
         * @return A stream of topology events meeting the {@code eventTypes}.
         */
        @Nonnull
        default Stream<TopologyEvent> events(Collection<TopologyEventType> eventTypes) {
            final Set<TopologyEventType> typeSet = Sets.immutableEnumSet(eventTypes);
            return events().stream()
                    .filter(e -> typeSet.contains(e.getType()));
        }

        /**
         * Filters events by {@code eventTypes} and returns a stream of events meeting the requested
         * types.
         * @param eventTypes The requested event types.
         * @return A stream of topology events meeting the {@code eventTypes}.
         */
        @Nonnull
        default Stream<TopologyEvent> events(TopologyEventType... eventTypes) {
            return events(Sets.newHashSet(eventTypes));
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
         * A builder class for constructing {@link TopologyEventLedger} instances.
         */
        class Builder extends ImmutableTopologyEventLedger.Builder {}
    }
}
