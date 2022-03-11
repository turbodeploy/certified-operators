package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_EVENTS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep5;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.vmturbo.cost.component.db.tables.records.EntitySavingsEventsRecord;

/**
 * Persistent events store that writes events to DB entity_savings_events table.
 */
public class SqlEntityEventsJournal implements EntityEventsJournal {
    private static final Logger logger = LogManager.getLogger();

    /**
     * JOOQ access.
     */
    private final DSLContext dsl;

    /**
     * Chunk size, default 1000, used for batch inserts.
     */
    private final int chunkSize;

    /**
     * When start/end time argument of DB request method needs to be ignored.
     */
    private static final long IGNORE_TIME = -1;

    /**
     * Constructor that creates the new instance.
     *
     * @param dsl DB access layer.
     * @param chunkSize Chunk size to use for reads and writes.
     */
    public SqlEntityEventsJournal(@Nonnull final DSLContext dsl, int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.chunkSize = chunkSize;
        logger.info("Initializing SqlEntityEventsJournal with chunk size {}.", this.chunkSize);
    }

    @Override
    public void addEvents(@Nonnull Collection<SavingsEvent> newEvents) {
        writeEvents(newEvents);
    }

    @Override
    public void addEvent(@Nonnull SavingsEvent newEvent) {
        writeEvents(ImmutableSet.of(newEvent));
    }

    @Override
    public Stream<SavingsEvent> getEventsBetween(long startTime, long endTime) {
        return readEvents(startTime, endTime);
    }

    @Override
    public int purgeEventsOlderThan(long eventTime) {
        return dsl.deleteFrom(ENTITY_SAVINGS_EVENTS)
                .where(ENTITY_SAVINGS_EVENTS.EVENT_CREATED.lt(eventTime))
                .execute();
    }

    @Override
    public boolean persistEvents() {
        return true;
    }

    @Nonnull
    @Override
    public List<SavingsEvent> removeEventsSince(long startTime) {
        return removeEventsBetween(startTime, IGNORE_TIME, Collections.emptySet());
    }

    @Nonnull
    @Override
    public List<SavingsEvent> removeEventsBetween(long startTime, long endTime,
            @Nonnull Set<Long> entityOids) {
        List<SavingsEvent> removedEvents = getEventsBetween(startTime, endTime)
                .filter(event -> entityOids.isEmpty() || entityOids.contains(event.getEntityId()))
                .collect(Collectors.toList());
        final List<Condition> conditions = getTimeConditions(startTime, endTime);
        if (!entityOids.isEmpty()) {
            conditions.add(ENTITY_SAVINGS_EVENTS.ENTITY_OID.in(entityOids));
        }
        dsl.deleteFrom(ENTITY_SAVINGS_EVENTS)
                .where(conditions)
                .execute();
        return removedEvents;
    }

    @Nonnull
    @Override
    public List<SavingsEvent> removeAllEvents() {
        return removeEventsSince(IGNORE_TIME);
    }

    @Nullable
    @Override
    public Long getOldestEventTime() {
        Result<Record1<Long>> result = dsl.select(DSL.min(ENTITY_SAVINGS_EVENTS.EVENT_CREATED))
                .from(ENTITY_SAVINGS_EVENTS).fetch();
        if (result != null && result.size() == 1) {
            return (Long)result.getValue(0, 0);
        }
        return null;
    }

    @Override
    public int size() {
        return dsl.selectCount()
                .from(ENTITY_SAVINGS_EVENTS)
                .fetchOne(0, int.class);
    }

    /**
     * Gets time related condition list for DB access.
     *
     * @param startTime Start time inclusive, ignored if -1.
     * @param endTime End time inclusive, ignored if -1.
     * @return List of conditions to use, can be empty.
     */
    @Nonnull
    private List<Condition> getTimeConditions(long startTime, long endTime) {
        final List<Condition> conditions = new ArrayList<>();
        if (startTime != IGNORE_TIME) {
            conditions.add(ENTITY_SAVINGS_EVENTS.EVENT_CREATED.ge(startTime));
        }
        if (endTime != IGNORE_TIME) {
            conditions.add(ENTITY_SAVINGS_EVENTS.EVENT_CREATED.lt(endTime));
        }
        return conditions;
    }

    /**
     * Writes events to DB table.
     *
     * @param events Events to be written.
     */
    private void writeEvents(@Nonnull Collection<SavingsEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        final AtomicInteger totalInserted = new AtomicInteger(0);
        try {
            Iterators.partition(events.iterator(), chunkSize)
                    .forEachRemaining(chunk -> {
                        final InsertValuesStep5<EntitySavingsEventsRecord, Long, Long, Integer,
                                Integer, String> insert = dsl
                                .insertInto(ENTITY_SAVINGS_EVENTS)
                                .columns(ENTITY_SAVINGS_EVENTS.EVENT_CREATED,
                                        ENTITY_SAVINGS_EVENTS.ENTITY_OID,
                                        ENTITY_SAVINGS_EVENTS.EVENT_TYPE,
                                        ENTITY_SAVINGS_EVENTS.ENTITY_TYPE,
                                        ENTITY_SAVINGS_EVENTS.EVENT_INFO);
                        chunk.forEach(event -> storeSavingsEvent(insert, event));
                        int inserted = insert.onDuplicateKeyIgnore().execute();
                        if (inserted < chunk.size()) {
                            logger.warn("Could only insert {} out of batch size "
                                    + "of {}. Total event count: {}. Chunk size: {}",
                                    inserted, chunkSize, events.size(), chunk.size());
                        }
                        totalInserted.getAndAdd(inserted);
                    });
            logger.trace("Total events inserted {}. Chunk size: {}. Input events: {}",
                    totalInserted, chunkSize, events.size());
        } catch (Exception e) {
            logger.warn("Could not write {} entity savings events to DB.", events.size(), e);
        }
    }

    /**
     * Reads events from DB, within the time range specified.
     *
     * @param startTime Start time (inclusive).
     * @param endTime End time (exclusive).
     * @return Stream of events - each will have event_id (generated by DB) set.
     */
    private Stream<SavingsEvent> readEvents(long startTime, long endTime) {
        final List<SavingsEvent> savingsEvents = new ArrayList<>();
        dsl.connection(conn -> {
            conn.setAutoCommit(false);
            try {
                DSL.using(conn, dsl.settings())
                        .selectFrom(ENTITY_SAVINGS_EVENTS)
                        .where(getTimeConditions(startTime, endTime))
                        .orderBy(ENTITY_SAVINGS_EVENTS.EVENT_ID)
                        .fetchSize(chunkSize)
                        .fetchLazy()
                        .stream()
                        .map(this::loadSavingsEvent)
                        .filter(Objects::nonNull)
                        .forEach(savingsEvents::add);
            } catch (Exception e) {
                logger.warn("Could not read savings events between {} & {}.", startTime,
                        endTime, e);
            }
        });
        return savingsEvents.stream();
    }

    /**
     * Binds a single event, in preparation for it to be written as a DB record.
     *
     * @param insert For inserting the record.
     * @param event Savings event to be written. If invalid, will not get written.
     */
    private void storeSavingsEvent(final InsertValuesStep5<EntitySavingsEventsRecord, Long, Long,
            Integer, Integer, String> insert, @Nonnull final SavingsEvent event) {
        if (!event.isValid()) {
            logger.warn("Not storing event without required fields set: {}", event);
            return;
        }
        insert.values(event.getTimestamp(),
                event.getEntityId(),
                event.getEventType(),
                event.getEntityType().getNumber(),
                event.serialize());
    }

    /**
     * Loads savings event instance from DB record.
     *
     * @param dbRecord Record read from DB.
     * @return Savings event instance, or null if there was an issue reading or event was invalid.
     */
    @Nullable
    private SavingsEvent loadSavingsEvent(@Nonnull final EntitySavingsEventsRecord dbRecord) {
        long eventId = dbRecord.get(ENTITY_SAVINGS_EVENTS.EVENT_ID);
        final String eventInfo = dbRecord.get(ENTITY_SAVINGS_EVENTS.EVENT_INFO);
        try {
            int entityType = dbRecord.get(ENTITY_SAVINGS_EVENTS.ENTITY_TYPE);
            int eventType = dbRecord.get(ENTITY_SAVINGS_EVENTS.EVENT_TYPE);
            long entityId = dbRecord.get(ENTITY_SAVINGS_EVENTS.ENTITY_OID);
            long timestamp = dbRecord.get(ENTITY_SAVINGS_EVENTS.EVENT_CREATED);

            final SavingsEvent.Builder builder = new SavingsEvent.Builder()
                    .eventId(eventId)
                    .timestamp(timestamp)
                    .entityId(entityId)
                    .deserialize(eventId, timestamp, entityId, eventType, entityType, eventInfo);

            final SavingsEvent event = builder.build();
            if (!event.isValid()) {
                logger.warn("Not loading event without required fields set: {}", event);
                return null;
            }
            return event;
        } catch (Exception e) {
            logger.warn("Unable to load SavingsEvent from DB for event {}. Info: {}",
                    eventId, eventInfo, e);
        }
        return null;
    }
}
