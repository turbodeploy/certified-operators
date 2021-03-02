package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_AUDIT_EVENTS;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.impl.DSL;
import org.jooq.tools.StringUtils;

import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsAuditEventsRecord;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;

/**
 * Writes audit event logs to DB for diagnostics later.
 */
public class SqlAuditLogWriter implements AuditLogWriter {
    /**
     * Minimal info logging.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * JOOQ access.
     */
    private final DSLContext dsl;

    /**
     * Used for timestamp conversions before storing/reading DB values.
     */
    private final Clock clock;

    /**
     * Chunk size, default 1000, used for batch inserts.
     */
    private final int chunkSize;

    /**
     * Need some dummy init time.
     */
    private static final LocalDateTime INIT_TIME = LocalDateTime.now();

    /**
     * Gson for serialization.
     */
    private static final Gson gson = createGson();

    /**
     * Create a new log writer.
     *
     * @param dsl DB access.
     * @param clock UTC clock.
     * @param chunkSize DB write chunk size.
     */
    public SqlAuditLogWriter(@Nonnull final DSLContext dsl, @Nonnull final Clock clock,
            final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
        logger.info("Created new SQL Audit Log Writer with chunk size {} and clock {}.",
                this.chunkSize, this.clock);
    }

    @Override
    public void write(@Nonnull List<SavingsEvent> events) {
        try {
            Iterators.partition(events.iterator(), chunkSize)
                    .forEachRemaining(chunk -> dsl.transaction(transaction -> {
                        final DSLContext transactionContext = DSL.using(transaction);
                        InsertReturningStep<EntitySavingsAuditEventsRecord> insert = dsl
                                .insertInto(ENTITY_SAVINGS_AUDIT_EVENTS)
                                .set(ENTITY_SAVINGS_AUDIT_EVENTS.ENTITY_OID, 0L)
                                .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_TYPE, 0)
                                .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_ID, StringUtils.EMPTY)
                                .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_TIME, INIT_TIME)
                                .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_INFO, StringUtils.EMPTY)
                                .onDuplicateKeyIgnore();
                        final BatchBindStep batch = transactionContext.batch(insert);
                        chunk.forEach(event -> {
                            try {
                                final AuditLogEntry logEntry = new AuditLogEntry(event, gson);
                                batch.bind(logEntry.entityOid, logEntry.eventType, logEntry.eventId,
                                        SavingsUtil.getLocalDateTime(logEntry.eventTime, clock),
                                        logEntry.eventInfo);
                            } catch (InvalidProtocolBufferException ipbe) {
                                logger.warn("Unable to serialize audit event {}.", event, ipbe);
                            }
                        });
                        if (batch.size() > 0) {
                            batch.execute();
                        }
                    }));
        } catch (Exception e) {
            logger.warn("Could not write {} entity savings event audit entries to DB.",
                    events.size(), e);
        }
    }

    /**
     * Creates Gson for entity price change.
     *
     * @return Gson for audit entry serialization.
     */
    @VisibleForTesting
    static Gson createGson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapterFactory(new GsonAdaptersEntityPriceChange());
        return gsonBuilder.create();
    }

    /**
     * Helper for writing audit log entries to DB.
     */
    @VisibleForTesting
    static class AuditLogEntry {
        /**
         * Entity oid for which the event applies to.
         */
        private final long entityOid;

        /**
         * Type code of the topology or action event.
         */
        private int eventType;

        /**
         * Unique id for event.
         */
        private String eventId;

        /**
         * Event occurrence time.
         */
        private final long eventTime;

        /**
         * Any additional JSON metadata about the event.
         */
        private String eventInfo;

        /**
         * For printing protobuf to json.
         */
        private static final Printer jsonPrinter = JsonFormat.printer()
                .omittingInsignificantWhitespace();

        /**
         * Create a new entry.
         *
         * @param event Savings event to store.
         * @param gson For serialization.
         * @throws InvalidProtocolBufferException Thrown on error parsing topology event.
         */
        AuditLogEntry(SavingsEvent event, Gson gson) throws InvalidProtocolBufferException {
            this.entityOid = event.getEntityId();
            this.eventTime = event.getTimestamp();

            final Optional<TopologyEvent> optionalTopologyEvent = event.getTopologyEvent();
            if (optionalTopologyEvent.isPresent()) {
                final TopologyEvent topologyEvent = optionalTopologyEvent.get();
                this.eventType = topologyEvent.getType().getNumber();
                if (topologyEvent.hasEventInfo() && topologyEvent.getEventInfo().hasVendorEventId()) {
                    this.eventId = topologyEvent.getEventInfo().getVendorEventId();
                }
                this.eventInfo = jsonPrinter.print(topologyEvent);
            } else {
                final Optional<ActionEvent> optionalActionEvent = event.getActionEvent();
                if (optionalActionEvent.isPresent()) {
                    final ActionEvent actionEvent = optionalActionEvent.get();
                    this.eventType = actionEvent.getEventType().getTypeCode();
                    this.eventId = String.valueOf(actionEvent.getActionId());
                    this.eventInfo = StringUtils.EMPTY;
                    Optional<EntityPriceChange> priceChange = event.getEntityPriceChange();
                    priceChange.ifPresent(entityPriceChange -> this.eventInfo =
                            gson.toJson(entityPriceChange));
                }
            }
        }

        public long getEntityOid() {
            return entityOid;
        }

        public int getEventType() {
            return eventType;
        }

        public String getEventId() {
            return eventId;
        }

        public long getEventTime() {
            return eventTime;
        }

        public String getEventInfo() {
            return eventInfo;
        }
    }
}
