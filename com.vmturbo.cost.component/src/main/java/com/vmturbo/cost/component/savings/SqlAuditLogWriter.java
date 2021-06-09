package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_AUDIT_EVENTS;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.InsertReturningStep;
import org.jooq.impl.DSL;
import org.jooq.tools.StringUtils;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
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
            InsertReturningStep<EntitySavingsAuditEventsRecord> insert = dsl
                    .insertInto(ENTITY_SAVINGS_AUDIT_EVENTS)
                    .set(ENTITY_SAVINGS_AUDIT_EVENTS.ENTITY_OID, 0L)
                    .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_TYPE, 0)
                    .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_ID, StringUtils.EMPTY)
                    .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_TIME, INIT_TIME)
                    .set(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_INFO, StringUtils.EMPTY)
                    .onDuplicateKeyIgnore();

            // Put all records within a single transaction, irrespective of the chunk size.
            dsl.transaction(transaction -> {
                final DSLContext transactionContext = DSL.using(transaction);
                final BatchBindStep batch = transactionContext.batch(insert);

                // Add to batch and bind in chunks based on chunk size.
                Iterators.partition(events.iterator(), chunkSize)
                        .forEachRemaining(chunk ->
                                chunk.forEach(event -> bindAuditEvent(batch, event)));

                if (batch.size() > 0) {
                    int[] insertCounts = batch.execute();
                    int totalInserted = IntStream.of(insertCounts).sum();
                    if (totalInserted < batch.size()) {
                        // This message is common as ActionListener sends exact same action events
                        // when those events are already present in the audit log, so change it
                        // from warn to trace.
                        logger.trace("Entity savings audit: Could only insert {} out of "
                                        + "batch size of {}. Total input entry count: {}. "
                                        + "Chunk size: {}", totalInserted, batch.size(),
                                events.size(), chunkSize);
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("Could not write {} entity savings event audit entries to DB.",
                    events.size(), e);
        }
    }

    @Override
    public int deleteOlderThan(long timestamp) {
        final LocalDateTime minDate = SavingsUtil.getLocalDateTime(timestamp, clock);
        return dsl.deleteFrom(ENTITY_SAVINGS_AUDIT_EVENTS)
                .where(ENTITY_SAVINGS_AUDIT_EVENTS.EVENT_TIME.lt(minDate))
                .execute();
    }

    /**
     * Creates an audit log entry out of the savings event. If successful, then binds that to the
     * batch in preparation for insert into DB.
     *
     * @param batch DB batch to add the event to.
     * @param event Savings event to write to audit log.
     */
    private void bindAuditEvent(final BatchBindStep batch, @Nonnull final SavingsEvent event) {
        try {
            final AuditLogEntry logEntry = new AuditLogEntry(event, gson);
            if (!logEntry.isValid()) {
                logger.warn("Skipping invalid audit event: {} log entry: {}.", event,
                        logEntry);
            } else {
                batch.bind(logEntry.entityOid, logEntry.eventType, logEntry.eventId,
                        SavingsUtil.getLocalDateTime(logEntry.eventTime, clock), logEntry.eventInfo);
            }
        } catch (InvalidProtocolBufferException ipbe) {
            logger.warn("Unable to serialize audit event {}.", event, ipbe);
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
        private final String eventInfo;

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

            final Map<String, Object> jsonData = new HashMap<>();

            final Optional<EntityPriceChange> priceChange = event.getEntityPriceChange();

            final Optional<TopologyEvent> topologyEvent = event.getTopologyEvent();
            topologyEvent.ifPresent(te -> serializeTopologyEvent(jsonData, te));

            final Optional<ActionEvent> actionEvent = event.getActionEvent();
            actionEvent.ifPresent(ae -> serializeActionEvent(jsonData, ae,
                    priceChange.orElse(null)));

            this.eventInfo = gson.toJson(jsonData);
            if (this.eventId == null) {
                // Use event id as checksum for the json message, always positive, tamper proof!
                // Get rid of that old confusing 'NA-<timestamp>' format.
                this.eventId = String.format("%d", eventInfo.hashCode() & 0xfffffff);
            }
        }

        /**
         * Updates useful fields in TopologyEvent into the json map to be serialized to DB.
         *
         * @param jsonData Map to be updated.
         * @param topologyEvent Event containing data to serialize.
         */
        private void serializeTopologyEvent(final Map<String, Object> jsonData,
                @Nonnull final TopologyEvent topologyEvent) {
            this.eventType = topologyEvent.getType().getNumber();
            if (!topologyEvent.hasEventInfo()) {
                return;
            }
            final TopologyEventInfo eventInfo = topologyEvent.getEventInfo();
            if (eventInfo.hasVendorEventId()) {
                // Vendor id may not be present in most cases.
                this.eventId = eventInfo.getVendorEventId();
            }
            if (eventInfo.hasStateChange()) {
                EntityStateChangeDetails stateChange = eventInfo.getStateChange();
                if (stateChange.hasSourceState() && stateChange.getSourceState()
                        != TopologyDTO.EntityState.UNKNOWN) {
                    jsonData.put("ss", stateChange.getSourceState()
                            == TopologyDTO.EntityState.POWERED_ON ? 1 : 0);
                }
                if (stateChange.hasDestinationState() && stateChange.getDestinationState()
                        != TopologyDTO.EntityState.UNKNOWN) {
                    jsonData.put("ds", stateChange.getDestinationState()
                            == TopologyDTO.EntityState.POWERED_ON ? 1 : 0);
                }
            } else if (eventInfo.hasProviderChange()) {
                ProviderChangeDetails providerChange = eventInfo.getProviderChange();
                if (providerChange.hasSourceProviderOid()
                        && !providerChange.hasUnknownSourceProvider()) {
                    jsonData.put("so", providerChange.getSourceProviderOid());
                }
                if (providerChange.hasDestinationProviderOid()
                        && !providerChange.hasUnknownDestinationProvider()) {
                    jsonData.put("do", providerChange.getDestinationProviderOid());
                }
            }
        }

        /**
         * Serializes action event and price change. Updates input json map.
         *
         * @param jsonData Map to be updated with values for DB json.
         * @param actionEvent Action event to read settings from.
         * @param priceChange Price change to be stored to jsonData.
         */
        private void serializeActionEvent(final Map<String, Object> jsonData,
                @Nonnull final ActionEvent actionEvent,
                @Nullable final EntityPriceChange priceChange) {

            this.eventType = actionEvent.getEventType().getTypeCode();
            this.eventId = String.valueOf(actionEvent.getActionId());

            jsonData.put("et", actionEvent.getEntityType());
            jsonData.put("at", actionEvent.getActionType());
            jsonData.put("ac", actionEvent.getActionCategory());
            jsonData.put("t", actionEvent.getDescription());

            if (priceChange == null) {
                return;
            }
            jsonData.put("sc", priceChange.getSourceCost());
            jsonData.put("dc", priceChange.getDestinationCost());
            double diff = priceChange.getDelta();
            jsonData.put("d", diff);

            long srcOid = priceChange.getSourceOid().orElse(0L);
            long dstOid = priceChange.getDestinationOid().orElse(0L);
            if (srcOid != dstOid) {
                jsonData.put("so", srcOid);
                jsonData.put("do", dstOid);
            } else {
                jsonData.put("po", srcOid);
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

        /**
         * Checks if this log entry is valid, ready to be inserted to DB.
         *
         * @return Whether log entry is valid.
         */
        public boolean isValid() {
            return entityOid != 0
                    && eventType != 0
                    && eventId != null
                    && eventTime != 0
                    && eventInfo != null;
        }

        /**
         * To string for this instance.
         *
         * @return To string value.
         */
        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("entityOid", entityOid)
                    .append("eventType", eventType)
                    .append("eventId", eventId)
                    .append("eventTime", eventTime)
                    .append("eventInfo", eventInfo)
                    .toString();
        }
    }
}
