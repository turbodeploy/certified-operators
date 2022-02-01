package com.vmturbo.sql.utils.jooq;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * The procedure for purging records from relevant tables.
 */
public class PurgeProcedure implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private static final int DELETE_CHUNK_SIZE = 1000;

    private final DSLContext dsl;
    private final Table<?> tableToPurge;
    private final Field<?> timestampField;
    private final Table<?> retentionPolicyTable;
    private final Field<String> policyNameField;
    private final Field<Integer> retentionPeriodField;
    private final String policyName;
    private final DatePart unit;
    private List<Condition> additionalConditions = new ArrayList<>();
    private Select<?> startPointQuery;

    /**
     * Constructor for creating a {@link PurgeProcedure}.
     *
     * @param dsl the {@link DSLContext} for interacting with db
     * @param tableToPurge the table to purge data from
     * @param timestampField the field which marks the timestamp of the record
     * @param retentionPolicyTable the table which contains the retention policy
     * @param policyNameField the column of the policy
     * @param retentionPeriodField the column of the retention period
     * @param policyName name of the policy
     * @param unit unit for the period, like day or month
     */
    public PurgeProcedure(DSLContext dsl, Table<?> tableToPurge, Field<?> timestampField,
            Table<?> retentionPolicyTable, Field<String> policyNameField,
            Field<Integer> retentionPeriodField, String policyName, DatePart unit) {
        this.dsl = dsl;
        this.tableToPurge = tableToPurge;
        this.timestampField = timestampField;
        this.retentionPolicyTable = retentionPolicyTable;
        this.policyNameField = policyNameField;
        this.retentionPeriodField = retentionPeriodField;
        this.policyName = policyName;
        this.unit = unit;
    }

    /**
     * The query used to fetch the starting timestamp from where we go backwards the retention
     * period window and remove any records before that. If not specified, it will start from now.
     *
     * @param startPointQuery the query used to find the start point
     * @return this
     */
    public PurgeProcedure withStartPointQuery(Select<?> startPointQuery) {
        this.startPointQuery = startPointQuery;
        return this;
    }

    /**
     * Add additional conditions that will be used in the delete query where clause.
     *
     * @param additionalConditions additional conditions
     * @return this
     */
    public PurgeProcedure withAdditionalConditions(Condition... additionalConditions) {
        this.additionalConditions = Arrays.asList(additionalConditions);
        return this;
    }

    @Override
    public void run() {
        try {
            final Stopwatch stopwatch = Stopwatch.createStarted();
            // add timestamp condition and additional conditions if any
            final List<Condition> allConditions = new ArrayList<>();
            allConditions.add(getTimestampCondition());
            allConditions.addAll(additionalConditions);

            // delete in chunks
            int numDeleted = JooqUtil.deleteInChunks(
                    dsl.deleteFrom(tableToPurge).where(allConditions), DELETE_CHUNK_SIZE);

            logger.info("Purged {} records from table {} in {}", numDeleted, tableToPurge.getName(),
                    stopwatch.elapsed());
        } catch (Exception e) {
            logger.error("Error when purging data from table {}", tableToPurge.getName(), e);
        }
    }

    /**
     * Get the timestamp condition used to decide which records to purge.
     *
     * @return time condition
     */
    private Condition getTimestampCondition() {
        final Class<?> type = timestampField.getType();
        if (type == Long.class) {
            final Field<Timestamp> cur = startPointQuery == null
                    ? DSL.currentTimestamp()
                    : fromUnixTime(startPointQuery.asField().coerce(Long.class).divide(1000L));
            return timestampField.coerce(Long.class).lessThan(
                    dsl.select(unixTimestamp(DSL.timestampSub(cur, retentionPeriodField, unit)).times(1000))
                            .from(retentionPolicyTable).where(policyNameField.eq(policyName)));
        } else if (type == Timestamp.class) {
            final Field<Timestamp> cur = startPointQuery == null
                    ? DSL.currentTimestamp() : startPointQuery.asField();
            return timestampField.coerce(Timestamp.class).lessThan(
                    dsl.select(DSL.timestampSub(cur, retentionPeriodField, unit))
                    .from(retentionPolicyTable).where(policyNameField.eq(policyName)));
        } else if (type == LocalDate.class) {
            final Field<LocalDate> cur = startPointQuery == null
                    ? DSL.currentLocalDate() : startPointQuery.asField();
            return timestampField.coerce(LocalDate.class).lessThan(
                    dsl.select(DSL.localDateSub(cur, retentionPeriodField, unit))
                            .from(retentionPolicyTable).where(policyNameField.eq(policyName)));
        } else if (type == LocalDateTime.class) {
            final Field<LocalDateTime> cur = startPointQuery == null
                    ? DSL.currentLocalDateTime() : startPointQuery.asField();
            return timestampField.coerce(LocalDateTime.class).lessThan(
                    dsl.select(DSL.localDateTimeSub(cur, retentionPeriodField, unit))
                            .from(retentionPolicyTable).where(policyNameField.eq(policyName)));
        } else {
            throw new IllegalArgumentException("Unsupported timestamp type: " + type.getSimpleName());
        }
    }

    /**
     * Convert from timestamp to epoch seconds.
     *
     * @param timestampField the time to convert
     * @return Field of type {@link Long}
     */
    public Field<Long> unixTimestamp(Field<Timestamp> timestampField) {
        switch (dsl.dialect()) {
            case MARIADB:
            case MYSQL:
                return DSL.field("UNIX_TIMESTAMP({0})", Long.class, timestampField);
            case POSTGRES:
                return DSL.field("extract(epoch from {0})", Long.class, timestampField);
            default:
                throw new UnsupportedOperationException("Dialect not supported");
        }
    }

    /**
     * Convert from unix time (epoch in seconds) to {@link Timestamp}.
     *
     * @param unixTime the unix time in seconds
     * @return Field of type {@link Timestamp}
     */
    public Field<Timestamp> fromUnixTime(Field<Long> unixTime) {
        switch (dsl.dialect()) {
            case MARIADB:
            case MYSQL:
                return DSL.field("FROM_UNIXTIME({0})", Timestamp.class, unixTime);
            case POSTGRES:
                return DSL.field("to_timestamp({0})::date", Timestamp.class, unixTime);
            default:
                throw new UnsupportedOperationException("Dialect not supported");
        }
    }
}
