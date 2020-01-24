package com.vmturbo.history.db.bulk;

import static java.time.ZoneOffset.UTC;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.codec.digest.DigestUtils;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.RelationTypeConverter;

/**
 * This class contains methods that assist in performing rollups of entity stats data.
 */
class RollupKey {

    private RollupKey() {
    }

    // represents a missing field value in a constructed rollup key
    private static final String MISSING_FIELD = "-";
    private static RelationTypeConverter relationTypeConverter = new RelationTypeConverter();

    /**
     * These are fields that contribute to a rollup key.
     *
     * <p>They are used in the order shown below. Each field's contribution is either the string
     * representation of its in-table value or, if the field is missing from the record, a
     * single hyphen ("-"). The rollup key is the MD5 checksum of this constructed value</p>
     *
     * <p>Because the in-database value is used, each value can be declared with a converter
     * that should transform its in-jooq-record value to the in-database value.</p>
     */
    private enum KeyField {
        uuid(null),
        producer_uuid(null),
        property_type(null),
        property_subtype(null),
        relation(r -> relationTypeConverter.to((RelationType)r)),
        commodity_key(null);

        private final Function<Object, Object> converter;

        /**
         * Define a new key field value.
         *
         * @param converter converter function for this field
         */
        KeyField(@Nullable Function<Object, Object> converter) {
            this.converter = converter;
        }

        /**
         * Call this field's converter on the given in-jook-record value.
         *
         * <p>The crucial point is that the toString value of the converted value is used
         * in constructing rollup keys.</p>
         *
         * @param value the field value as present in the JOOQ record
         * @return value compatible with the in-database representation
         */
        @Nonnull
        public Object convert(@Nonnull Object value) {
            return converter != null ? converter.apply(value) : value;
        }
    }

    // tables that make use of this class need to have a timestamp field with this name
    private static final String SNAPSHOT_TIME_FIELD_NAME = "snapshot_time";

    /**
     * Get the key value for the hourly snapshot that this record should roll up to.
     *
     * @param table  the jooq table object for this record
     * @param record the record to be rolled up
     * @param <R>    type of record
     * @return the value for the record's hour_key column
     */
    static <R extends Record> String getHourKey(@Nonnull Table<R> table, @Nonnull R record) {
        return getRollupKey(r -> rollupTimestamps(r).hour(), table, record);
    }

    /**
     * Get the key value for the daily snapshot that this record should roll up to.
     *
     * @param table  the jooq table object for this record
     * @param record the record to be rolled up
     * @param <R>    type of record
     * @return the value for the record's day_key column
     */
    @Nonnull
    static <R extends Record> String getDayKey(@Nonnull Table<R> table, @Nonnull R record) {
        return getRollupKey(r -> rollupTimestamps(r).day(), table, record);
    }

    /**
     * Get the key value for the monthly snapshot that this record should roll up to.
     *
     * @param table  the jooq table object for this record
     * @param record the record to be rolled up
     * @param <R>    type of record
     * @return the value for the record's month_key column
     */
    @Nonnull
    static <R extends Record> String getMonthKey(@Nonnull Table<R> table, @Nonnull R record) {
        return getRollupKey(r -> rollupTimestamps(r).month(), table, record);
    }

    /**
     * Compute a rollup key for a record.
     *
     * @param getTimestamp function to return timestamp string for this rollup record.
     * @param table        table corresponding to record type
     * @param record       record whose key is needed
     * @param <R>          record type
     * @return rollup key
     */
    private static <R extends Record> String getRollupKey(
            Function<R, String> getTimestamp, Table<R> table, R record) {
        Objects.requireNonNull(table);
        Objects.requireNonNull(record);
        return md5(getTimestamp.apply(record) + commonKey(table, record));
    }

    /**
     * Construct the common part of a rollup key.
     *
     * <p>The fields identified in {@link KeyField} are extracted from the record in order, and in
     * each case the string representation of what would be the in-database column value is
     * obtained. Missing values are rendered as "-". These per-field strings are then joined to
     * yield the final value.</p>
     *
     * @param table  the jooq table that the record comes from
     * @param record the record that will be rolled up
     * @param <R>    type of record
     * @return the common part of the rollup key
     */
    @Nonnull
    private static <R extends Record> String commonKey(@Nonnull Table<R> table, @Nonnull R record) {
        return Stream.of(KeyField.values()).map(kf -> {
            final Object value = record.get(JooqUtils.getField(table, kf.name()));
            return value != null ? String.valueOf(kf.convert(value)) : null;
        }).map(s -> s != null ? s : MISSING_FIELD)
                .collect(Collectors.joining());
    }


    /**
     * Compute rollup timestamps for the snapshot field in the given record.
     *
     * @param record the record containing the snapshot field
     * @param <R>    record type
     * @return RollupTimestamps object with rollup timestamps for the given timestamp
     */
    @Nonnull
    static <R extends Record> RollupTimestamps rollupTimestamps(@Nonnull R record) {
        final Timestamp timestamp = (Timestamp)record.get(SNAPSHOT_TIME_FIELD_NAME);
        return getRollupTimestamps(Instant.ofEpochMilli(timestamp.getTime()));
    }

    @Nonnull
    @VisibleForTesting
    static RollupTimestamps getRollupTimestamps(final Instant t) {
        if (t != null) {
            try {
                return timestampCache.get(t, () -> new RollupTimestamps(t));
            } catch (ExecutionException e) {
                // should be rare, just cons up a redundant copy
                return new RollupTimestamps(t);
            }
        } else {
            return RollupTimestamps.MISSING;
        }
    }

    // we generally get a big burst of stats records, all with the same timestamp (corresponding
    // to whatever broadcast gave rise to all these records), and then we never see that timestamp
    // again. So we'll use a very little cache to avoid having to recompute these very often.
    private static final int TIMESTAMP_CACHE_SIZE = 10;
    private static Cache<Instant, RollupTimestamps> timestampCache = CacheBuilder.newBuilder()
            .maximumSize(TIMESTAMP_CACHE_SIZE)
            .build();

    @Nonnull
    private static String md5(@Nonnull String s) {
        return DigestUtils.md5Hex(s);
    }

    /**
     * Class that computes timestamps for the hour, day, and month rollups that should include
     * a record with the given timestamp.
     *
     * <p>Results are delivered as formatted strings of the form YYYY-MM-DD hh:mm:ss, or
     * "-" (a single hyphen) if the base timestamp is null.</p>
     */
    static class RollupTimestamps {

        private static final ZoneId UTC = ZoneId.of("Z");

        private static final DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(UTC);

        private final String hourTimestamp;
        private final String dayTimestamp;
        private final String monthTimestamp;

        static final RollupTimestamps MISSING = new RollupTimestamps(Instant.ofEpochMilli(0L));

        /**
         * Create a new instance.
         *
         * @param timestamp timestamp of the stats record
         */
        RollupTimestamps(@Nonnull Instant timestamp) {
            this.hourTimestamp = computeHourTimestamp(timestamp);
            this.dayTimestamp = computeDayTimestamp(timestamp);
            this.monthTimestamp = computeMonthTimestamp(timestamp);
        }

        /**
         * Get the snapshot timestamp for the hourly rollup.
         *
         * <p>We compute time 00:00 in the hour of the base snapshot time.</p>
         *
         * @return formatted timestamp for the hourly rollup, or "-" if base is null
         */
        @Nonnull
        public String hour() {
            return hourTimestamp;
        }

        @Nonnull
        private String computeHourTimestamp(Instant timestamp) {
            return format(timestamp != null ? timestamp.truncatedTo(ChronoUnit.HOURS) : null);
        }

        /**
         * Get the snapshot timestamp for the daily rollup.
         *
         * <p>We compute midnight on the day of the base snapshot time.</p>
         *
         * @return formatted timestamp for the daily rollup, or "-" if base is null
         */
        @Nonnull
        public String day() {
            return dayTimestamp;
        }

        @Nonnull
        private String computeDayTimestamp(Instant timestamp) {
            return format(timestamp != null ? timestamp.truncatedTo(ChronoUnit.DAYS) : null);
        }

        /**
         * Get the snapshot timestamp for the monthly rollup.
         *
         * <p>We compute midnight on the final day of the month in which the base snapshot time
         * occurred, for compatibility with the snapshots chosen by the prior implementation
         * in a mariadb stored proc.</p>
         *
         * @return formatted timestamp for monthly snapshot, or "-" if base is null
         */
        @Nonnull
        public String month() {
            return monthTimestamp;
        }

        @Nonnull
        private String computeMonthTimestamp(Instant timestamp) {
            return format(timestamp != null ? truncateToMonth(timestamp) : null);
        }

        @Nonnull
        private String format(@Nullable Instant t) {
            return t != null ? formatter.format(t) : MISSING_FIELD;
        }
    }

    /**
     * Compute the correct monthly rollup timestamp for the given timestamp.
     *
     * <p>This means midnight of the last day of the month in which the given time occurs.</p>
     *
     * @param t timestamp to be truncated
     * @return truncated value
     */
    private static Instant truncateToMonth(Instant t) {
        return LocalDateTime.ofInstant(t, UTC)
                .truncatedTo(ChronoUnit.DAYS)
                .plus(1L, ChronoUnit.MONTHS)
                .withDayOfMonth(1)
                .minusDays(1L)
                .toInstant(UTC);
    }
}
