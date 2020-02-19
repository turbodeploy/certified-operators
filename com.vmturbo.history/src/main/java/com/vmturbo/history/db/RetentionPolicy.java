package com.vmturbo.history.db;

import static com.vmturbo.history.schema.HistoryVariety.ENTITY_STATS;
import static com.vmturbo.history.schema.HistoryVariety.PRICE_DATA;
import static com.vmturbo.history.schema.RetentionUtil.DAILY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.HOURLY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.LATEST_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.MONTHLY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.PERCENTILE_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.SYSTEM_LOAD_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.abstraction.Tables.AVAILABLE_TIMESTAMPS;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Functions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.schema.RetentionUtil;
import com.vmturbo.history.schema.TimeFrame;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.RetentionPoliciesRecord;

/**
 * This class mimics the retention_policies DB table and provides related utility methods.
 */
public enum RetentionPolicy {
    /**
     * Retention policy for all the _latest entity stats tables.
     */
    LATEST_STATS(LATEST_STATS_RETENTION_POLICY_NAME),
    /**
     * Retention policy for all the _by_hour entity stats tables.
     */
    HOURLY_STATS(HOURLY_STATS_RETENTION_POLICY_NAME),
    /**
     * Retention policy for all the _by_day entity stats tables.
     */
    DAILY_STATS(DAILY_STATS_RETENTION_POLICY_NAME),
    /**
     * Retention policy for all the _by_month entity stats tables.
     */
    MONTHLY_STATS(MONTHLY_STATS_RETENTION_POLICY_NAME),
    /**
     * Retention policy for system_load data.
     */
    SYSTEM_LOAD(SYSTEM_LOAD_RETENTION_POLICY_NAME),
    /**
     * Retention policy for percentile data.
     */
    PERCENTILE(PERCENTILE_RETENTION_POLICY_NAME);

    private static Logger logger = LogManager.getLogger();

    // map of keys to enum values
    private static Map<String, RetentionPolicy> keyToPolicy = Stream.of(values())
            .collect(Collectors.toMap(RetentionPolicy::getKey, Functions.identity()));

    // map of policy enums to policy data loaded from DB
    private static Map<RetentionPolicy, Pair<Integer, ChronoUnit>> retentionPeriods;

    private final String key;

    /**
     * Create a new policy enum.
     *
     * @param key key (i.e. policy_name DB column value) for this policy
     */
    RetentionPolicy(String key) {
        this.key = key;
    }

    /**
     * Get the key (policy name) for this policy.
     *
     * @return key value
     */
    public String getKey() {
        return key;
    }

    /**
     * Get the retention period (numeric value) for this policy.
     *
     * @return number of time units to retain data
     */
    public Integer getPeriod() {
        // ensure retention data is loaded
        final Map<RetentionPolicy, Pair<Integer, ChronoUnit>> data = getRetentionData();
        return data.containsKey(this) ? data.get(this).getLeft() : null;
    }

    /**
     * Get the time unit used to define this retention policy.
     *
     * @return time unit
     */
    public ChronoUnit getUnit() {
        // ensure retention data is loaded
        final Map<RetentionPolicy, Pair<Integer, ChronoUnit>> data = getRetentionData();
        return data.containsKey(this) ? data.get(this).getRight() : null;
    }

    /**
     * Compute the expiration time for a given creation time, per this policy.
     *
     * @param t creation time
     * @return expiration time
     */
    public Instant getExpiration(Instant t) {
        // ensure policy data is loaded
        final Map<RetentionPolicy, Pair<Integer, ChronoUnit>> data = getRetentionData();
        if (data.containsKey(this)) {
            Integer period = data.get(this).getLeft();
            ChronoUnit unit = data.get(this).getRight();
            return RetentionUtil.getExpiration(t, unit, period);
        } else {
            throw new IllegalStateException(String.format("No retention policy data loaded for policy %s", name()));
        }
    }

    /**
     * Get the retention policy enum for the given key.
     *
     * @param key key
     * @return policy
     */
    public static RetentionPolicy forKey(String key) {
        return keyToPolicy.get(key);
    }

    /**
     * This methods is used to indicate that the retention policies in the database have been changed.
     *
     * <p>We discard our current retention and then recalculate expiration timestamps for all records in
     * available_timestamps (which will fault in the new data, assuming there are any records.</p>
     */
    public static synchronized void onChange() {
        retentionPeriods = null;
        updateAvailableTimestampExpiration();
    }

    /**
     * Load retention policy data from the database, if we don't currently have it.
     *
     * @return the newly loaded data
     */
    private static synchronized Map<RetentionPolicy, Pair<Integer, ChronoUnit>> getRetentionData() {
        if (retentionPeriods == null) {
            // no loaded retention data, so load it now
            retentionPeriods = new HashMap<>();
            HistorydbIO historydbIO = (HistorydbIO)HistorydbIO.instance();
            try (Connection conn = historydbIO.connection()) {
                Result<RetentionPoliciesRecord> records = historydbIO.using(conn)
                        .selectFrom(Tables.RETENTION_POLICIES)
                        .fetch();
                for (RetentionPoliciesRecord record : records) {
                    RetentionPolicy policy = RetentionPolicy.forKey(record.getPolicyName());
                    if (policy == null) {
                        // someone has created a new retention policy without updating this class
                        logger.warn("Unrecognized policy name in retention_policies table: {}",
                                record.getPolicyName());
                        continue;
                    }
                    ChronoUnit unit;
                    try {
                        unit = ChronoUnit.valueOf(record.getUnit());
                    } catch (IllegalArgumentException e) {
                        logger.error("Invalid unit in retention_policies table: {}", record.getUnit());
                        continue;
                    }
                    int period = record.getRetentionPeriod();
                    retentionPeriods.put(policy, Pair.of(period, unit));
                }
            } catch (VmtDbException | SQLException | DataAccessException e) {
                logger.error("Failed to load retention policy data", e);
            }
        }
        return retentionPeriods;
    }

    /**
     * Recompute expiration timestamps for records in available_timestamps table, called when we're notified that the
     * retention policies have changed in the database.
     */
    private static void updateAvailableTimestampExpiration() {
        HistorydbIO historydbIO = (HistorydbIO)HistorydbIO.instance();
        try (Connection conn = historydbIO.transConnection()) {
            DSLContext ctx = historydbIO.using(conn);
            Result<AvailableTimestampsRecord> records = ctx.selectFrom(AVAILABLE_TIMESTAMPS).fetch();
            records.stream()
                    .filter(r -> r.getHistoryVariety().equals(ENTITY_STATS.name())
                            || r.getHistoryVariety().equals(PRICE_DATA.name()))
                    .forEach(r -> updateExpiration(r, ctx));
            conn.commit();
        } catch (VmtDbException | SQLException | DataAccessException e) {
            logger.error("Failed to update available_snapshots entries on retention settings change");
        }
    }

    /**
     * Update the expiration time in the given record from available_timestamps table.
     *
     * @param record the available_timestamps record
     * @param ctx    jOOQ DSL context to use
     */
    private static void updateExpiration(AvailableTimestampsRecord record, DSLContext ctx) {
        RetentionPolicy policy;
        TimeFrame timeFrame = TimeFrame.valueOf(record.getTimeFrame());
        switch (timeFrame) {
            case LATEST:
                policy = LATEST_STATS;
                break;
            case HOUR:
                policy = HOURLY_STATS;
                break;
            case DAY:
                policy = DAILY_STATS;
                break;
            case MONTH:
                policy = MONTHLY_STATS;
                break;
            default:
                logger.warn("Unexpected timeframe from available_stats record: {}", timeFrame);
                return;
        }
        Instant expiration = policy.getExpiration(record.getTimeStamp().toInstant());
        ctx.update(AVAILABLE_TIMESTAMPS)
                .set(AVAILABLE_TIMESTAMPS.EXPIRES_AT, Timestamp.from(expiration))
                .where(AVAILABLE_TIMESTAMPS.TIME_STAMP.eq(record.getTimeStamp()),
                        AVAILABLE_TIMESTAMPS.TIME_FRAME.eq(record.getTimeFrame()),
                        AVAILABLE_TIMESTAMPS.HISTORY_VARIETY.eq(record.getHistoryVariety()))
                .execute();
    }
}
