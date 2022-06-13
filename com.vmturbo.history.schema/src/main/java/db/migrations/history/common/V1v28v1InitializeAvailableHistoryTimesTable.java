package db.migrations.history.common;

import static com.vmturbo.components.common.utils.RollupTimeFrame.DAY;
import static com.vmturbo.components.common.utils.RollupTimeFrame.HOUR;
import static com.vmturbo.components.common.utils.RollupTimeFrame.LATEST;
import static com.vmturbo.components.common.utils.RollupTimeFrame.MONTH;
import static com.vmturbo.history.schema.HistoryVariety.ENTITY_STATS;
import static com.vmturbo.history.schema.HistoryVariety.PRICE_DATA;
import static com.vmturbo.history.schema.RetentionUtil.DAILY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.HOURLY_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.LATEST_STATS_RETENTION_POLICY_NAME;
import static com.vmturbo.history.schema.RetentionUtil.MONTHLY_STATS_RETENTION_POLICY_NAME;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.migration.jdbc.BaseJdbcMigration;
import org.jooq.Record;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.utils.RollupTimeFrame;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.RetentionUtil;

/**
 * Migration to populate newly-created (by migration V1.27) available_timestamps table, with
 * existing
 * timestamps found in the corresponding stats tables.
 *
 * <p>This will will permit much more efficient execution of some frequently used queries.</p>
 */
public class V1v28v1InitializeAvailableHistoryTimesTable extends BaseJdbcMigration {
    private static final String UTC_TIME_ZONE_STRING = "+00:00";

    private final Logger logger;

    /**
     * Create a new callback instance to be used as a delegate for the V1.28.1 migration of the
     * specific database scenario (Legacy or MariaDB) and the V.1.35.1 migration, and use that
     * migration's logger, to reflect its identity in log messages.
     *
     * @param logger logger to use
     */
    public V1v28v1InitializeAvailableHistoryTimesTable(Logger logger) {
        this.logger = logger != null ? logger : LogManager.getLogger();
    }

    @Override
    public void migrate(Connection connection) {
        // be idempotent
        if (availableTimestampsIsEmpty(connection)) {
            loadAvaiableTimestamps(connection);
        }
    }

    private boolean availableTimestampsIsEmpty(Connection connection) {
        String sql = "SELECT time_stamp FROM available_timestamps LIMIT 1";
        final Record record = DSL.using(connection).fetchOne(sql);
        return record == null;
    }

    private void loadAvaiableTimestamps(Connection connection) {
        String originalTimeZone = setTimeZone(connection, UTC_TIME_ZONE_STRING);
        final String sql = String.format("INSERT INTO %s (%s, %s, %s, %s) values (?, ?, ?, ?)",
                "available_timestamps", "time_stamp", "time_frame", "history_variety",
                "expires_at");
        int toLoad = 0;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // For ENTITY_STATS we assume the topology has VMs, and load snapshot times from the VM stats tables.
            toLoad += loadTimes(connection, ps, "vm_stats_latest", "snapshot_time", LATEST,
                    ENTITY_STATS, LATEST_STATS_RETENTION_POLICY_NAME);
            toLoad += loadTimes(connection, ps, "vm_stats_by_hour", "snapshot_time", HOUR,
                    ENTITY_STATS, HOURLY_STATS_RETENTION_POLICY_NAME);
            toLoad +=
                    loadTimes(connection, ps, "vm_stats_by_day", "snapshot_time", DAY, ENTITY_STATS,
                            DAILY_STATS_RETENTION_POLICY_NAME);
            toLoad += loadTimes(connection, ps, "vm_stats_by_month", "snapshot_time", MONTH,
                    ENTITY_STATS, MONTHLY_STATS_RETENTION_POLICY_NAME);
            // We'll use the same timestamps for price data since the data to exract more accurate data from existing
            // tables would make the upgrade process prohibitively expensive.
            toLoad += loadTimes(connection, ps, "vm_stats_latest", "snapshot_time", LATEST,
                    PRICE_DATA, LATEST_STATS_RETENTION_POLICY_NAME);
            toLoad +=
                    loadTimes(connection, ps, "vm_stats_by_hour", "snapshot_time", HOUR, PRICE_DATA,
                            HOURLY_STATS_RETENTION_POLICY_NAME);
            toLoad += loadTimes(connection, ps, "vm_stats_by_day", "snapshot_time", DAY, PRICE_DATA,
                    DAILY_STATS_RETENTION_POLICY_NAME);
            toLoad += loadTimes(connection, ps, "vm_stats_by_month", "snapshot_time", MONTH,
                    PRICE_DATA, MONTHLY_STATS_RETENTION_POLICY_NAME);
            final int[] results = ps.executeBatch();
            int loaded = IntStream.of(results).sum();
            logger.info("Loaded {} discovered timestamps to available_timestamps table", loaded);
            if (toLoad != loaded) {
                logger.warn("Expected to load {} discovered timestamps, not {}", toLoad, loaded);
            }
        } catch (SQLException e) {
            logger.warn("Failed to save {} discovered timestamps to available_timestamps table",
                    toLoad, e);
        } finally {
            setTimeZone(connection, originalTimeZone);
            try {
                connection.commit();
            } catch (SQLException e) {
                logger.error("Failed to commit", e);
            }
        }
    }

    private String setTimeZone(Connection connection, String timeZone) {
        if (timeZone != null) {
            try {
                String originalTimeZone = getTimeZone(connection);
                final String sql = String.format("SET SESSION time_zone = '%s'", timeZone);
                try (Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                }
                return originalTimeZone;
            } catch (SQLException e) {
                logger.error("Failed to change session timezone for migration operations", e);
                return null;
            }
        } else {
            return null;
        }
    }

    private String getTimeZone(Connection connection) {
        String sql = "SELECT @@SESSION.time_zone";
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql);) {
            return result.next() ? result.getString(1) : null;
        } catch (SQLException e) {
            logger.error("Failed to retrieve current timezone setting from database", e);
            return null;
        }
    }

    private int loadTimes(Connection connection, PreparedStatement ps, String tableName,
            String timestampFieldName, RollupTimeFrame timeFrame, HistoryVariety historyVariety,
            String policyName) {
        final List<Timestamp> timestamps = getTimestamps(connection, tableName, timestampFieldName);
        Pair<ChronoUnit, Integer> retention = getRetention(connection, timeFrame, policyName);
        for (Timestamp timestamp : timestamps) {
            Instant expiration =
                    RetentionUtil.getExpiration(Instant.ofEpochMilli(timestamp.getTime()),
                            retention.getLeft(), retention.getRight());
            addToBatch(ps, timestamp, timeFrame, historyVariety, expiration);
        }
        return timestamps.size();
    }

    private void addToBatch(final PreparedStatement ps, final Timestamp timestamp,
            final RollupTimeFrame timeFrame, final HistoryVariety historyVariety,
            final Instant expiration) {
        try {
            ps.setTimestamp(1, timestamp);
            ps.setString(2, timeFrame.name());
            ps.setString(3, historyVariety.name());
            ps.setTimestamp(4, Timestamp.from(expiration));
            ps.addBatch();
        } catch (SQLException e) {
            logger.warn("Failed to insert available_stats record[{}, {}, {}, {}]", timestamp,
                    timeFrame, historyVariety, expiration, e);
        }
    }

    private List<Timestamp> getTimestamps(final Connection connection, final String tableName,
            final String timestampFieldName) {
        final String sql =
                String.format("SELECT DISTINCT %s FROM %s", timestampFieldName, tableName);
        return DSL
                .using(connection)
                .fetch(sql)
                .stream()
                .map(r -> r.get(0, Timestamp.class))
                .collect(Collectors.toList());
    }

    private Pair<ChronoUnit, Integer> getRetention(Connection connection, RollupTimeFrame timeFrame,
            String policyName) {
        String sql = String.format("SELECT unit, retention_period "
                + "FROM retention_policies "
                + "WHERE policy_name = '%s'", policyName);
        final Record record = DSL.using(connection).fetchOne(sql);
        return Pair.of(ChronoUnit.valueOf(record.get(0, String.class)),
                record.get(1, Integer.class));
    }
}
