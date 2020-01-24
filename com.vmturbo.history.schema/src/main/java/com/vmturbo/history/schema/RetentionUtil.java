package com.vmturbo.history.schema;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/**
 * Class that provides some retention-related members that were extracted from the RetentionPolicy
 * class in com.vmturbo.history module.
 *
 * <p>The extract was done so that the V1.28.1 Java migration could be housed with all the other
 * history component migrations in the history.schema component. Ideally, at some point, the
 * history and history.schema modules will be merged, and some of this can be stitched back
 * together in a less fragmented form.</p>
 */
public class RetentionUtil {

    /** name of policy in the retention_policies for retention of _latest entity stats data. */
    public static final String LATEST_STATS_RETENTION_POLICY_NAME = "retention_latest_hours";
    /** name of policy in the retention_policies for retention of _by_hour entity stats data. */
    public static final String HOURLY_STATS_RETENTION_POLICY_NAME = "retention_hours";
    /** name of policy in the retention_policies for retention of _by_day entity stats data. */
    public static final String DAILY_STATS_RETENTION_POLICY_NAME = "retention_days";
    /** name of policy in the retention_policies for retention of _by_month entity stats data. */
    public static final String MONTHLY_STATS_RETENTION_POLICY_NAME = "retention_months";
    /** name of policy in the retention_policies for retention of system_load data. */
    public static final String SYSTEM_LOAD_RETENTION_POLICY_NAME = "systemload_retention_days";
    /** name of policy in the retention_policies for retention of percentile data. */
    public static final String PERCENTILE_RETENTION_POLICY_NAME = "percentile_retention_days";

    /** prevent instantiation. */
    private RetentionUtil(){
    }

    /**
     * Compute the expiration time for a given creation time, based on provided policy parameters.
     *
     * @param t      creation time
     * @param unit   time unit of retention intervals
     * @param period period (# of units) in retention interval
     * @return expiration time
     */
    public static Instant getExpiration(Instant t, ChronoUnit unit, int period) {
        switch (unit) {
            case MILLIS:
            case SECONDS:
            case MINUTES:
            case HOURS:
            case DAYS: {
                Instant truncated = t.truncatedTo(unit);
                // if creation time was mid-unit, compute expiration from end of that unit, else from creation time
                return truncated.plus(t.equals(truncated) ? period : period + 1, unit);
            }
            case MONTHS:
                // compute beginning of month containing creation time
                Instant truncated = LocalDateTime.ofInstant(t, ZoneOffset.UTC)
                        .truncatedTo(ChronoUnit.DAYS)
                        .withDayOfMonth(1).toInstant(ZoneOffset.UTC);
                // compute expiration from end of that month, unless creation time is exactly
                // beginning of month
                return LocalDateTime.ofInstant(truncated, ZoneOffset.UTC)
                        .plus(t.equals(truncated) ? period : period + 1, unit)
                        .toInstant(ZoneOffset.UTC);
            default:
                throw new IllegalArgumentException(String.format("Unit %s not supported", unit));
        }
    }

}
