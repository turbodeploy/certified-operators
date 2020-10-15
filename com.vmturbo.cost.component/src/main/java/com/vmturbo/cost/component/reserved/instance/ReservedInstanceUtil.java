package com.vmturbo.cost.component.reserved.instance;

import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.min;
import static org.jooq.impl.DSL.sum;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord.StatValue;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * This class contains a list of constant string which related with reserved instance and also
 * contains some help functions which used by reserved instance logic.
 */
public class ReservedInstanceUtil {
    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    public static final String SNAPSHOT_TIME = "snapshot_time";

    public static final String TOTAL_COUPONS = "total_coupons";

    public static final String USED_COUPONS = "used_coupons";

    public static final String TOTAL_COUPONS_SUM_VALUE = "total_coupons_sum";

    public static final String TOTAL_COUPONS_AVG_VALUE = "total_coupons_avg";

    public static final String TOTAL_COUPONS_MAX_VALUE = "total_coupons_max";

    public static final String TOTAL_COUPONS_MIN_VALUE = "total_coupons_min";

    public static final String USED_COUPONS_AVG_VALUE = "used_coupons_avg";

    public static final String USED_COUPONS_SUM_VALUE = "used_coupons_sum";

    public static final String USED_COUPONS_MAX_VALUE = "used_coupons_max";

    public static final String USED_COUPONS_MIN_VALUE = "used_coupons_min";

    public static final String REGION_ID = "region_id";
    /**
     * for floating point comparison of coupons.
     */
    public static final float COUPON_EPSILON = 0.001f;
    /**
     * Set of the Linux variations which is currently used to determine if an
     * RI is instance size flexible.
     */
    public static final Set<OSType> LINUX_BASED_OS_SET = Sets.immutableEnumSet(
            OSType.LINUX,
            OSType.LINUX_WITH_SQL_STANDARD,
            OSType.LINUX_WITH_SQL_WEB,
            OSType.LINUX_WITH_SQL_ENTERPRISE);

    /**
     * The name of the entity id column in the RI coverage database tables.
     */
    public static final String ENTITY_ID = "entity_id";

    public static final String AVAILABILITY_ZONE_ID = "availability_zone_id";

    public static final String BUSINESS_ACCOUNT_ID = "business_account_id";

    /**
     * Get a list of table fields for plan reserved instance utilization and coverage stats query.
     *
     * @param table the table need to query.
     * @return a list of table fields.
     */
    public static List<Field<?>> createSelectFieldsForPlanRIUtilizationCoverage(@Nonnull final Table<?> table) {
        return Lists.newArrayList(
                sum(((Field<? extends Number>)table.field(TOTAL_COUPONS))).as(TOTAL_COUPONS_SUM_VALUE),
                avg(((Field<? extends Number>)table.field(TOTAL_COUPONS))).as(TOTAL_COUPONS_AVG_VALUE),
                max(((Field<? extends Number>)table.field(TOTAL_COUPONS))).as(TOTAL_COUPONS_MAX_VALUE),
                min(((Field<? extends Number>)table.field(TOTAL_COUPONS))).as(TOTAL_COUPONS_MIN_VALUE),
                sum(((Field<? extends Number>)table.field(USED_COUPONS))).as(USED_COUPONS_SUM_VALUE),
                avg(((Field<? extends Number>)table.field(USED_COUPONS))).as(USED_COUPONS_AVG_VALUE),
                max(((Field<? extends Number>)table.field(USED_COUPONS))).as(USED_COUPONS_MAX_VALUE),
                min(((Field<? extends Number>)table.field(USED_COUPONS))).as(USED_COUPONS_MIN_VALUE)
        );
    }

    /**
     * Get a list of table fields for reserved instance utilization and coverage stats query.
     *
     * @param table the table need to query.
     * @return a list of table fields.
     */
    public static List<Field<?>> createSelectFieldsForRIUtilizationCoverage(@Nonnull final Table<?> table) {
        final List<Field<?>> fields = createSelectFieldsForPlanRIUtilizationCoverage(table);
        fields.add(table.field(SNAPSHOT_TIME));
        return fields;
    }

    /**
     * Convert {@link Record} to a {@link ReservedInstanceStatsRecord}.
     *
     * @param record {@link Record} which contains the aggregated plan reserved instance stats data.
     * @param endDate End date (if non-0) to use as stats snapshot time.
     * @return a {@link ReservedInstanceStatsRecord}.
     */
    public static ReservedInstanceStatsRecord convertPlanRIUtilizationCoverageRecordToRIStatsRecord(
            @Nonnull final Record record, long endDate) {
        final ReservedInstanceStatsRecord.Builder statsRecord = convertCouponValues(record);
        final long projectedTimeMillis = endDate != 0 ? endDate : Clock.systemUTC().instant()
                .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
        statsRecord.setSnapshotDate(projectedTimeMillis);
        return statsRecord.build();
    }

    private static ReservedInstanceStatsRecord.Builder convertCouponValues(final Record record) {
        final ReservedInstanceStatsRecord.Builder statsRecord = ReservedInstanceStatsRecord.newBuilder();
        statsRecord.setCapacity(StatValue.newBuilder()
                .setTotal(record.getValue(TOTAL_COUPONS_SUM_VALUE, Float.class))
                .setAvg(record.getValue(TOTAL_COUPONS_AVG_VALUE, Float.class))
                .setMax(record.getValue(TOTAL_COUPONS_MAX_VALUE, Float.class))
                .setMin(record.getValue(TOTAL_COUPONS_MIN_VALUE, Float.class)));
        statsRecord.setValues(StatValue.newBuilder()
                .setTotal(record.getValue(USED_COUPONS_SUM_VALUE, Float.class))
                .setAvg(record.getValue(USED_COUPONS_AVG_VALUE, Float.class))
                .setMax(record.getValue(USED_COUPONS_MAX_VALUE, Float.class))
                .setMin(record.getValue(USED_COUPONS_MIN_VALUE, Float.class)));
        return statsRecord;
    }

    /**
     * Convert {@link Record} to a {@link ReservedInstanceStatsRecord}.
     *
     * @param record {@link Record} which contains the aggregated plan reserved instance stats data.
     * @return a {@link ReservedInstanceStatsRecord}.
     */
    public static ReservedInstanceStatsRecord convertRIUtilizationCoverageRecordToRIStatsRecord(
            @Nonnull final Record record) {
        final ReservedInstanceStatsRecord.Builder statsRecord = convertCouponValues(record);
        statsRecord.setSnapshotDate(record.getValue(SNAPSHOT_TIME, Timestamp.class).getTime());
        return statsRecord.build();
    }

    public static ReservedInstanceStatsRecord createRIStatsRecord(float totalCoupons,
                                                                  float usedCoupons,
                                                                  long snapshotTime) {
        final ReservedInstanceStatsRecord.Builder statsRecord = ReservedInstanceStatsRecord.newBuilder();
        statsRecord.setCapacity(StatValue.newBuilder()
                .setTotal(totalCoupons)
                .setAvg(totalCoupons)
                .setMax(totalCoupons)
                .setMin(totalCoupons));
        statsRecord.setValues(StatValue.newBuilder()
                .setTotal(usedCoupons)
                .setAvg(usedCoupons)
                .setMax(usedCoupons)
                .setMin(usedCoupons));
        statsRecord.setSnapshotDate(snapshotTime);
        return statsRecord.build();
    }
}
