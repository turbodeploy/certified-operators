package com.vmturbo.api.component.external.api.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.stats.ImmutableTimeWindow;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext.TimeWindow;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class StatsUtils {

    private static final Logger logger = LogManager.getLogger();

    private static final String KBIT_SEC = "Kbit/sec";
    private static final String BIT_SEC = "bit/sec";
    /**
     * Prefix used for commodity key.
     */
    public static final String COMMODITY_KEY_PREFIX = "KEY: ";

    // map of commodity number to units-multiplier pair
    private static final Map<Integer, Pair<String, Integer>> UNITS_CONVERTER = ImmutableMap.of(
        CommodityType.PORT_CHANEL_VALUE, new Pair<>(KBIT_SEC, 8),
        CommodityType.NET_THROUGHPUT_VALUE, new Pair<>(KBIT_SEC, 8),
        CommodityType.IO_THROUGHPUT_VALUE, new Pair<>(KBIT_SEC, 8),
        CommodityType.SWAPPING_VALUE, new Pair<>(BIT_SEC, 8));

    private static final Set<ApiEntityType> SUPPORTED_RI_FILTER_TYPES =
            Sets.immutableEnumSet(ApiEntityType.AVAILABILITY_ZONE, ApiEntityType.BUSINESS_ACCOUNT,
                    ApiEntityType.REGION, ApiEntityType.SERVICE_PROVIDER);

    /**
     * Convert the default commodity units into converted units with multiplier that we need to
     * to the current value.
     *
     * @param commodityTypeValue the target commodity type value.
     * @param commodityTypeUnits the current commodity units.
     * @return pair of the converted units and multiplier.
     */
    public static Pair<String, Integer> getConvertedUnits(final int commodityTypeValue,
        @Nonnull final CommodityTypeUnits commodityTypeUnits) {
        if (!UNITS_CONVERTER.containsKey(commodityTypeValue)) {
            logger.warn("No converted units found for commodity type {}, will use the original " +
                "units", commodityTypeValue);
            return new Pair<>(commodityTypeUnits.getUnits(), 1);
        }
        return UNITS_CONVERTER.get(commodityTypeValue);
    }

    /**
     * Multiply all the values of a {@link StatValueApiDTO}.
     *
     * @param valueDTO stat value DTO.
     * @param multiplier the multiplier that apply to the stat value.
     */
    public static void convertDTOValues(@Nonnull final StatValueApiDTO valueDTO,
        final int multiplier) {
        valueDTO.setAvg(multiply(valueDTO.getAvg(), multiplier));
        valueDTO.setMin(multiply(valueDTO.getMin(), multiplier));
        valueDTO.setMax(multiply(valueDTO.getMax(), multiplier));
        valueDTO.setTotal(multiply(valueDTO.getTotal(), multiplier));
    }

    /**
     * Round a double value to float value in specific precision.
     *
     * @param value value to round.
     * @param precision number of decimals.
     * @return rounded value.
     */
    public static float round(final double value, final int precision) {
        return new BigDecimal(value).setScale(precision, RoundingMode.HALF_UP).floatValue();
    }

    @Nullable
    private static Float multiply(@Nullable final Float d, int multiplier) {
        return d != null && !Float.isNaN(d) && !Float.isInfinite(d) ? d * multiplier : null;
    }

    public static boolean isValidScopeForRIBoughtQuery(@Nonnull ApiId scope) {
        //Only allow non-scoped-observer users.
        if (UserScopeUtils.isUserObserver() && UserScopeUtils.isUserScoped()) {
            return false;
        }
        // Always true for Plans where the entities are Cloud
        if (scope.isCloudPlan()) {
            return true;
        }
        final Optional<Set<ApiEntityType>> scopeTypes = scope.getScopeTypes();

        // This is the case where the user might query for an invalid oid. The scope types will not be
        // present. However, the scope types are not present for a global scope as well, so we only return
        // false from here if the market is not realtime (global scope)
        if (!scope.isRealtimeMarket()  && !scopeTypes.isPresent()) {
            return false;
        }
        return scopeTypes
                // If this is scoped to a set of entity types, if any of the scope entity types
                // are supported, RIs will be scoped through the supported types and non-supported
                // types will be ignored
                .map(scopeEntityTypes -> !Sets.intersection(SUPPORTED_RI_FILTER_TYPES, scopeEntityTypes).isEmpty())
                // this is a global or plan scope
                .orElse(true);
    }

    /**
     * If ONLY one of startDate/endDate is provided, adjust their values so that are
     * consistent in what they represent. (If both of them are provided or none of them is,
     * there is no need to modify anything so the function does nothing.)
     * Adjustment goes as follows:
     * - If only endDate is provided:
     * --- if end date is in the past, throw an exception since no assumption can be made for
     *      the start date; it is considered invalid input.
     * --- if end date is now* , set it to null. (When both dates are null, the most recent
     *      timestamp is returned.)
     * --- if end date is in the future, set the start date to current time
     * - If only startDate is provided:
     * --- if start date is in the past, set the end date to current time.
     * --- if start date is now* , set it to null. (When both dates are null, the most recent
     *      timestamp is returned.)
     * --- if start date is in the future, set the end date equal to start date.
     *
     * <p>*'now' is considered a small time frame of current time up to a minute ago/after.
     *
     * @param period user requested dto
     * @param clockTimeNow current time in millis
     * @param toleranceInMillis the tolerance time within which is considered as current
     * @return optional TimeWindow
     */
    public static Optional<TimeWindow> sanitizeStartDateOrEndDate(
            @Nullable StatPeriodApiInputDTO period, final long clockTimeNow, long toleranceInMillis) {
        if (period == null || (period.getStartDate() == null && period.getEndDate() == null)) {
            // no need to modify
            return Optional.empty();
        }

        // give the startTime a +/- 60 second window for delineating between "current" and
        // "projected" stats requests. Without this window, the stats retrieval is too sensitive
        // to clock skew issues between the browser and the server, leading to incorrect results
        // in the UI.
        final long currentStatsTimeWindowStart = clockTimeNow - toleranceInMillis;
        final long currentStatsTimeWindowEnd = clockTimeNow + toleranceInMillis;

        Long startTime = null;
        Long endTime = null;
        if (period.getStartDate() == null) {
            // in this case we have just endDate
            endTime = DateTimeUtil.parseTime(period.getEndDate());
            if (endTime < currentStatsTimeWindowStart) {
                // end date in the past
                throw new IllegalArgumentException(
                        "Incorrect combination of start and end date: Start date missing & end date in the past.");
            } else if (endTime <= currentStatsTimeWindowEnd) {
                // end date now (belongs to [currentStatsTimeWindowStart, currentStatsTimeWindowEnd])
                endTime = null;
            } else {
                // end date in the future
                startTime = clockTimeNow;
            }
        } else if (period.getEndDate() == null) {
            // in this case we have just startDate
            startTime = DateTimeUtil.parseTime(period.getStartDate());
            if (startTime < currentStatsTimeWindowStart) {
                // start date in the past
                period.setEndDate(Long.toString(clockTimeNow));
                endTime = clockTimeNow;
            } else if (startTime <= currentStatsTimeWindowEnd) {
                // start date now (belongs to [currentStatsTimeWindowStart, currentStatsTimeWindowEnd])
                startTime = null;
            } else {
                // start date in the future
                endTime = startTime;
            }
        } else {
            // if none of startDate, endDate is null, there is no need to modify anything
            startTime = DateTimeUtil.parseTime(period.getStartDate());
            endTime = DateTimeUtil.parseTime(period.getEndDate());
        }

        // modify input dto
        period.setStartDate(startTime == null ? null : Long.toString(startTime));
        period.setEndDate(endTime == null ? null : Long.toString(endTime));

        // starttime and endtime should be both nonnull or both null
        if (startTime != null && endTime != null) {
            // create TimeWindow based on sanitized date
            return Optional.of(ImmutableTimeWindow.builder()
                    .startTime(startTime)
                    .endTime(endTime)
                    .includeHistorical(startTime < currentStatsTimeWindowStart)
                    .includeCurrent(startTime <= currentStatsTimeWindowEnd && endTime >= currentStatsTimeWindowStart)
                    .includeProjected(endTime > currentStatsTimeWindowEnd)
                    .build());
        } else {
            // both startTime and endTime are null
            return Optional.empty();
        }
    }

    /**
     * Get the string formatted date to use for projected stat snapshot.
     *
     * @param timeWindow time window of input
     * @param currentTimeInMillis current time
     * @param liveStatsRetrievalWindow the tolerance time
     * @return string date for projected stat
     */
    public static String getProjectedStatSnapshotDate(Optional<TimeWindow> timeWindow,
            long currentTimeInMillis, Duration liveStatsRetrievalWindow) {
        return DateTimeUtil.toString(timeWindow
                .map(TimeWindow::endTime)
                // If the request didn't have an explicit end time, set the time the future (and beyond).
                // We want it to be out of the "live stats retrieval window" (to keep the semantics
                // that anything within the live stats retrieval window = current stats), so we add
                // a minute.
                .orElseGet(() -> currentTimeInMillis + liveStatsRetrievalWindow.plusMinutes(1).toMillis()));
    }

    /**
     * Precision enum for API to calculate the value after rounding.
     */
    public enum PrecisionEnum {
        /**
         * For cost price we set the round precision 7. Since we also do some price calculation on
         * UI, a higher precision will be good for accurate value.
         */
        COST_PRICE(7),
        /**
         * For the other kind of stats value, the round precision will be 2.
         */
        STATS(2);

        private final int precision;

        PrecisionEnum(final int precision) {
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }
    }
}
