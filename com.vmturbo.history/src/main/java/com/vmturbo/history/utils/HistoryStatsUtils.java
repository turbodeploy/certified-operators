package com.vmturbo.history.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.stats.live.ConfiguredPropertyType;
import com.vmturbo.history.stats.live.PropertyType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class HistoryStatsUtils {
    private static final Logger logger = LogManager.getLogger();

    private HistoryStatsUtils() {
    }

    /**
     * The default price index value.
     */
    public static final double DEFAULT_PRICE_IDX = 1.0f;

    private static final Set<Table<?>> MARKET_TABLES = ImmutableSet.<Table<?>>builder()
        .add(Tables.MARKET_STATS_LATEST)
        .add(Tables.MARKET_STATS_BY_HOUR)
        .add(Tables.MARKET_STATS_BY_DAY)
        .add(Tables.MARKET_STATS_BY_MONTH)
        .build();

    /**
     * If any of these metrics are requested, we need to post-process the response and tally
     * up counts.
     */
    public static final Set<String> countPerSEsMetrics =
        ConfiguredPropertyType.getComputedPropertyTypes().stream()
            .map(PropertyType::getName)
            .collect(ImmutableSet.toImmutableSet());

    /**
     * Map to link any of the post-processed count metrics requested to the corresponding
     * entity types.
     */
    public static final ImmutableBiMap<String, String> countSEsMetrics =
            ConfiguredPropertyType.getMetricPropertyTypes().stream()
                    .collect(ImmutableBiMap.toImmutableBiMap(
                            prop -> prop.getCountedEntityType().getName(),
                            PropertyType::getName));


    /**
     * Map from Database EntityType String name to SDK EntityType Enum numeric value for
     * Entity Types to be counted.
     */
    public static final ImmutableMap<CommonDTO.EntityDTO.EntityType, EntityType>
            SDK_ENTITY_TYPE_TO_ENTITY_TYPE =
            new ImmutableMap.Builder<CommonDTO.EntityDTO.EntityType, EntityType>()
                    .put(CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION, EntityDTO.EntityType.BUSINESS_APPLICATION)
                    .put(EntityDTO.EntityType.BUSINESS_TRANSACTION, EntityDTO.EntityType.BUSINESS_TRANSACTION)
                    .put(CommonDTO.EntityDTO.EntityType.SERVICE, EntityDTO.EntityType.SERVICE)
                    .put(CommonDTO.EntityDTO.EntityType.APPLICATION_SERVER, EntityDTO.EntityType.APPLICATION_SERVER)
                    .put(CommonDTO.EntityDTO.EntityType.APPLICATION, EntityDTO.EntityType.APPLICATION)
                    .put(EntityDTO.EntityType.APPLICATION_COMPONENT, EntityDTO.EntityType.APPLICATION_COMPONENT)
                    .put(CommonDTO.EntityDTO.EntityType.CHASSIS, EntityDTO.EntityType.CHASSIS)
                    .put(CommonDTO.EntityDTO.EntityType.CONTAINER, EntityType.CONTAINER)
                    .put(CommonDTO.EntityDTO.EntityType.CONTAINER_POD, EntityType.CONTAINER_POD)
                    // DC's are intentionally mapped to PM's
                    .put(CommonDTO.EntityDTO.EntityType.DATACENTER, EntityType.PHYSICAL_MACHINE)
                    .put(CommonDTO.EntityDTO.EntityType.DISK_ARRAY, EntityDTO.EntityType.DISK_ARRAY)
                    .put(CommonDTO.EntityDTO.EntityType.DPOD, EntityType.DPOD)
                    .put(CommonDTO.EntityDTO.EntityType.IO_MODULE, EntityDTO.EntityType.IO_MODULE)
                    .put(CommonDTO.EntityDTO.EntityType.LOGICAL_POOL, EntityType.LOGICAL_POOL)
                    .put(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE, EntityType.PHYSICAL_MACHINE)
                    .put(CommonDTO.EntityDTO.EntityType.RESERVED_INSTANCE, EntityType.RESERVED_INSTANCE)
                    .put(CommonDTO.EntityDTO.EntityType.STORAGE, EntityType.STORAGE)
                    .put(CommonDTO.EntityDTO.EntityType.STORAGE_CONTROLLER, EntityDTO.EntityType.STORAGE_CONTROLLER)
                    .put(CommonDTO.EntityDTO.EntityType.SWITCH, EntityDTO.EntityType.SWITCH)
                    .put(CommonDTO.EntityDTO.EntityType.VIRTUAL_DATACENTER, EntityDTO.EntityType.VIRTUAL_DATACENTER)
                    .put(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE)
                    .put(CommonDTO.EntityDTO.EntityType.VPOD, EntityType.VPOD)
                    .put(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER, EntityType.DATABASE_SERVER)
                    .put(CommonDTO.EntityDTO.EntityType.DESKTOP_POOL, EntityDTO.EntityType.DESKTOP_POOL)
                    .put(CommonDTO.EntityDTO.EntityType.BUSINESS_USER, EntityDTO.EntityType.BUSINESS_USER)
                    .put(CommonDTO.EntityDTO.EntityType.VIEW_POD, EntityDTO.EntityType.VIEW_POD)
                    .put(CommonDTO.EntityDTO.EntityType.DATABASE, EntityType.DATABASE)
                    .build();

    /**
     * Map from SDK EntityType Enum to Database EntityType Enum without mapping DATACENTER to PM.
     */
    public static final ImmutableMap<CommonDTO.EntityDTO.EntityType, EntityType>
        SDK_ENTITY_TYPE_TO_ENTITY_TYPE_NO_ALIAS =
        new ImmutableMap.Builder<CommonDTO.EntityDTO.EntityType, EntityType>()
            .put(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE, EntityType.VIRTUAL_MACHINE)
            .put(CommonDTO.EntityDTO.EntityType.DATACENTER, EntityType.DATACENTER)
            .put(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE, EntityType.PHYSICAL_MACHINE)
            .put(CommonDTO.EntityDTO.EntityType.STORAGE, EntityType.STORAGE)
            .put(CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION, EntityType.BUSINESS_APPLICATION)
            .put(EntityDTO.EntityType.BUSINESS_TRANSACTION, EntityType.BUSINESS_TRANSACTION)
            .put(CommonDTO.EntityDTO.EntityType.SERVICE, EntityType.SERVICE)
            .put(CommonDTO.EntityDTO.EntityType.APPLICATION_SERVER, EntityType.APPLICATION_SERVER)
            .put(EntityDTO.EntityType.APPLICATION_COMPONENT, EntityType.APPLICATION_COMPONENT)
            .put(CommonDTO.EntityDTO.EntityType.APPLICATION, EntityType.APPLICATION)
            .put(CommonDTO.EntityDTO.EntityType.CHASSIS, EntityType.CHASSIS)
            .put(CommonDTO.EntityDTO.EntityType.DISK_ARRAY, EntityType.DISK_ARRAY)
            .put(CommonDTO.EntityDTO.EntityType.IO_MODULE, EntityType.IO_MODULE)
            .put(CommonDTO.EntityDTO.EntityType.STORAGE_CONTROLLER, EntityType.STORAGE_CONTROLLER)
            .put(CommonDTO.EntityDTO.EntityType.SWITCH, EntityType.SWITCH)
            .put(CommonDTO.EntityDTO.EntityType.VIRTUAL_DATACENTER, EntityType.VIRTUAL_DATACENTER)
            .put(CommonDTO.EntityDTO.EntityType.CONTAINER, EntityType.CONTAINER)
            .put(CommonDTO.EntityDTO.EntityType.CONTAINER_POD, EntityType.CONTAINER_POD)
            .put(CommonDTO.EntityDTO.EntityType.LOGICAL_POOL, EntityType.LOGICAL_POOL)
            .put(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER, EntityType.DATABASE_SERVER)
            .put(CommonDTO.EntityDTO.EntityType.DESKTOP_POOL, EntityType.DESKTOP_POOL)
            .put(CommonDTO.EntityDTO.EntityType.BUSINESS_USER, EntityType.BUSINESS_USER)
            .put(CommonDTO.EntityDTO.EntityType.VIEW_POD, EntityType.VIEW_POD)
            .put(CommonDTO.EntityDTO.EntityType.DATABASE, EntityType.DATABASE)
            .build();

    /**
     * Convert an int commodityType value, as defined by the SDK, into a mixed-case name.
     * The UX expects commodity names in this format.
     *
     * @param intCommodityType the int value for the {@link CommodityType} enum for the given
     *                         commodity
     * @return a mixed-case string name for the commodity, or null if the mixed-case name cannot
     * be determined.
     */
    public static String formatCommodityName(int intCommodityType) {
        return formatCommodityName(intCommodityType, Optional.empty());
    }

    /**
     * Convert an int commodityType value, as defined by the SDK, into a mixed-case name.
     * The UX expects commodity names in this format.
     *
     * @param intCommodityType the int value for the {@link CommodityType} enum for the given
     *                         commodity
     * @param prefixString a string to prepend to the mixed case name.
     * @return a mixed-case string name for the commodity, or null if the mixed-case name cannot
     * be determined.
     */
    public static String formatCommodityName(int intCommodityType, String prefixString) {
        return formatCommodityName(intCommodityType, Optional.of(prefixString));
    }

    /**
     * Convert an int commodityType value, as defined in {@link CommodityType} - in the SDK -
     * into a mixed-case name using {@link CommodityTypeUnits} - with
     * an optional prefix.
     *
     * <p>For example, the int value for SWAPPING is 33;  and is mapped to "Swapping".
     *
     * <p>If the prefix is supplied, then the first character of the mixed
     * case name is upcased.
     *
     * <p>for example:
     * <ul>
     *     <li>prefix null, commodity "NUM_CPUS" -> "numCPUs"
     *     <li>prefix "sold", commodity "NUM_CPUS" -> "soldNumCPUs"
     *     <li>prefix null, commodity "CPU_ALLOCATION" -> "CPUAllocation"
     *     <li>prefix "sold", commodity "CPU_ALLOCATION" -> "soldCPUAllocation"
     * </ul>
     *
     * @param intCommodityType the int value for the {@link CommodityType} enum for the given
     *                         commodity
     * @param propertyPrefix an optional prefix to prepend the mixed case name.
     * @return the mixed case commodity name used by the DB, optionally prepended with
     * the given propertyPrefix
     */
    public static @Nullable String formatCommodityName(int intCommodityType,
                                                       @Nonnull Optional<String> propertyPrefix) {
        final CommodityType commodityType = CommodityType.forNumber(intCommodityType);
        if (commodityType == null) {
            logger.warn("Commodity type (" + intCommodityType + ") not found");
            return null;
        }
        final String upcaseCommodityName = commodityType.name();
        String mixedCaseName;
        try {
            mixedCaseName = CommodityTypeUnits.valueOf(upcaseCommodityName).getMixedCase();
        } catch (IllegalArgumentException e) {
            // if this happens there are commodities for which the Enum value is missing
            mixedCaseName = upcaseCommodityName;
        }
        if (!propertyPrefix.isPresent()) {
            return mixedCaseName;
        }
        if (mixedCaseName.length() == 0) {
            return propertyPrefix.get();
        }
        return addPrefix(mixedCaseName, propertyPrefix.get());
    }

    public static String addPrefix(@Nonnull final String commodityName,
                                   @Nonnull final String prefix) {
        return prefix.isEmpty() ? commodityName : prefix + StringUtils.capitalize(commodityName);
    }

    /**
     * Produces a 'between' condition for the beginning/end of the times provided.
     * If the TimeFrame is 'hour' the condition will be for the beginning of the
     * hour of 'startTime' and the end of the hour (last second) of 'endTime'.
     *
     * <p>Note that the GUI has selectors "hour", "day", "week", "month", "year".  Each of these
     * selected user-facing ranges is fetched from the next-lower time-frame stats table.
     * E.g. when the "day" UI button is clicked the stats are fetched from the "xxx_by_hour" table,
     * and when the "year" UI button is clicked the stats are fetched from the "xxx_by_month" table.
     *
     * <p>Further, the end-time is extended to the end of the period in which "endTime" falls. since
     * the stats are rolled up to the end of each period.  For example, if the request is for
     * Jan 9 5PM to Jan 10 5PM we want to include both the Jan 9 roll-up and the Jan 10 rollup.
     * To do that, the time range returned in this "between" clause would run from
     * Jan 9, 0:0:0, to Jan 10, 23:59:59. Note that the "between" selection is inclusive of the
     * endpoints.
     *
     * @param dField - The field on which the condition runs
     * @param tFrame - The table timeFrame used for the condition
     * @param startTime the epoc timestamp for the beginning of the "where" clause
     * @param endTime the epoc timestamp for the end of the "where" clause
     * @return an sql "between" {@link Condition} specifying the snapshot_time range to be included in the
     * query results
     */
    public static Condition betweenStartEndTimestampCond(Field<?> dField,
                                                         TimeFrame tFrame,
                                                         long startTime,
                                                         long endTime) {
        switch (tFrame) {
            case LATEST:
                return timestamp(dField).between(new java.sql.Timestamp(startTime),
                    new java.sql.Timestamp(endTime));
            case HOUR:
                return timestamp(dField).between(startOfHour(new java.sql.Timestamp(startTime)),
                    endOfHour(new java.sql.Timestamp(endTime)));
            case DAY:
                return timestamp(dField).between(startOfDay(new Timestamp(startTime)),
                    endOfDay(new Timestamp(endTime)));
            case MONTH:
                return timestamp(dField).between(startOfMonth(new Timestamp(startTime)),
                    endOfMonth(new Timestamp(endTime)));
            default:
                return null;
        }
    }

    /**
     * Clip a {@link Timestamp} to the beginning of the hour of the given timestamp. Minutes, seconds, and
     * milliseconds are zero'ed.
     *
     * @param timeStamp the {@link Timestamp} to clip
     * @return a {@link Timestamp} with the mins, secs, and ms set to zero
     */
    public static java.sql.Timestamp startOfHour(Timestamp timeStamp) {
        Calendar answer = Calendar.getInstance();
        answer.setTime(timeStamp);
        zeroMinutes(answer);
        return new Timestamp(answer.getTimeInMillis());
    }

    /**
     * Clip a {@link Timestamp} to the beginning of the day of the given timestamp. Day of Month,
     * Minutes, seconds, and milliseconds are zero'ed.
     *
     * @param timeStamp the {@link Timestamp} to clip
     * @return a {@link Timestamp} with the day of month set to 1, mins, secs, and ms set to zero
     */
    public static java.sql.Timestamp startOfDay(Date timeStamp) {
        Calendar answer = Calendar.getInstance();
        answer.setTime(timeStamp);
        zeroHour(answer);
        return new java.sql.Timestamp(answer.getTimeInMillis());
    }

    /**
     * Clip a {@link Timestamp} to the beginning of the month of the given timestamp. Month,
     * Day of Month, Minutes, seconds, and milliseconds are zero'ed.
     *
     * @param timeStamp the {@link Timestamp} to clip
     * @return a {@link Timestamp} with the day of month set to 1, mins, secs, and ms set to zero
     */
    public static java.sql.Timestamp startOfMonth(Timestamp timeStamp) {
        Calendar answer = Calendar.getInstance();
        answer.setTime(timeStamp);
        zeroDay(answer);
        return new java.sql.Timestamp(answer.getTimeInMillis());
    }

    /**
     * Roll a {@link Timestamp} to the end of the hour of the given timestamp.
     * Calculation:
     * <ol>
     *     <li>zero the minutes -  h:m -> h:0</li>
     *     <li>add one to the hour -: h:0 -> (h+1):0</li>
     *     <li>back up 1 second - (h+1):0 -> h:59</li>
     * </ol>
     *
     * @param timeStamp the {@link Timestamp} to roll forward
     * @return a {@link Timestamp} with the mins, secs, and ms set to max
     */
    private static java.sql.Timestamp endOfHour(Timestamp timeStamp) {
        Calendar answer = Calendar.getInstance();
        answer.setTime(timeStamp);
        // back to beginning of the current hour
        zeroMinutes(answer);
        // roll up to the next hour
        answer.add(Calendar.HOUR, 1);
        // roll back 1 sec == end of hour
        answer.add(Calendar.SECOND, -1);
        return new Timestamp(answer.getTimeInMillis());
    }

    /**
     * Roll a {@link Timestamp} to the end of the day of the given timestamp.
     * Calculation:
     * <ol>
     *     <li>zero the hour and minutes -  d:h:m -> d:0:0</li>
     *     <li>add one to the hour -: d:0:0 -> (d+1):0:0</li>
     *     <li>back up 1 second - (d+1):0:0 -> d:59:59</li>
     * </ol>
     *
     * @param timeStamp the {@link Timestamp} to roll forward
     * @return a {@link Timestamp} with the hours, mins, secs, and ms set to max
     */
    private static java.sql.Timestamp endOfDay(Timestamp timeStamp) {
        Calendar answer = Calendar.getInstance();
        answer.setTime(timeStamp);
        // back to beginning of the current day
        zeroHour(answer);
        // roll up to the next day
        answer.add(Calendar.DATE, 1);
        // roll back 1 sec == end of day
        answer.add(Calendar.SECOND, -1);
        return new java.sql.Timestamp(answer.getTimeInMillis());
    }

    /**
     * Roll a {@link Timestamp} to the end of the month of the given timestamp.
     * Calculation:
     * <ol>
     *     <li>zero the day, hour, minutes -  M:d:h:m -> M:0:0:0</li>
     *     <li>add one to the hour -: M:0:0:0 -> (M+1):0:0:0</li>
     *     <li>back up 1 second - (M+1):0:0:0 -> M:23:59:59</li>
     * </ol>
     *
     * @param timeStamp the {@link Timestamp} to roll forward
     * @return a {@link Timestamp} with the day-of-month set to max
     */
    private static java.sql.Timestamp endOfMonth(Timestamp timeStamp) {
        Calendar answer = Calendar.getInstance();
        answer.setTime(timeStamp);
        // back to the beginning of the month
        zeroDay(answer);
        // move to next month
        answer.add(Calendar.MONTH, 1);
        // move back 1 sec = end of month
        answer.add(Calendar.SECOND, -1);
        return new java.sql.Timestamp(answer.getTimeInMillis());
    }

    /**
     * Set the Minute, Second, and MS fields of the given {@link Calendar} to the min.
     * @param answer the {@link Calendar} with the Minute, Second, and MS set to min
     */
    private static void zeroMinutes(Calendar answer) {
        answer.set(Calendar.MINUTE, answer.getActualMinimum(Calendar.MINUTE));
        answer.set(Calendar.SECOND, answer.getActualMinimum(Calendar.SECOND));
        answer.set(Calendar.MILLISECOND, answer.getActualMinimum(Calendar.MILLISECOND));
    }

    /**
     * Set the Hour, Minute, Second, and MS fields of the given {@link Calendar} to the min.
     * @param answer the {@link Calendar} with the Hour, Minute, Second, and MS set to min
     */
    private static void zeroHour(Calendar answer) {
        zeroMinutes(answer);
        answer.set(Calendar.HOUR_OF_DAY, answer.getActualMinimum(Calendar.HOUR_OF_DAY));
    }

    /**
     * Zero the Day, Minute, Second, and MS fields of the given {@link Calendar}.
     * @param answer the {@link Calendar} to zero
     */
    private static void zeroDay(Calendar answer) {
        zeroHour(answer);
        answer.set(Calendar.DAY_OF_MONTH, answer.getActualMinimum(Calendar.DAY_OF_MONTH));
    }

    @SuppressWarnings("unchecked")
    public static Field<Timestamp> timestamp(Field<?> field){
        checkNotNull(field);
        checkFieldType(field.getType(), Timestamp.class);
        return (Field<Timestamp>)field;
    }

    /*
     * Type-safe wrappers for casting Fields to the required generic type.
     */
    private static void checkFieldType(Class<?> given, Class<?> expected){
        checkFieldType(given, expected, false);
    }

    private static void checkFieldType(Class<?> given, Class<?> expected, boolean subClsOK){
        checkArgument(subClsOK ? expected.isAssignableFrom(given) : given==expected,
            "Incorrect field type %s (expected %s)",
            given.getName(), expected.getName());
    }

    public static boolean isMarketStatsTable(@Nonnull final Table<?> table) {
        return MARKET_TABLES.contains(table);
    }

    /**
     * List of entities relevant for headroom calculation.
     */
    private static final Set<Integer> HEADROOM_ENTITY_TYPES =
        ImmutableSet.of(EntityDTO.EntityType.STORAGE_VALUE,
            EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE,
            EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE);

    public static Set<Integer> getHeadroomEntityTypes() {
        return HEADROOM_ENTITY_TYPES;
    }
}
