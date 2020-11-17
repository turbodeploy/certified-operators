package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_BOUGHT;
import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;
import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.SelectWhereStep;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCostFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;
import com.vmturbo.sql.utils.DbException;

/**
 * This class is used to update reserved instance table by latest reserved instance bought data which
 * comes from Topology Processor. It uses "probeReservedInstanceId" to tell if the latest reserved instance
 * is same with current existing reserved instance record or not.
 */
public class SQLReservedInstanceBoughtStore extends AbstractReservedInstanceStore
        implements ReservedInstanceBoughtStore {

    private static final String reservedInstanceBoughtDumpFile = "reservedInstanceBought_dump";

    private static final String RI_AMORTIZED_COST = "ri_amortized_cost";

    private static final String RI_RECURRING_COST = "ri_recurring_cost";

    private static final String RI_FIXED_COST = "ri_fixed_cost";

    private static final Field<Object> RESERVED_INSTANCE_ID_FIELD = DSL.field(
            "reserved_instance_id");

    private static final Field<Double> USED_COUPONS_FIELD = DSL.field("used_coupons", Double.class);

    private final PriceTableStore priceTableStore;

    private final Set<Runnable> updateCallbacks = new HashSet<>();

    @Override
    public DSLContext getDSLContext() {
        return getDsl();
    }

    @Override
    public TableImpl<ReservedInstanceBoughtRecord> getTable() {
        return RESERVED_INSTANCE_BOUGHT;
    }

    @Nonnull
    @Override
    public String getFileName() {
        return reservedInstanceBoughtDumpFile;
    }

    /**
     * Construct a new SQL implementation of {@link ReservedInstanceBoughtStore}.
     * @param dsl The {@link DSLContext} to use for queries & transactions
     * @param identityProvider The identity provider
     * @param reservedInstanceCostCalculator The calculator for RI related costs.
     * @param priceTableStore The {@link PriceTableStore}, used to calculate RI costs when discovery
     * @param entityReservedInstanceMappingStore The {@Link EntityReservedInstanceMappingStore},
     *                                          used to look up discovered accounts usage for an RI.
     * @param accountRIMappingStore The {@Link AccountRIMappingStore}, used to look up
     *                             undiscovered accounts usage for an RI.
     * @param businessAccountHelper Business Account helper to look up discovered accounts.
     */
    public SQLReservedInstanceBoughtStore(@Nonnull final DSLContext dsl,
                                          @Nonnull final IdentityProvider identityProvider,
                                          @Nonnull final ReservedInstanceCostCalculator reservedInstanceCostCalculator,
                                          @Nonnull final PriceTableStore priceTableStore,
                                          @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
                                          @Nonnull final AccountRIMappingStore accountRIMappingStore,
                                          @Nonnull final BusinessAccountHelper businessAccountHelper) {
        super(dsl, identityProvider, reservedInstanceCostCalculator,
                accountRIMappingStore, entityReservedInstanceMappingStore, businessAccountHelper);
        this.priceTableStore = priceTableStore;
    }

    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilter(
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        return getReservedInstanceBoughtByFilterWithContext(getDsl(), filter);
    }

    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtByFilterWithContext(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        return internalGet(context, filter).stream()
                .map(this::reservedInstancesToProto)
                .collect(toList());
    }

    @Override
    public Map<Long, Long> getReservedInstanceCountMap(@Nonnull final ReservedInstanceBoughtFilter filter) {
        final Map<Long, Long> retMap = new HashMap<>();
        getDsl().select(RESERVED_INSTANCE_SPEC.TIER_ID,
            (sum(RESERVED_INSTANCE_BOUGHT.COUNT)).as(RI_SUM_COUNT))
            .from(RESERVED_INSTANCE_BOUGHT)
            .join(RESERVED_INSTANCE_SPEC)
            .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
            .where(filter.generateConditions())
            .groupBy(RESERVED_INSTANCE_SPEC.TIER_ID)
            .fetch()
            .forEach(record -> retMap.put(record.value1(), record.value2().longValue()));
        return retMap;
    }

    @Override
    public Cost.ReservedInstanceCostStat getReservedInstanceAggregatedCosts(@Nonnull ReservedInstanceCostFilter filter) {
        final Result<Record3<BigDecimal, BigDecimal, BigDecimal>> riAggregatedCostResult =
                        getDsl().select(sum(RESERVED_INSTANCE_BOUGHT.PER_INSTANCE_AMORTIZED_COST_HOURLY.mul(RESERVED_INSTANCE_BOUGHT.COUNT))
                                                       .as(RI_AMORTIZED_SUM),
                                   sum(RESERVED_INSTANCE_BOUGHT.PER_INSTANCE_RECURRING_COST_HOURLY.mul(RESERVED_INSTANCE_BOUGHT.COUNT))
                                                       .as(RI_RECURRING_SUM),
                                   sum(RESERVED_INSTANCE_BOUGHT.PER_INSTANCE_FIXED_COST.mul(RESERVED_INSTANCE_BOUGHT.COUNT))
                                                       .as(RI_FIXED_SUM))
                            .from(RESERVED_INSTANCE_BOUGHT)
                            .join(RESERVED_INSTANCE_SPEC)
                                .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
                            .where(filter.generateConditions())
                            .fetch();

        return convertToRICostStat(riAggregatedCostResult);
    }

    /**
     * Get the Reserved instance cost stats for each RI Type. The amortized, fixed and recurring costs are
     * multiplied by the number of counts of the RI Type to get the total costs for that RI Type.
     *
     * @param filter filter of type ReservedInstanceCostFilter.
     * @return List of type ReservedInstanceCostStat containing the cost stats.
     */
    private List<Cost.ReservedInstanceCostStat> getReservedInstanceCostStats(@Nonnull ReservedInstanceCostFilter filter) {
        final Result<Record4<Long, Double, Double, Double>> riCostResult =
                        getDsl().select(RESERVED_INSTANCE_BOUGHT.ID,
                                        RESERVED_INSTANCE_BOUGHT.PER_INSTANCE_AMORTIZED_COST_HOURLY
                                                        .mul(RESERVED_INSTANCE_BOUGHT.COUNT)
                                                        .as(RI_AMORTIZED_COST),
                                        RESERVED_INSTANCE_BOUGHT.PER_INSTANCE_FIXED_COST
                                                        .mul(RESERVED_INSTANCE_BOUGHT.COUNT)
                                                        .as(RI_FIXED_COST),
                                        RESERVED_INSTANCE_BOUGHT.PER_INSTANCE_RECURRING_COST_HOURLY
                                                        .mul(RESERVED_INSTANCE_BOUGHT.COUNT)
                                                        .as(RI_RECURRING_COST))
                                        .from(RESERVED_INSTANCE_BOUGHT).join(RESERVED_INSTANCE_SPEC)
                                        .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID
                                                        .eq(RESERVED_INSTANCE_SPEC.ID))
                                        .where(filter.generateConditions()).fetch();
        final List<Cost.ReservedInstanceCostStat> riCostStats = new ArrayList<>();
        for (Record4<Long, Double, Double, Double> record : riCostResult) {
            final Cost.ReservedInstanceCostStat riCostStat = Cost.ReservedInstanceCostStat.newBuilder()
                            .setReservedInstanceOid(
                                            record.get(RESERVED_INSTANCE_BOUGHT.ID))
                            .setRecurringCost(record.get(RI_RECURRING_COST, Double.class))
                            .setFixedCost(record.get(RI_FIXED_COST, Double.class))
                            .setAmortizedCost(record.get(RI_AMORTIZED_COST, Double.class))
                            .setSnapshotTime(now()).build();
            riCostStats.add(riCostStat);
        }
        return riCostStats;
    }

    private static long now() {
        return Clock.systemUTC().instant().toEpochMilli();
    }

    @Nonnull
    @Override
    public List<Cost.ReservedInstanceCostStat> queryReservedInstanceBoughtCostStats(@Nonnull ReservedInstanceCostFilter reservedInstanceCostFilter) {
        final Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy =
                        reservedInstanceCostFilter.getGroupBy();
        if (Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME == groupBy) {
            final Cost.ReservedInstanceCostStat currentRIAggregatedCosts = getReservedInstanceAggregatedCosts(reservedInstanceCostFilter);
            return Collections.singletonList(currentRIAggregatedCosts);
        } else if (Cost.GetReservedInstanceCostStatsRequest.GroupBy.NONE == groupBy) {
            return getReservedInstanceCostStats(reservedInstanceCostFilter);
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Map<Long, Long> getReservedInstanceCountByRISpecIdMap(
            final ReservedInstanceBoughtFilter filter) {
        final SelectJoinStep<Record2<Long, Long>> query = getDsl()
                .select(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID,
                        sum(RESERVED_INSTANCE_BOUGHT.COUNT).cast(Long.class))
                .from(RESERVED_INSTANCE_BOUGHT);
        if (filter.isJoinWithSpecTable()) {
            query.join(RESERVED_INSTANCE_SPEC)
                    .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID
                            .eq(RESERVED_INSTANCE_SPEC.ID));
        }
        return query.where(filter.generateConditions())
                .groupBy(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID)
                .fetchStream().collect(Collectors.toMap(Record2::value1, Record2::value2));
    }

    @Override
    public void updateReservedInstanceBought(
            @Nonnull final DSLContext context,
            @Nonnull final List<ReservedInstanceBoughtInfo> newReservedInstances) {
        getLogger().info("Updating reserved instance bought...");
        final List<ReservedInstanceBoughtInfo> updatedNewReservedInstances =
                internalCheckRIBoughtAndUpdatePrices(newReservedInstances);
        final List<ReservedInstanceBoughtRecord> existingReservedInstances =
                internalGet(context, ReservedInstanceBoughtFilter.newBuilder()
                        .includeExpired(true)
                        .build());
        final Map<String, ReservedInstanceBoughtRecord> existingRIsProbeKeyToRecord =
                existingReservedInstances.stream()
                        .collect(Collectors.toMap(
                                ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                                Function.identity(),
                                // It's possible there are duplicates given we were previously
                                // accidentally inserting duplicates of expired RIs. We pick
                                // the latest here and expect them to be cleaned up through
                                // garbage collection
                                (ri1, ri2) -> ri1.getId() > ri2.getId() ? ri1 : ri2));
        final List<ReservedInstanceBoughtInfo> reservedInstanceBoughtInfos =
                unsetNumberOfCouponsUsed(updatedNewReservedInstances);
        final Map<String, ReservedInstanceBoughtInfo> newRIsProbeKeysToRI = reservedInstanceBoughtInfos.stream()
                .collect(Collectors.toMap(ReservedInstanceBoughtInfo::getProbeReservedInstanceId,
                        Function.identity()));
        final List<ReservedInstanceBoughtInfo> reservedInstanceToAdd =
                Sets.difference(newRIsProbeKeysToRI.keySet(),
                        existingRIsProbeKeyToRecord.keySet()).stream()
                        .map(newRIsProbeKeysToRI::get)
                        .collect(toList());
        final Map<String, ReservedInstanceBoughtInfo> reservedInstanceUpdates =
                Sets.intersection(newRIsProbeKeysToRI.keySet(), existingRIsProbeKeyToRecord.keySet()).stream()
                        .collect(Collectors.toMap(
                                key -> existingRIsProbeKeyToRecord.get(key).getProbeReservedInstanceId(),
                                newRIsProbeKeysToRI::get));
        final Set<ReservedInstanceBoughtRecord> reservedInstanceToRemove =
                Sets.difference(existingRIsProbeKeyToRecord.keySet(), newRIsProbeKeysToRI.keySet()).stream()
                        .map(existingRIsProbeKeyToRecord::get)
                        .collect(Collectors.toSet());

        final Map<Long, Integer> riSpecToTermInYearMap =
            getReservedInstanceCostCalculator().getRiSpecIdToTermInYearMap(updatedNewReservedInstances, context);

        final Map<String, Double> probeRIIDToAmortizedCost =
            getReservedInstanceCostCalculator().calculateReservedInstanceAmortizedCost(updatedNewReservedInstances,
                        riSpecToTermInYearMap);

        internalInsert(context, reservedInstanceToAdd, probeRIIDToAmortizedCost);
        internalUpdate(context, reservedInstanceUpdates, existingRIsProbeKeyToRecord, probeRIIDToAmortizedCost);
        internalDelete(context, reservedInstanceToRemove);

        // Means the RI Buy inventory has changed.
        if (reservedInstanceToAdd.size() > 0 || reservedInstanceToRemove.size() > 0) {
            updateCallbacks.forEach(Runnable::run);
        }
        getLogger().info("Finished updating reserved instance bought.");
    }

    @Override
    public void updateRIBoughtFromRIPriceList(
            @Nonnull Map<Long, PricingDTO.ReservedInstancePrice> reservedInstanceSpecPrices) {
        try {
            getDsl().transaction(configuration -> {
                DSLContext context = DSL.using(configuration);
                final List<ReservedInstanceBoughtRecord> existingReservedInstances =
                        internalGet(context, ReservedInstanceBoughtFilter.newBuilder()
                                .build());
                checkRIBoughtAndUpdatePrices(context, existingReservedInstances, reservedInstanceSpecPrices);
            });
        } catch (DataAccessException e) {
            getLogger().error("Error while trying to update RIBoughtSpec.", e);
        }
    }

    @Override
    public void onInventoryChange(@Nonnull final Runnable callback) {
        updateCallbacks.add(callback);
    }

    @Override
    public List<ReservedInstanceBought> getReservedInstanceBoughtForAnalysis(
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        return adjustAvailableCouponsForPartialCloudEnv(getReservedInstanceBoughtByFilter(filter));
    }

    private List<ReservedInstanceBoughtInfo> internalCheckRIBoughtAndUpdatePrices(
            @Nonnull List<ReservedInstanceBoughtInfo> riBoughtInfos) {
        Map<String, ReservedInstanceBoughtInfo> retVal = riBoughtInfos.stream().collect(
                Collectors.toMap(ReservedInstanceBoughtInfo::getProbeReservedInstanceId,
                Function.identity()));
        List<ReservedInstanceBoughtInfo> noCostReservedInstanceBoughtInfos = getRIInfoWithZeroCosts(riBoughtInfos);
        if (!noCostReservedInstanceBoughtInfos.isEmpty()) {
            ReservedInstancePriceTable reservedInstancePriceTable = priceTableStore.getMergedRiPriceTable();
            Map<Long, ReservedInstancePrice> reservedInstancePriceMap =
                    ImmutableMap.copyOf(reservedInstancePriceTable.getRiPricesBySpecIdMap());
            noCostReservedInstanceBoughtInfos.forEach(riBoughtInfo -> {
                ReservedInstanceBoughtInfo reservedInstanceBoughtInfo = updateRIBoughtInfoFromRIPriceTable(
                        reservedInstancePriceMap, riBoughtInfo);
                retVal.put(reservedInstanceBoughtInfo.getProbeReservedInstanceId(), reservedInstanceBoughtInfo);
            });
        }
        return new ArrayList<>(retVal.values());
    }

    private void checkRIBoughtAndUpdatePrices(
            @Nonnull final DSLContext context,
            @Nonnull final List<ReservedInstanceBoughtRecord> existingReservedInstances,
            @Nonnull Map<Long, PricingDTO.ReservedInstancePrice> reservedInstanceSpecPrices ) {
            final Map<String, ReservedInstanceBoughtRecord> existingRIsProbeKeyToRecord =
                existingReservedInstances.stream()
                        .collect(Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                                Function.identity()));
            //create map indexed by probeRIId to ReservedInstanceBoughtInfo.
            final Map<String, ReservedInstanceBoughtInfo> existingRIBoughtInfo = existingReservedInstances.stream()
                    .collect(Collectors.toMap(ReservedInstanceBoughtRecord::getProbeReservedInstanceId,
                            existingReservedInstance -> updateRIBoughtInfoFromRIPriceTable(reservedInstanceSpecPrices,
                                    existingReservedInstance.getReservedInstanceBoughtInfo())));
            final Map<Long, Integer> riSpecToTermInYearMap =
                    getReservedInstanceCostCalculator().getRiSpecIdToTermInYearMap(
                            new ArrayList<>(existingRIBoughtInfo.values()), context);
            //create map indexed by probeRIId and calculate AmortizedCost.
            final Map<String, Double> probeRIIDToAmortizedCost =
                    getReservedInstanceCostCalculator().calculateReservedInstanceAmortizedCost(
                            new ArrayList<>(existingRIBoughtInfo.values()), riSpecToTermInYearMap);
            //update DB.
            internalUpdate(context, existingRIBoughtInfo, existingRIsProbeKeyToRecord, probeRIIDToAmortizedCost);
    }

    private List<ReservedInstanceBoughtInfo> getRIInfoWithZeroCosts(
            @Nonnull final List<ReservedInstanceBoughtInfo> existingReservedInstances) {
        return existingReservedInstances.stream().filter(this::isReservedInstanceBoughtInfoCostZero).collect(toList());
    }

    private boolean isReservedInstanceBoughtInfoCostZero(final ReservedInstanceBoughtInfo existingReservedInstance) {
        return (existingReservedInstance.getReservedInstanceBoughtCost().getFixedCost().getAmount() == 0.0
                && existingReservedInstance.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount() == 0.0);
    }

    private ReservedInstanceBoughtInfo updateRIBoughtInfoFromRIPriceTable(
            @Nonnull final Map<Long, ReservedInstancePrice> reservedInstanceSpecPrices,
            @Nonnull final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo) {
        if (isReservedInstanceBoughtInfoCostZero(reservedInstanceBoughtInfo)) {
            final PricingDTO.ReservedInstancePrice currentReservedInstancePrice =
                    reservedInstanceSpecPrices.get(reservedInstanceBoughtInfo.getReservedInstanceSpec());
            if (currentReservedInstancePrice == null) {
                getLogger().warn("Was unable to lookup RISpec {} in RIPricetable.",
                        reservedInstanceBoughtInfo.getReservedInstanceSpec());
                return reservedInstanceBoughtInfo;
            }
            final ReservedInstanceBoughtCost updatedReservedInstancePrice =
                    reservedInstanceBoughtInfo.getReservedInstanceBoughtCost().toBuilder().setRecurringCostPerHour(
                            currentReservedInstancePrice.getRecurringPrice().getPriceAmount())
                            .setFixedCost(currentReservedInstancePrice.getUpfrontPrice().getPriceAmount()).build();
            return reservedInstanceBoughtInfo.toBuilder().setReservedInstanceBoughtCost(updatedReservedInstancePrice).build();
        }
        return reservedInstanceBoughtInfo;
    }

    @Nonnull
    private static List<ReservedInstanceBoughtInfo> unsetNumberOfCouponsUsed(@Nonnull List<ReservedInstanceBoughtInfo> newReservedInstances) {
        return newReservedInstances.stream().map(ReservedInstanceBoughtInfo::toBuilder)
                        .peek(riInfoBuilder -> riInfoBuilder.getReservedInstanceBoughtCouponsBuilder()
                                        .setNumberOfCouponsUsed(0D))
                        .map(ReservedInstanceBoughtInfo.Builder::build)
                        .collect(toList());
    }

    /**
     * Insert a list of new {@link ReservedInstanceBoughtInfo} into table.
     *
     * @param context {@link DSLContext} transactional context.
     * @param reservedInstancesToAdd a list of new {@link ReservedInstanceBoughtInfo}.
     * @param probeRIIDToAmortizedCost a map of probeReservedInstanceID -> amortizedCost used to update the amortized cost.
     */
    private void internalInsert(@Nonnull final DSLContext context,
                                @Nonnull final List<ReservedInstanceBoughtInfo> reservedInstancesToAdd,
                                @Nonnull final Map<String, Double> probeRIIDToAmortizedCost) {
        final List<ReservedInstanceBoughtRecord> reservedInstancesRecordToAdd = reservedInstancesToAdd.stream()
                .map(ri -> createNewReservedInstanceRecord(
                        context,
                        getIdentityProvider().next(),
                        ri,
                        probeRIIDToAmortizedCost.get(ri.getProbeReservedInstanceId()), null
                    ))
                .filter(Objects::nonNull)
                .collect(toList());
        context.batchInsert(reservedInstancesRecordToAdd).execute();
    }

    /**
     * Update existing {@link ReservedInstanceBoughtRecord} based on latest {@link ReservedInstanceBoughtInfo} data.
     *
     * @param context {@link DSLContext} transactional context.
     * @param reservedInstanceInfoMap A Map which key is "probeReservedInstanceId", value is
     *                                {@link ReservedInstanceBoughtInfo}.
     * @param reservedInstanceRecordMap A Map which key is "ReservedInstanceInfo", value is
     *                                  {@link ReservedInstanceBoughtRecord}.
     * @param probeRIIDToAmortizedCost a map of probeReservedInstanceID -> amortizedCost used to update the amortized cost.
     */
    private void internalUpdate(@Nonnull final DSLContext context,
                                @Nonnull final Map<String, ReservedInstanceBoughtInfo> reservedInstanceInfoMap,
                                @Nonnull final Map<String, ReservedInstanceBoughtRecord> reservedInstanceRecordMap,
                                @Nonnull final Map<String, Double> probeRIIDToAmortizedCost) {
        final List<ReservedInstanceBoughtRecord> reservedInstanceRecordsUpdates =
                reservedInstanceInfoMap.entrySet()
                        .stream()
                        .map(riInfoByProbeId -> {

                            final String probeId = riInfoByProbeId.getKey();
                            final ReservedInstanceBoughtRecord record =  reservedInstanceRecordMap.get(probeId);

                            return createNewReservedInstanceRecord(context,
                                    record.getId(),
                                    riInfoByProbeId.getValue(),
                                    probeRIIDToAmortizedCost.get(probeId),
                                    record.getDiscoveryTime());

                        }).collect(toList());
        context.batchUpdate(reservedInstanceRecordsUpdates).execute();
    }

    /**
     * Delete a list of {@link ReservedInstanceBoughtRecord} from table.
     *
     * @param context DSL context.
     * @param reservedInstanceToRemove set of ReservedInstanceBoughtRecord to be removed.
     */
    private static void internalDelete(@Nonnull final DSLContext context,
                                @Nonnull final Set<ReservedInstanceBoughtRecord> reservedInstanceToRemove) {
        context.batchDelete(reservedInstanceToRemove).execute();
    }

    /**
     * Get a list of {@link ReservedInstanceBoughtRecord} filter by the input {@link ReservedInstanceBoughtFilter}.
     *
     * @param context {@link DSLContext} transactional context.
     * @param filter {@link ReservedInstanceBoughtFilter} contains all filter condition.
     * @return a list of matched {@link ReservedInstanceBoughtRecord}.
     */
    private static List<ReservedInstanceBoughtRecord> internalGet(
            @Nonnull final DSLContext context,
            @Nonnull final ReservedInstanceBoughtFilter filter) {
        if (filter.isJoinWithSpecTable()) {
            return context.select(RESERVED_INSTANCE_BOUGHT.fields())
                    .from(RESERVED_INSTANCE_BOUGHT)
                    .join(RESERVED_INSTANCE_SPEC)
                    .on(RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
                    .where(filter.generateConditions(context))
                    .fetch()
                    .into(RESERVED_INSTANCE_BOUGHT);
        } else {
            return context.selectFrom(RESERVED_INSTANCE_BOUGHT)
                    .where(filter.generateConditions(context))
                    .fetch();
        }
    }

    private static ReservedInstanceBoughtInfo addDerivedCostRIBoughtInfo(
            @Nonnull ReservedInstanceBoughtInfo riBoughtInfo,
            @Nullable Double amortizedCost) {

        return riBoughtInfo.toBuilder()
                .setReservedInstanceDerivedCost(
                        ReservedInstanceDerivedCost.newBuilder()
                                .setAmortizedCostPerHour(
                                        CurrencyAmount.newBuilder()
                                                .setAmount(amortizedCost)
                                                .build())
                                .build())
                .build();
    }

    /**
     * Create a new {@link ReservedInstanceBoughtRecord} based on input {@link ReservedInstanceBoughtInfo}.
     *
     * @param context context for DSL.
     * @param reservedInstanceId The ID to use for the newly created record.
     * @param reservedInstanceInfo bought information on an RI.
     * @param amortizedCost amortized cost computed as (fixedCost / Term * 730 * 12) + recurringCost.
     * @param existingDiscoveryTime the initial time stamp the RI was discovered by turbo.
     * @return a record for the ReservedInstance that was bought
     */
    @Nullable
    private ReservedInstanceBoughtRecord createNewReservedInstanceRecord(
            @Nonnull final DSLContext context,
            long reservedInstanceId,
            @Nonnull final ReservedInstanceBoughtInfo reservedInstanceInfo,
            @Nullable final Double amortizedCost,
            @Nullable final LocalDateTime existingDiscoveryTime) {
        if (amortizedCost == null) {
            getLogger().debug("Unable to get amortized cost for RI with probeReservedInstanceID {}. Amortized cost will default to 0.",
                            reservedInstanceInfo.getProbeReservedInstanceId());
        }

        if (reservedInstanceInfo.getEndTime() == 0) {
            getLogger().error("The expiry time for RI with probeReservedInstanceID {} is not available"
                    + ". This RI will be ignored.", reservedInstanceInfo.getProbeReservedInstanceId());
            return null;
        }

        LocalDateTime startTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(reservedInstanceInfo.getStartTime()),
            ZoneId.from(ZoneOffset.UTC));
        LocalDateTime expiryTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(reservedInstanceInfo.getEndTime()),
                ZoneId.from(ZoneOffset.UTC));
        LocalDateTime discoveryTime = (existingDiscoveryTime != null) ? existingDiscoveryTime
                :   LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()),
                ZoneId.from(ZoneOffset.UTC));

        return context.newRecord(RESERVED_INSTANCE_BOUGHT, new ReservedInstanceBoughtRecord(
                reservedInstanceId,
                reservedInstanceInfo.getBusinessAccountId(),
                reservedInstanceInfo.getProbeReservedInstanceId(),
                reservedInstanceInfo.getReservedInstanceSpec(),
                reservedInstanceInfo.getAvailabilityZoneId(),
                addDerivedCostRIBoughtInfo(reservedInstanceInfo, amortizedCost),
                reservedInstanceInfo.getNumBought(),
                reservedInstanceInfo.getReservedInstanceBoughtCost().getFixedCost().getAmount(),
                reservedInstanceInfo.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount(),
                (amortizedCost == null ? 0D : amortizedCost.doubleValue()),
                startTime, expiryTime,
                discoveryTime));
    }

    /**
     * Convert {@link ReservedInstanceBoughtRecord} to {@link ReservedInstanceBought}.
     *
     * @param reservedInstanceRecord {@link ReservedInstanceBoughtRecord}.
     * @return a {@link ReservedInstanceBought}.
     */
    private ReservedInstanceBought reservedInstancesToProto(
            @Nonnull final ReservedInstanceBoughtRecord reservedInstanceRecord) {
        return ReservedInstanceBought.newBuilder()
                .setId(reservedInstanceRecord.getId())
                .setReservedInstanceBoughtInfo(reservedInstanceRecord.getReservedInstanceBoughtInfo())
                .build();
    }

    @Nonnull
    @Override
    public Map<Long, Double> getNumberOfUsedCouponsForReservedInstances(
            @Nonnull final Collection<Long> filterByReservedInstanceIds) {
        return getNumberOfUsedCouponsForReservedInstances(getDsl(), filterByReservedInstanceIds);
    }

    @Nonnull
    @Override
    public Map<Long, Double> getNumberOfUsedCouponsForReservedInstances(
            @Nonnull final DSLContext context,
            @Nonnull final Collection<Long> filterByReservedInstanceIds) {
        return context.select(RESERVED_INSTANCE_ID_FIELD, sum(USED_COUPONS_FIELD)).from(DSL.select(
                Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID,
                Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.USED_COUPONS)
                .from(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                .unionAll(
                        DSL.select(Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID,
                                Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING.USED_COUPONS)
                                .from(Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING))).where(
                filterByReservedInstanceIds.isEmpty() ? DSL.trueCondition()
                        : RESERVED_INSTANCE_ID_FIELD.in(filterByReservedInstanceIds)).groupBy(
                RESERVED_INSTANCE_ID_FIELD).fetchStream().collect(
                Collectors.toMap(r -> r.getValue(0, Long.class), r -> r.getValue(1, Double.class)));
    }

    @Nonnull
    @Override
    public List<ReservedInstanceBought> getUndiscoveredReservedInstances() {
        final SelectWhereStep<ReservedInstanceBoughtRecord> records
                = getDsl().selectFrom(RESERVED_INSTANCE_BOUGHT);
        final Set<Long> discoveredAccounts = businessAccountHelper.getDiscoveredBusinessAccounts();
        return records.stream()
                .filter(record -> !discoveredAccounts.contains(record.getBusinessAccountId()))
                .map(this::reservedInstancesToProto)
                .collect(toList());
    }


    @Nonnull
    @Override
    public List<ReservedInstanceBought> getUndiscoveredUnusedReservedInstancesInScope(
            final ReservedInstanceBoughtFilter filter) {
        List<ReservedInstanceBought> undiscoveredRIs = getUndiscoveredReservedInstances();
        Optional<Condition> usedCondition = filter.generateUsedByDiscoveredAccountsCondition(getDSLContext());
        final List<Long> usedRIs;
        if (usedCondition.isPresent()) {
            usedRIs = getDsl()
                    .selectFrom(RESERVED_INSTANCE_BOUGHT)
                    .where(usedCondition.get())
                    .fetchStream()
                    .map(r -> r.getId())
                    .collect(Collectors.toList());
        } else {
            usedRIs = Collections.emptyList();
        }
        return undiscoveredRIs.stream()
                .filter(ri -> !usedRIs.contains(ri.getId()))
                .collect(toList());
    }

    @Nullable
    @Override
    public Long getCreationTime(final long riOid) throws DbException {
        Record1<LocalDateTime> discoveryTime =
                getDSLContext().select(RESERVED_INSTANCE_BOUGHT.DISCOVERY_TIME)
                .from(RESERVED_INSTANCE_BOUGHT)
                .where(RESERVED_INSTANCE_BOUGHT.ID.eq(riOid))
                .fetchOne();
        if (discoveryTime == null) {
            return null;
        }
        return discoveryTime.value1().toEpochSecond(ZoneOffset.UTC);
    }
}
