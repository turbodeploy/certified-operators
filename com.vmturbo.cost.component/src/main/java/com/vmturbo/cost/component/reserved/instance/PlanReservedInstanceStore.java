package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.TableRecord;
import org.jooq.impl.TableImpl;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cloud.CloudCommon;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.cost.component.TableDiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.PlanReservedInstanceBought;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;

/**
 * This class is used to update plan RI table by plan reserved instance bought data
 * from Topology Processor.
 */
public class PlanReservedInstanceStore extends AbstractReservedInstanceStore implements
        TableDiagsRestorable<Object, PlanReservedInstanceBoughtRecord> {

    private static final String planReservedInstanceDumpFile = "planReservedInstance_dump";

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    /**
     * Creates {@link PlanReservedInstanceStore} instance.
     *  @param dsl DSL context.
     * @param identityProvider identity provider.
     * @param reservedInstanceCostCalculator RI cost calculator.
     * @param businessAccountHelper BusinessAccountHelper.
     * @param entityReservedInstanceMappingStore the entity to RI mapping store.
     * @param accountRIMappingStore undiscovered account to RI mapping store.
     */
    public PlanReservedInstanceStore(@Nonnull DSLContext dsl, @Nonnull IdentityProvider identityProvider,
                                     @Nonnull final ReservedInstanceCostCalculator reservedInstanceCostCalculator,
                                     final BusinessAccountHelper businessAccountHelper,
                                     final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
                                     final AccountRIMappingStore accountRIMappingStore,
                                     final ReservedInstanceBoughtStore reservedInstanceBoughtStore) {
        super(dsl, identityProvider, reservedInstanceCostCalculator,
                accountRIMappingStore, entityReservedInstanceMappingStore, businessAccountHelper);
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
    }

    /**
     * Get the ids of plans with information in the store.
     *
     * @return Set of plan IDs.
     */
    @Nonnull
    public Set<Long> getPlanIds() {
        return getDsl().selectDistinct(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID)
            .from(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
            .fetch().stream()
            .map(Record1::value1)
            .collect(Collectors.toSet());
    }

    /**
     * Inserts new records to the plan RI bought table.
     *
     * @param newReservedInstances list of reserved instance bought
     * @param planId plan id
     */
    public void insertPlanReservedInstanceBought(@Nonnull final List<ReservedInstanceBought> newReservedInstances, long planId) {
        final List<ReservedInstanceBoughtInfo> newReservedInstanceBoughtInfos = newReservedInstances.stream()
                        .map(ReservedInstanceBought::getReservedInstanceBoughtInfo).collect(Collectors.toList());
        final Set<TableRecord<?>> records = new HashSet<>();
        final DSLContext dsl = getDsl();
        final Map<String, Double> probeRIIDToAmortizedCost =
                        getReservedInstanceCostCalculator().calculateReservedInstanceAmortizedCost(newReservedInstanceBoughtInfos, dsl);

        for (ReservedInstanceBought reservedInstanceBought : newReservedInstances) {
            final Long id = reservedInstanceBought.getId();
            final ReservedInstanceBoughtInfo reservedInstanceInfo = reservedInstanceBought.getReservedInstanceBoughtInfo();
            records.add(dsl.newRecord(Tables.PLAN_RESERVED_INSTANCE_BOUGHT,
                    new PlanReservedInstanceBoughtRecord(
                            id,
                            planId,
                            reservedInstanceInfo.getReservedInstanceSpec(),
                            reservedInstanceInfo,
                            reservedInstanceInfo.getNumBought(),
                            reservedInstanceInfo.getReservedInstanceBoughtCost().getFixedCost().getAmount(),
                            reservedInstanceInfo.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount(),
                            probeRIIDToAmortizedCost.getOrDefault(reservedInstanceInfo.getProbeReservedInstanceId(), 0D))));
        }
        dsl.batchInsert(records).execute();
    }

    /**
     * Get the sum count of reserved instance bought by RI spec ID.
     *
     * @param planId plan ID.
     * @return a Map which key is reservedInstance spec ID (Long) and value is the sum count
     * of reserved instance bought which belong to this spec.
     */
    public Map<Long, Long> getPlanReservedInstanceCountByRISpecIdMap(final Long planId) {
        return getDsl().select(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID,
                sum(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.COUNT).cast(Long.class))
                .from(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID.eq(planId))
                .groupBy(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID)
                .fetch()
                .stream()
                .collect(Collectors.toMap(Record2::value1, Record2::value2));
    }

    /**
     * Delete RI stats from the DB for the specified plan.
     *
     * @param planId plan ID.
     * @return count of deleted rows.
     */
    public int deletePlanReservedInstanceStats(final Long planId) {
        getLogger().info("Deleting data from plan reserved instance bought for planId: {}.",
                planId);
        return getDsl().deleteFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID
                                        .eq(planId)).execute();

    }

    /**
     * Get the Plan reserved instance cost stats for each RI Type. The amortized, fixed and recurring costs are
     * multiplied by the number of counts of the RI Type to get the total costs for that RI Type.
     *
     * @param planId plan ID
     * @return List of type ReservedInstanceCostStat containing the cost stats.
     */
    public Cost.ReservedInstanceCostStat getPlanReservedInstanceAggregatedCosts(long planId) {
        final PlanReservedInstanceBought planRIBoughtTable = Tables.PLAN_RESERVED_INSTANCE_BOUGHT;
        final Result<Record3<BigDecimal, BigDecimal, BigDecimal>> riAggregatedCostResult =
                        getDsl().select(sum(planRIBoughtTable.PER_INSTANCE_AMORTIZED_COST_HOURLY.mul(planRIBoughtTable.COUNT))
                                                        .as(RI_AMORTIZED_SUM),
                                        sum(planRIBoughtTable.PER_INSTANCE_RECURRING_COST_HOURLY.mul(planRIBoughtTable.COUNT))
                                                        .as(RI_RECURRING_SUM),
                                        sum(planRIBoughtTable.PER_INSTANCE_FIXED_COST.mul(planRIBoughtTable.COUNT))
                                                        .as(RI_FIXED_SUM))
                                 .from(planRIBoughtTable)
                                 .join(RESERVED_INSTANCE_SPEC)
                                     .on(planRIBoughtTable.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
                                 .where(planRIBoughtTable.PLAN_ID.eq(planId))
                                 .fetch();

        return convertToRICostStat(riAggregatedCostResult);
    }

    /**
     * Returns reserved instance bought list for the specified plan.
     *
     * @param planId plan ID.
     * @return list of {@link ReservedInstanceBought}.
     */
    public List<ReservedInstanceBought> getReservedInstanceBoughtByPlanId(final long planId) {
        final List<PlanReservedInstanceBoughtRecord> records = getDsl().selectFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID.eq(planId))
                        .fetch();
        return records.stream().map(this::toReservedInstanceBoughtProto).collect(Collectors.toList());
    }

    /**
     * Returns reserved instance bought list for the specified plan.
     *
     * @param planId plan ID.
     * @param vmOidSet VMs in scope to calculate the current RI Utilization.
     * @return list of {@link ReservedInstanceBought}.
     */
    public List<ReservedInstanceBought> getReservedInstanceBoughtForAnalysis(final long planId, final Set<Long> vmOidSet) {

        final List<ReservedInstanceBought> reservedInstanceBoughtByPlanId = getReservedInstanceBoughtByPlanId(planId);

        if (reservedInstanceBoughtByPlanId.isEmpty()) {
            return Collections.emptyList();
        }

        //Get total RI Utilization
        Map<Long, Double> numberOfUsedCouponsForReservedInstances = reservedInstanceBoughtStore.getNumberOfUsedCouponsForReservedInstances(
                reservedInstanceBoughtByPlanId.stream()
                        .map(ReservedInstanceBought::getId)
                        .collect(Collectors.toSet()));

        //Get total Discovered RI Utilization for RIs in scope
        final Map<Long, Double> riToDiscoveredUsageMap = entityReservedInstanceMappingStore
                .getReservedInstanceUsedCouponsMapByFilter(
                        EntityReservedInstanceMappingFilter.newBuilder().riBoughtFilter(
                                Cost.ReservedInstanceBoughtFilter.newBuilder()
                                        .addAllRiBoughtId(reservedInstanceBoughtByPlanId.stream()
                                                .map(a -> a.getId()).collect(Collectors.toList())).build()).build());

        // For VM oids, retrieve their entity to reserved instance mapping.
        final Map<Long, Double> riUtilizationForVmsInScope = getRiUtilizationForVmsInScope(vmOidSet);

        //Decrease both RI capacity and Utilization to filter out RI coupons used by undiscovered accounts.
        final List<ReservedInstanceBought> riBoughtUndiscoveredAccountUsageFilteredOut = adjustAvailableCouponsForPartialCloudEnv(reservedInstanceBoughtByPlanId, numberOfUsedCouponsForReservedInstances);

        //Decrease both RI capacity and Utilization to filter out RI coupons used from accounts that are discovered but not in scope of the plan
        final List<ReservedInstanceBought> riBoughtOutOfScopeDiscoveredUsageFilteredOut =
                filterDiscoveredRIUtilizationNotInScope(riBoughtUndiscoveredAccountUsageFilteredOut, riUtilizationForVmsInScope,
                        riToDiscoveredUsageMap);
       return riBoughtOutOfScopeDiscoveredUsageFilteredOut;
    }

    private Map<Long, Double> getRiUtilizationForVmsInScope(Set<Long> vmOidSet) {
        final EntityReservedInstanceMappingFilter entityReservedInstanceMappingFilter = EntityReservedInstanceMappingFilter.newBuilder()
                .entityFilter(CloudCommon.EntityFilter.newBuilder().addAllEntityId(vmOidSet).build()).build();
        final Map<Long, Set<Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage>> riCoverageByEntity =
                entityReservedInstanceMappingStore.getRICoverageByEntity(entityReservedInstanceMappingFilter);
        final Map<Long, Double> riUtilizationForVmsInScope = new HashMap<>();
        for (Map.Entry<Long, Set<Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage>> entry : riCoverageByEntity.entrySet()) {
            final Set<Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage> coverageSet = entry.getValue();
            Iterator<Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage> iterator = coverageSet.iterator();
            while (iterator.hasNext()) {
                Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage coverage = iterator.next();
                long reservedInstanceId = coverage.getReservedInstanceId();
                double coveredCoupons = coverage.getCoveredCoupons();
                if (riUtilizationForVmsInScope.containsKey(reservedInstanceId)) {
                    double updatedCoverage = coveredCoupons + riUtilizationForVmsInScope.get(reservedInstanceId);
                    riUtilizationForVmsInScope.put(reservedInstanceId, updatedCoverage);
                } else {
                    riUtilizationForVmsInScope.put(reservedInstanceId, coveredCoupons);
                }
            }
        }
        return riUtilizationForVmsInScope;
    }

    private List<ReservedInstanceBought> filterDiscoveredRIUtilizationNotInScope(List<ReservedInstanceBought> reservedInstanceBoughtList,
                                                                                 Map<Long, Double> riUtilizationForVmsInScope,
                                                                                 Map<Long, Double> riToDiscoveredUsageMap) {
        return reservedInstanceBoughtList.stream().map(ReservedInstanceBought::toBuilder).peek(riBuilder -> {
            final ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons.Builder riCouponsBuilder =
                    riBuilder.getReservedInstanceBoughtInfoBuilder()
                            .getReservedInstanceBoughtCouponsBuilder();
            long riId = riBuilder.getId();
            double numberOfCouponsUsedInScope = riUtilizationForVmsInScope.getOrDefault(riId, 0D);
            double riUsageFromDiscoveredEntities = riToDiscoveredUsageMap.getOrDefault(riId, 0D);

            //RI Coupon Utilization outside of scope = Total Discovered RI Utilization - RI Utilization In scope.
            double riDiscoveredUtilizationOutsideScope = riUsageFromDiscoveredEntities - numberOfCouponsUsedInScope;

            double numberOfCouponsCapacity = riCouponsBuilder.getNumberOfCoupons() - riDiscoveredUtilizationOutsideScope;
            double numberOfCouponsUsed = riCouponsBuilder.getNumberOfCouponsUsed() - riDiscoveredUtilizationOutsideScope;
            if (numberOfCouponsCapacity < 0 || numberOfCouponsUsed < 0) {
                if (numberOfCouponsCapacity < 0) {
                    logger.warn("For RI {}, overall number of coupons available for discovered workloads {} is less than RI utilization by discovered workloads outside scope {}. Setting the used and capacity to 0. Should never hit this case",
                            riId, riCouponsBuilder.getNumberOfCoupons(), riDiscoveredUtilizationOutsideScope);
                } else {
                    logger.warn("For RI {}, overall RI utilization by discovered workloads {} is less than RI utilization by discovered workloads outside scope {}. Setting the used and capacity to 0. Should never hit this case",
                            riId, riCouponsBuilder.getNumberOfCouponsUsed(), riDiscoveredUtilizationOutsideScope);
                }
                numberOfCouponsCapacity = 0;
                numberOfCouponsUsed = 0;
            }
            riCouponsBuilder.setNumberOfCoupons(numberOfCouponsCapacity).setNumberOfCouponsUsed(numberOfCouponsUsed);
        }).map(ReservedInstanceBought.Builder::build).collect(Collectors.toList());
    }

    private ReservedInstanceBought toReservedInstanceBoughtProto(PlanReservedInstanceBoughtRecord record) {
        return ReservedInstanceBought.newBuilder()
                        .setId(record.getId())
                        .setReservedInstanceBoughtInfo(record.getReservedInstanceBoughtInfo())
                        .build();

    }

    @Override
    public DSLContext getDSLContext() {
        return getDsl();
    }

    @Override
    public TableImpl<PlanReservedInstanceBoughtRecord> getTable() {
        return Tables.PLAN_RESERVED_INSTANCE_BOUGHT;
    }

    @Nonnull
    @Override
    public String getFileName() {
        return planReservedInstanceDumpFile;
    }
}
