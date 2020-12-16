package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanReservedInstanceStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanReservedInstanceStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UpdatePlanBuyReservedInstanceCostsRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdatePlanBuyReservedInstanceCostsResponse;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceImplBase;
import com.vmturbo.cost.component.entity.cost.PlanProjectedEntityCostStore;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;
import com.vmturbo.cost.component.reserved.instance.filter.PlanProjectedEntityReservedInstanceMappingFilter;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * Plan reserved instance service.
 */
public class PlanReservedInstanceRpcService extends PlanReservedInstanceServiceImplBase {
    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    private final Logger logger = LogManager.getLogger();

    private final PlanReservedInstanceStore planReservedInstanceStore;
    private final BuyReservedInstanceStore buyReservedInstanceStore;
    private final ReservedInstanceSpecStore reservedInstanceSpecStore;
    private final PlanProjectedEntityCostStore planProjectedEntityCostStore;
    private final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore;

    /**
     * Creates {@link PlanReservedInstanceRpcService} instance.
     *
     * @param planReservedInstanceStore plan RI store.
     * @param buyReservedInstanceStore buy RI Store.
     * @param reservedInstanceSpecStore Store for RI specification (meta-data) info.
     * @param planProjectedEntityCostStore Store for projected entity costs.
     * @param planProjectedRICoverageAndUtilStore Store for RI coverage.
     */
    public PlanReservedInstanceRpcService(
            @Nonnull final PlanReservedInstanceStore planReservedInstanceStore,
            @Nonnull final BuyReservedInstanceStore buyReservedInstanceStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull final PlanProjectedEntityCostStore planProjectedEntityCostStore,
            @Nonnull final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore) {
        this.planReservedInstanceStore = Objects.requireNonNull(planReservedInstanceStore);
        this.buyReservedInstanceStore = Objects.requireNonNull(buyReservedInstanceStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.planProjectedEntityCostStore = Objects.requireNonNull(planProjectedEntityCostStore);
        this.planProjectedRICoverageAndUtilStore = Objects.requireNonNull(planProjectedRICoverageAndUtilStore);
    }

    @Override
    public void getPlanReservedInstanceBoughtCountByTemplateType(GetPlanReservedInstanceBoughtCountRequest request,
        StreamObserver<GetPlanReservedInstanceBoughtCountByTemplateResponse> responseObserver) {
        try {
            final long planId = request.getPlanId();
            final Map<Long, Long> riCountByRiSpecId = planReservedInstanceStore
                            .getPlanReservedInstanceCountByRISpecIdMap(planId);

            final Map<Long, ReservedInstanceSpec> riSpecBySpecId = reservedInstanceSpecStore
                    .getReservedInstanceSpecByIds(riCountByRiSpecId.keySet())
                    .stream().collect(Collectors.toMap(ReservedInstanceSpec::getId,
                            Function.identity()));

            final Map<Long, Long> riBoughtCountByTierId =
                    riCountByRiSpecId.entrySet().stream()
                            .filter(e -> riSpecBySpecId.containsKey(e.getKey()))
                            .collect(Collectors.toMap(e -> riSpecBySpecId.get(e.getKey())
                                            .getReservedInstanceSpecInfo().getTierId(),
                                    Entry::getValue, Long::sum));

            final GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                            GetPlanReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                                            .putAllReservedInstanceCountMap(riBoughtCountByTierId)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to get plan reserved instance count map.")
                            .asException());
        }
    }

    @Override
    public void getPlanReservedInstanceBought(GetPlanReservedInstanceBoughtRequest request,
        StreamObserver<GetReservedInstanceBoughtByFilterResponse> responseObserver) {
        try {
            final Long topologyContextId = request.getPlanId();
            final List<ReservedInstanceBought> reservedInstances =
                            planReservedInstanceStore.getReservedInstanceBoughtByPlanId(topologyContextId);
            final GetReservedInstanceBoughtByFilterResponse response =
                            GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                            .addAllReservedInstanceBoughts(reservedInstances)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to get plan reserved instances.")
                            .asException());
        }
    }

    @Override
    public void deletePlanReservedInstanceStats(DeletePlanReservedInstanceStatsRequest request,
        StreamObserver<DeletePlanReservedInstanceStatsResponse> responseObserver) {
        try {
            final Long topologyContextId = request.getTopologyContextId();
            final int rowsDeleted = planReservedInstanceStore.deletePlanReservedInstanceStats(topologyContextId);
            final DeletePlanReservedInstanceStatsResponse response =
                            DeletePlanReservedInstanceStatsResponse.newBuilder()
                                            .setDeleted(rowsDeleted > 0 ? true : false)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to delete plan reserved instance stats.")
                            .asException());
        }
    }

    /**
     * Insert plan reserved instances bought (Included RIs).
     *
     * @param request  The request.
     * @param responseObserver The response observer.
     */
    @Override
    public void insertPlanReservedInstanceBought(UploadRIDataRequest request,
        StreamObserver<UploadRIDataResponse> responseObserver) {
        try {
            final Long planId = request.getTopologyContextId();
            planReservedInstanceStore.insertPlanReservedInstanceBought(request.getReservedInstanceBoughtList(),
                                                                       planId);
            responseObserver.onNext(UploadRIDataResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to insert plan bought reserved instance(s).")
                            .asException());
        }
    }

    @Override
    public void getPlanReservedInstanceCostStats(GetPlanReservedInstanceCostStatsRequest request,
        StreamObserver<GetPlanReservedInstanceCostStatsResponse> responseObserver) {
        try {
            final long planId = request.getPlanId();
            final Instant instant = Clock.systemUTC().instant();
            final long currentTime = instant.toEpochMilli();
            final long projectedTime = instant.plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
            final Cost.ReservedInstanceCostStat riCostStats = planReservedInstanceStore.getPlanReservedInstanceAggregatedCosts(planId);
            // if current data has already been queried, use that as part of projection data.
            final List<Cost.ReservedInstanceCostStat> projectedRICostBuilders;
            //check if we should include buy RI data
            if (request.getIncludeBuyRi()) {
                final BuyReservedInstanceCostFilter buyReservedInstanceCostFilter =
                                BuyReservedInstanceCostFilter.newBuilder()
                                                .addTopologyContextId(planId)
                                                .addGroupBy(GroupBy.SNAPSHOT_TIME).build();
                final List<Cost.ReservedInstanceCostStat> buyRICostStats =
                                buyReservedInstanceStore.queryBuyReservedInstanceCostStats(buyReservedInstanceCostFilter);
                projectedRICostBuilders = unifyProjectedRICostStats(riCostStats, buyRICostStats);
            } else {
                projectedRICostBuilders = Collections.singletonList(riCostStats);
            }

            final List<Cost.ReservedInstanceCostStat> currentReservedInstanceCostStats =
                            updateSnapshotTime(Collections.singletonList(riCostStats), currentTime);
            final List<Cost.ReservedInstanceCostStat> projectedReservedInstanceCostStats =
                            updateSnapshotTime(projectedRICostBuilders, projectedTime);
            final GetPlanReservedInstanceCostStatsResponse response =
                            GetPlanReservedInstanceCostStatsResponse.newBuilder()
                            .addAllStats(currentReservedInstanceCostStats)
                            .addAllStats(projectedReservedInstanceCostStats)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to get plan reserved instances cost stats.")
                            .asException());
        }
    }

    @Nonnull
    private static List<Cost.ReservedInstanceCostStat> unifyProjectedRICostStats(@Nonnull Cost.ReservedInstanceCostStat boughtRICostStats,
                    @Nonnull List<Cost.ReservedInstanceCostStat> buyRICostStats) {
        if (CollectionUtils.isEmpty(buyRICostStats)) {
            return Collections.singletonList(boughtRICostStats);
        }
        final Cost.ReservedInstanceCostStat boughtRICostStat = boughtRICostStats;
        final Cost.ReservedInstanceCostStat buyRICostStat = buyRICostStats.get(0);
        final Cost.ReservedInstanceCostStat projectedRICostStat =
                        Cost.ReservedInstanceCostStat.newBuilder()
                                        .setAmortizedCost(boughtRICostStat.getAmortizedCost()
                                            + buyRICostStat.getAmortizedCost())
                                        .setFixedCost(boughtRICostStat.getFixedCost()
                                            + buyRICostStat.getFixedCost())
                                        .setRecurringCost(boughtRICostStat.getRecurringCost()
                                            + buyRICostStat.getRecurringCost())
                                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
        return Collections.singletonList(projectedRICostStat);
    }

    @Nonnull
    private static List<Cost.ReservedInstanceCostStat> updateSnapshotTime(
        @Nonnull List<Cost.ReservedInstanceCostStat> reservedInstanceCostStats, long snapshotTime) {
        final List<Cost.ReservedInstanceCostStat> riCostStats = reservedInstanceCostStats.stream()
                        .map(Cost.ReservedInstanceCostStat::toBuilder)
                        .peek(riCostStatBuilder -> riCostStatBuilder.setSnapshotTime(snapshotTime))
                        .map(Cost.ReservedInstanceCostStat.Builder::build)
                        .collect(Collectors.toList());
        return riCostStats;
    }

    /**
     * Updates some BuyRI costs in DB tables. Called for MPC plan from PlanRpcService after the
     * plan completes. Plan projected entity costs and used coupon coverage is updated with BuyRI
     * discount info available.
     *
     * @param request Request containing plan id for which updates are needed.
     * @param responseObserver Response containing plan id and number of cost records updated.
     */
    @Override
    public void updatePlanBuyReservedInstanceCosts(
            @Nonnull final UpdatePlanBuyReservedInstanceCostsRequest request,
            @Nonnull final StreamObserver<UpdatePlanBuyReservedInstanceCostsResponse> responseObserver) {
        long planId = request.getPlanId();

        final UpdatePlanBuyReservedInstanceCostsResponse.Builder responseBuilder =
                UpdatePlanBuyReservedInstanceCostsResponse.newBuilder()
                .setPlanId(planId)
                .setUpdateCount(0);

        // Get all entity ids, used coupons and total coupons for this plan.
        final Map<Long, EntityReservedInstanceCoverage> coverageMap =
                planProjectedRICoverageAndUtilStore.getPlanProjectedRiCoverage(planId,
                        PlanProjectedEntityReservedInstanceMappingFilter.newBuilder()
                                .topologyContextId(planId).build());
        logger.trace("Coverage map for plan {} is: {}", planId, coverageMap);
        if (coverageMap.isEmpty()) {
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        // Get existing projected entity costs for those VMs.
        final Map<Long, EntityCost> originalEntityCosts = planProjectedEntityCostStore
                .getPlanProjectedEntityCosts(coverageMap.keySet(), planId);
        if (originalEntityCosts.isEmpty()) {
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            return;
        }

        // Find out for which entities the used coupons and RI discounts need to be updated.
        final Map<Long, Double> usedCouponsToUpdate = new HashMap<>();
        final Map<Long, EntityCost> entityCostsToUpdate = new HashMap<>();
        fillUsedCouponsAndEntityCosts(originalEntityCosts, coverageMap, usedCouponsToUpdate,
                entityCostsToUpdate, request.getShouldRiDiscountLicenseCost());

        // Update used_coupons in plan_projected_reserved_instance_coverage, doesn't seem to
        // break anything not doing it for BuyRI, but do it for completeness.
        logger.trace("Updating BuyRI used coupons for plan {}: {}", planId, usedCouponsToUpdate);
        int updateCount = planProjectedRICoverageAndUtilStore.updatePlanProjectedRiCoverage(planId,
                usedCouponsToUpdate);

        // Update missing BuyRI discount in plan projected entity costs table.
        logger.debug("Updating BuyRI (coverage updated: {}) costs for {} entities in plan {}.",
                updateCount, entityCostsToUpdate.size(), planId);
        updateCount = planProjectedEntityCostStore.updatePlanProjectedEntityCosts(
                planId, entityCostsToUpdate);
        if (updateCount < entityCostsToUpdate.size()) {
            logger.warn("Could only update {} BuyRI costs out of {}, for plan {}.",
                    updateCount, entityCostsToUpdate.size(), planId);
        } else {
            logger.debug("Successfully updated {} BuyRI costs for plan {}.",
                    updateCount, planId);
        }
        responseObserver.onNext(responseBuilder.setUpdateCount(updateCount).build());
        responseObserver.onCompleted();
    }

    /**
     * Fills up the usedCouponsToUpdate and entityCostsToUpdate maps with values, related to
     * used coupon value and BuyRI discounts in plan projected entity costs table.
     *
     * @param originalEntityCosts Original projected entity costs fetched for the plan.
     * @param coverageMap Info about RI coverage for entities for this plan.
     * @param usedCouponsToUpdate Map of entityId to usedCoupons covered, for update in
     *      plan_projected ri coverage table. Input map updated in place.
     * @param entityCostsToUpdate Map of entityId to costs with RI discount, updated in
     *      plan projected entity cost table. Input map updated in place.
     * @param shouldRiDiscountLicenseCost Whether license costs should be discounted based on RI coverage
     */
    private void fillUsedCouponsAndEntityCosts(
            @Nonnull final Map<Long, EntityCost> originalEntityCosts,
            @Nonnull final Map<Long, EntityReservedInstanceCoverage> coverageMap,
            @Nonnull final Map<Long, Double> usedCouponsToUpdate,
            @Nonnull final Map<Long, EntityCost> entityCostsToUpdate,
            @Nonnull final boolean shouldRiDiscountLicenseCost) {
        // Find those entities for which we have BuyRIs with missing ri_inventory_discounts.
        for (Map.Entry<Long, EntityCost> entry : originalEntityCosts.entrySet()) {
            final long entityId = entry.getKey();
            final EntityCost entityCost = entry.getValue();
            Double computeRate = null;
            Double licenseRate = null;
            boolean hasRiDiscount = false;
            for (ComponentCost cc : entityCost.getComponentCostList()) {
                if (!cc.hasCategory() || !cc.hasCostSource()) {
                    continue;
                }
                if (cc.getCostSource() == CostSource.RI_INVENTORY_DISCOUNT) {
                    hasRiDiscount = true;
                }
                if (cc.getCategory() == CostCategory.ON_DEMAND_COMPUTE
                        && cc.getCostSource() == CostSource.ON_DEMAND_RATE) {
                    computeRate = cc.getAmount().getAmount();
                } else if (cc.getCategory() == CostCategory.ON_DEMAND_LICENSE
                        && cc.getCostSource() == CostSource.ON_DEMAND_RATE) {
                    licenseRate = cc.getAmount().getAmount();
                }
            }
            // Skip if RI discount is already there or no new rates (compute/license) to update.
            if (hasRiDiscount || (computeRate == null && licenseRate == null)) {
                continue;
            }
            final EntityReservedInstanceCoverage riCoverage = coverageMap.get(entityId);
            int totalCoupons = riCoverage.getEntityCouponCapacity();
            double usedCoupons = planProjectedRICoverageAndUtilStore.getAggregatedEntityRICoverage(
                    Collections.singletonList(riCoverage)).values().iterator().next();
            if (totalCoupons == 0 && usedCoupons > 0d) {
                // In case totalCoupons is 0, but we have a valid usedCoupon count, set total.
                totalCoupons = (int)usedCoupons;
            }
            usedCouponsToUpdate.put(entityId, usedCoupons);
            final EntityCost.Builder newCostBuilder = EntityCost.newBuilder(entityCost);
            if (computeRate != null) {
                updateRiDiscountedCost(CostCategory.ON_DEMAND_COMPUTE, totalCoupons, usedCoupons,
                        newCostBuilder, computeRate);
            }
            if (licenseRate != null && shouldRiDiscountLicenseCost) {
                updateRiDiscountedCost(CostCategory.ON_DEMAND_LICENSE, totalCoupons, usedCoupons,
                        newCostBuilder, licenseRate);
            }
            entityCostsToUpdate.put(entityId, newCostBuilder.build());
        }
    }

    /**
     * Updates entity cost after taking RI discount into account, to be updated back
     * to the entity costs table.
     *
     * @param costCategory Type of cost (compute or license).
     * @param totalCoupons Total coupon capacity.
     * @param usedCoupons Number of used coupons.
     * @param costBuilder Cost builder to update.
     * @param onDemandRate Compute or license rate on which discount needs to be applied.
     */
    private void updateRiDiscountedCost(CostCategory costCategory, int totalCoupons,
            double usedCoupons, @Nonnull final EntityCost.Builder costBuilder, double onDemandRate) {
        double riDiscount = totalCoupons == 0 ? 0d
                : -1 * onDemandRate * (usedCoupons / totalCoupons);
        costBuilder.addComponentCost(ComponentCost.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setCurrency(costBuilder.getTotalAmount().getCurrency())
                        .setAmount(riDiscount).build())
                .setCategory(costCategory)
                .setCostSource(CostSource.RI_INVENTORY_DISCOUNT)
                .build())
                // Because we added a discount (negative cost), we need to reduce the total by that
                // amount, so the negative discount is being added to the total below.
                .setTotalAmount(CurrencyAmount.newBuilder()
                        .setCurrency(costBuilder.getTotalAmount().getCurrency())
                        .setAmount(costBuilder.getTotalAmount().getAmount() + riDiscount)
                        .build())
                .build();
    }
}
