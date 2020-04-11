package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Sub-query responsible for getting reserved instance stats for the plan from the cost component.
 */
public class PlanRIStatsSubQuery extends AbstractRIStatsSubQuery {
    private final PlanReservedInstanceServiceBlockingStub planReservedInstanceService;
    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;
    private final BuyRiScopeHandler buyRiScopeHandler;

    /**
     * Creates {@link PlanRIStatsSubQuery} instance.
     *
     * @param repositoryApi repository API.
     * @param planReservedInstanceService plan RI service blocking stub.
     * @param riUtilizationCoverageService the RI utilization and coverage service
     * @param buyRiScopeHandler buy RI scope handler.
     */
    public PlanRIStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
            @Nonnull final PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
            @Nonnull final BuyRiScopeHandler buyRiScopeHandler) {
        super(repositoryApi, buyRiScopeHandler);
        this.planReservedInstanceService = Objects.requireNonNull(planReservedInstanceService);
        this.riUtilizationCoverageService = Objects.requireNonNull(riUtilizationCoverageService);
        this.buyRiScopeHandler = Objects.requireNonNull(buyRiScopeHandler);
    }

    @Override
    public boolean applicableInContext(final StatsQueryContext context) {
        return context.getInputScope().isPlan();
    }

    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(final Set<StatApiInputDTO> stats,
            final StatsQueryContext context) throws OperationFailedException {
        final List<StatSnapshotApiDTO> snapshots = new ArrayList<>();
        final Optional<PlanInstance> planInstanceOpt = context.getPlanInstance();
        if (!planInstanceOpt.isPresent()) {
            return Collections.emptyList();
        }
        final PlanInstance planInstance = planInstanceOpt.get();
        if (containsStat(StringConstants.NUM_RI, stats)) {
            snapshots.addAll(getNumRIStatsSnapshots(planInstance.getPlanId()));
        }
        if (containsStat(StringConstants.RI_COUPON_COVERAGE, stats)) {
            snapshots.addAll(getRICoverageSnapshots(context));
        }
        if (containsStat(StringConstants.RI_COUPON_UTILIZATION, stats)) {
            snapshots.addAll(getRIUtilizationSnapshots(context));
        }
        if (containsStat(StringConstants.RI_COST, stats)) {
            final boolean includeBuyRi = planInstance.getScenario()
                    .getScenarioInfo()
                    .getChangesList()
                    .stream()
                    .anyMatch(ScenarioChange::hasRiSetting);
            final GetPlanReservedInstanceCostStatsRequest riCostRequest =
                            GetPlanReservedInstanceCostStatsRequest.newBuilder()
                                            .setPlanId(planInstance.getPlanId())
                                            .setIncludeBuyRi(includeBuyRi)
                                            .build();
            final GetPlanReservedInstanceCostStatsResponse response =
                            planReservedInstanceService.getPlanReservedInstanceCostStats(riCostRequest);
            snapshots.addAll(convertRICostStatsToSnapshots(response.getStatsList()));

        }
        return mergeStatsByDate(snapshots);
    }

    private List<StatSnapshotApiDTO> getRICoverageSnapshots(final StatsQueryContext context)
            throws OperationFailedException {
        final GetReservedInstanceCoverageStatsRequest coverageRequest =
                createCoverageRequest(context);
        final GetReservedInstanceCoverageStatsResponse coverageResponse =
                riUtilizationCoverageService.getReservedInstanceCoverageStats(coverageRequest);
        return internalConvertRIStatsRecordsToStatSnapshotApiDTO(
                coverageResponse.getReservedInstanceStatsRecordsList(), true);
    }

    private List<StatSnapshotApiDTO> getRIUtilizationSnapshots(final StatsQueryContext context)
            throws OperationFailedException {
        final GetReservedInstanceUtilizationStatsRequest utilizationRequest =
                createUtilizationRequest(context);
        final GetReservedInstanceUtilizationStatsResponse utilizationResponse =
                riUtilizationCoverageService.getReservedInstanceUtilizationStats(
                        utilizationRequest);
        return internalConvertRIStatsRecordsToStatSnapshotApiDTO(
                utilizationResponse.getReservedInstanceStatsRecordsList(), false);
    }

    private List<StatSnapshotApiDTO> getNumRIStatsSnapshots(final long planId) {
        final GetPlanReservedInstanceBoughtCountRequest countRequest =
                GetPlanReservedInstanceBoughtCountRequest.newBuilder().setPlanId(planId).build();
        final GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                planReservedInstanceService.getPlanReservedInstanceBoughtCountByTemplateType(
                        countRequest);
        final Map<String, Long> riBoughtCountByTierName = response.getReservedInstanceCountMapMap();
        return convertNumRIStatsMapToStatSnapshotApiDTO(riBoughtCountByTierName);
    }

    @Nonnull
    GetReservedInstanceCoverageStatsRequest createCoverageRequest(
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final long planId = context.getPlanInstance().get().getPlanId();
        return internalCreateCoverageRequest(context,
                GetReservedInstanceCoverageStatsRequest.newBuilder()
                        .setTopologyContextId(planId)
                        .setIncludeBuyRiCoverage(buyRiScopeHandler.shouldIncludeBuyRiDiscount(
                                context.getInputScope())));
    }

    @Nonnull
    GetReservedInstanceUtilizationStatsRequest createUtilizationRequest(
            @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final long planId = context.getPlanInstance().get().getPlanId();
        return internalCreateUtilizationRequest(context,
                GetReservedInstanceUtilizationStatsRequest.newBuilder()
                        .setTopologyContextId(planId)
                        .setIncludeBuyRiUtilization(buyRiScopeHandler.shouldIncludeBuyRiDiscount(
                                context.getInputScope())));
    }
}
