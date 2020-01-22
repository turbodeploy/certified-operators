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
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Sub-query responsible for getting reserved instance stats for the plan from the cost component.
 */
public class PlanRIStatsSubQuery extends AbstractRIStatsSubQuery {
    private final PlanReservedInstanceServiceBlockingStub planReservedInstanceService;

    /**
     * Creates {@link PlanRIStatsSubQuery} instance.
     *
     * @param repositoryApi repository API.
     * @param planReservedInstanceService plan RI service blocking stub.
     * @param buyRiScopeHandler buy RI scope handler.
     */
    public PlanRIStatsSubQuery(@Nonnull RepositoryApi repositoryApi,
                    @Nonnull PlanReservedInstanceServiceBlockingStub planReservedInstanceService,
                    @Nonnull BuyRiScopeHandler buyRiScopeHandler) {
        super(repositoryApi, buyRiScopeHandler);
        this.planReservedInstanceService = Objects.requireNonNull(planReservedInstanceService);
    }

    @Override
    public boolean applicableInContext(StatsQueryContext context) {
        return context.getInputScope().isPlan();
    }

    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(Set<StatApiInputDTO> stats, StatsQueryContext context)
                    throws OperationFailedException {
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
            //TODO: RI coupon coverage stats should be added to snapshots.
        }
        if (containsStat(StringConstants.RI_COUPON_UTILIZATION, stats)) {
            //TODO: RI coupon utilization stats should be added to snapshots.
        }
        if (containsStat(StringConstants.RI_COST, stats)) {
            final boolean includeBuyRi = planInstance.getScenario().getScenarioInfo().getChangesList().stream()
                            .anyMatch(change -> change.hasRiSetting());
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

    private List<StatSnapshotApiDTO> getNumRIStatsSnapshots(long planId) {
        final GetPlanReservedInstanceBoughtCountRequest countRequest =
                        GetPlanReservedInstanceBoughtCountRequest.newBuilder().setPlanId(planId).build();
        final GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                        planReservedInstanceService.getPlanReservedInstanceBoughtCountByTemplateType(countRequest)
                                                .toBuilder().build();
        final Map<String, Long> riBoughtCountByTierName = response.getReservedInstanceCountMapMap();
        return convertNumRIStatsMapToStatSnapshotApiDTO(riBoughtCountByTierName);
    }

}
