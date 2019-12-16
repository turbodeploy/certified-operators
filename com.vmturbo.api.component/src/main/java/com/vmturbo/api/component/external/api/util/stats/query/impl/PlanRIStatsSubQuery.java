package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
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
     */
    public PlanRIStatsSubQuery(@Nonnull RepositoryApi repositoryApi,
                    @Nonnull PlanReservedInstanceServiceBlockingStub planReservedInstanceService) {
        super(repositoryApi);
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
        if (containsStat(StringConstants.NUM_RI, stats)) {
            snapshots.addAll(getNumRIStatsSnapshots(context.getPlanInstance().get().getPlanId()));
        }
        if (containsStat(StringConstants.RI_COUPON_COVERAGE, stats)) {
            //TODO: RI coupon coverage stats should be added to snapshots.
        }
        if (containsStat(StringConstants.RI_COUPON_UTILIZATION, stats)) {
            //TODO: RI coupon utilization stats should be added to snapshots.
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
