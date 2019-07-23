package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * This stats sub-query retrieves the number of clusters.
 *
 * We don't currently save the number of clusters historically (because there is no need). For
 * the current value we simply need to make an RPC call to the group component.
 */
public class NumClustersStatsSubQuery implements StatsSubQuery {

    private final GroupServiceBlockingStub groupRpcService;

    public NumClustersStatsSubQuery(@Nonnull final GroupServiceBlockingStub groupRpcService) {
        this.groupRpcService = groupRpcService;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        // Only support global scope for now.
        // It might make sense to support cluster counts in other groups too - in particular for
        // groups of clusters - but it's unclear if we need to.
        //
        // We only have the current number of clusters.
        // Because the number of clusters is dynamic, it would be misleading to return current
        // stats for some date in the past.
        return context.isGlobalScope() && context.includeCurrent();
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        return SubQuerySupportedStats.some(context.findStats(Collections.singleton(StringConstants.NUM_CLUSTERS)));
    }

    @Nonnull
    @Override
    public Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> stats,
                                                         @Nonnull final StatsQueryContext context) throws OperationFailedException {
        final int numClusters = groupRpcService.countGroups(GetGroupsRequest.newBuilder()
                .addTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                    .setTypeFilter(ClusterInfo.Type.COMPUTE))
                .build())
            .getCount();
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(StringConstants.NUM_CLUSTERS);
        statApiDTO.setValue((float)numClusters);

        final StatValueApiDTO values = new StatValueApiDTO();
        values.setTotal((float)numClusters);
        values.setMin((float)numClusters);
        values.setMax((float)numClusters);
        values.setAvg((float)numClusters);

        statApiDTO.setValues(values);

        return ImmutableMap.of(context.getCurTime(), Collections.singletonList(statApiDTO));
    }
}
