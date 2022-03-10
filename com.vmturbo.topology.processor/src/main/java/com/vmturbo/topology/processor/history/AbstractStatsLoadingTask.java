package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Base for history loading tasks that use standard stats request.
 *
 * @param <Config> per-editor type configuration values holder
 * @param <T> the historical data subtype specific value as returned from historydb
 */
public abstract class AbstractStatsLoadingTask<Config, T> implements IHistoryLoadingTask<Config, T> {
    /**
     * Create the stats request from a passed list of commodity references.
     * It is expected that all entities are of the same type.
     * Group the list of references by entity for convenience of remote call
     * return value processing.
     *
     * @param commodities commodities to query from history component
     * @param startMs starting timestamp
     * @param endMs ending timestamp
     * @param rollup optional enforced rollup period to request
     * @return request for the history rpc service
     */
    @Nonnull
    protected static GetEntityStatsRequest
              createStatsRequest(@Nonnull Collection<EntityCommodityReference> commodities,
                                 long startMs,
                                 long endMs,
                                 @Nullable Long rollup) {
        // scope of entities
        EntityStatsScope.Builder scope = EntityStatsScope.newBuilder();
        Set<Long> entities = new HashSet<>();
        Set<CommodityTypeView> commTypes = new HashSet<>();
        // partition by entity
        for (EntityCommodityReference commRef : commodities) {
            entities.add(commRef.getEntityOid());
            commTypes.add(commRef.getCommodityType());
        }
        scope.setEntityList(EntityList.newBuilder().addAllEntities(entities));

        // filter by commodity types
        StatsFilter.Builder statsFilter = StatsFilter.newBuilder();
        commTypes.forEach(commType -> statsFilter.addCommodityRequests(CommodityRequest.newBuilder()
                        .setCommodityName(UICommodityType.fromType(commType.getType()).apiStr())));

        statsFilter.setStartDate(startMs);
        statsFilter.setEndDate(endMs);
        if (rollup != null) {
            statsFilter.setRollupPeriod(rollup);
        }

        return GetEntityStatsRequest.newBuilder()
                        .setScope(scope)
                        .setFilter(statsFilter)
                        .build();
    }

}
