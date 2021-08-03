package com.vmturbo.extractor.action.commodity;

import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.HistoryUtilizationType;
import com.vmturbo.history.schema.RelationType;

/**
 * Responsible for retrieving projected percentile values from the history component.
 * The history component keeps these values in memory so the overhead of making remote calls
 * shouldn't be too bad.
 */
class ProjectedTopologyCommodityDataRetriever {
    private static final Logger logger = LogManager.getLogger();

    private final StatsHistoryServiceBlockingStub statsService;

    ProjectedTopologyCommodityDataRetriever(StatsHistoryServiceBlockingStub statsService) {
        this.statsService = statsService;
    }

    @Nonnull
    TopologyActionCommodityData fetchProjectedCommodityData(@Nonnull final LongSet entityIds,
            @Nonnull final IntSet commodityTypes, @Nonnull final LongSet projectedProviders) {
        // Pagination is mandatory on the getProjectedEntityStats call.
        // TODO: Allow streaming, to avoid unnecessary sorting + RPC calls in large environments.
        String nextCursor = "";
        // Enforce a maximum number of iterations to avoid an infinite loop in case there is
        // something wrong with the server.
        int maxNumIterations = 10000;
        TopologyActionCommodityData data = new TopologyActionCommodityData();
        try {
            do {
                PaginationParameters params =
                        nextCursor.isEmpty() ? PaginationParameters.getDefaultInstance() : PaginationParameters.newBuilder().setCursor(nextCursor).build();
                ProjectedEntityStatsResponse response = statsService.getProjectedEntityStats(
                        ProjectedEntityStatsRequest.newBuilder()
                                .setScope(EntityStatsScope.newBuilder()
                                        .setEntityList(EntityList.newBuilder().addAllEntities(entityIds)))
                                .addAllCommodityName(commodityTypes.stream()
                                        .map(UICommodityType::fromType)
                                        .map(UICommodityType::apiStr)
                                        .collect(Collectors.toList()))
                                // only request bought commodities for given providers
                                .addAllProviders(projectedProviders)
                                .setPaginationParams(params).build());
                response.getEntityStatsList().forEach(entityStats -> addCommodityDataForEntity(data, entityStats));
                nextCursor = response.getPaginationResponse().getNextCursor();
            } while (!nextCursor.isEmpty() && maxNumIterations-- > 0);

            if (maxNumIterations <= 0) {
                logger.error("Exceeded maximum number of iterations."
                    + " Continuing with percentile data: {}", data.toString());
            }
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve projected percentile data for action percentiles.", e);
        }
        data.finish();
        return data;
    }

    private void addCommodityDataForEntity(TopologyActionCommodityData commodityData,
            EntityStats entityStats) {
        final long oid = entityStats.getOid();

        entityStats.getStatSnapshotsList().stream().findFirst().ifPresent(projectedSnapshot -> {
            projectedSnapshot.getStatRecordsList().forEach(statRecord -> {
                final CommodityType.Builder commTypeBldr = CommodityType.newBuilder();
                commTypeBldr.setType(UICommodityType.fromString(statRecord.getName()).typeNumber());
                if (!StringUtils.isEmpty(statRecord.getStatKey())) {
                    commTypeBldr.setKey(statRecord.getStatKey());
                }
                final CommodityType commType = commTypeBldr.build();
                // most StatRecord has providerUuid set, but some not (only relation is set)
                final boolean isBought = statRecord.hasProviderUuid()
                        || statRecord.getRelation().equals(RelationType.COMMODITIESBOUGHT.getLiteral());

                if (statRecord.hasUsed() && statRecord.getUsed().hasTotal()
                        && statRecord.hasCapacity() && statRecord.getCapacity().hasTotal()) {
                    if (isBought) {
                        commodityData.putBoughtCommodity(oid, commType,
                                statRecord.getUsed().getTotal(), statRecord.getCapacity().getTotal());
                    } else {
                        commodityData.putSoldCommodity(oid, commType,
                                statRecord.getUsed().getTotal(),
                                statRecord.getCapacity().getTotal());
                    }
                }

                statRecord.getHistUtilizationValueList().stream()
                    .filter(histValue -> histValue.getType()
                            .equalsIgnoreCase(HistoryUtilizationType.Percentile.getApiParameterName()))
                    .findFirst()
                    .ifPresent(percentileUtilization -> {
                        final float capacity = percentileUtilization.getCapacity().getTotal();
                        final float usage = percentileUtilization.getUsage().getTotal();
                        if (capacity > 0) {
                            final float projectedPercentile = usage / capacity;
                            if (isBought) {
                                commodityData.putBoughtPercentile(oid, commType, projectedPercentile);
                            } else {
                                commodityData.putSoldPercentile(oid, commType, projectedPercentile);
                            }
                        }
                    });
            });
        });
    }
}
