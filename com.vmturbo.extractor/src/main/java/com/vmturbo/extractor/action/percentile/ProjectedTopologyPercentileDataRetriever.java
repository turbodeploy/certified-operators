package com.vmturbo.extractor.action.percentile;

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
import com.vmturbo.common.protobuf.utils.StringConstants;

/**
 * Responsible for retrieving projected percentile values from the history component.
 * The history component keeps these values in memory so the overhead of making remote calls
 * shouldn't be too bad.
 */
class ProjectedTopologyPercentileDataRetriever {
    private static final Logger logger = LogManager.getLogger();

    private final StatsHistoryServiceBlockingStub statsService;

    ProjectedTopologyPercentileDataRetriever(StatsHistoryServiceBlockingStub statsService) {
        this.statsService = statsService;
    }

    @Nonnull
    TopologyPercentileData fetchPercentileData(@Nonnull final LongSet entityIds,
                                               @Nonnull final IntSet commodityTypes) {
        // Pagination is mandatory on the getProjectedEntityStats call.
        // TODO: Allow streaming, to avoid unnecessary sorting + RPC calls in large environments.
        String nextCursor = "";
        // Enforce a maximum number of iterations to avoid an infinite loop in case there is
        // something wrong with the server.
        int maxNumIterations = 10000;
        TopologyPercentileData data = new TopologyPercentileData();
        try {
            do {
                PaginationParameters params =
                        nextCursor.isEmpty() ? PaginationParameters.getDefaultInstance() : PaginationParameters.newBuilder().setCursor(nextCursor).build();
                ProjectedEntityStatsResponse response = statsService.getProjectedEntityStats(
                        ProjectedEntityStatsRequest.newBuilder().setScope(EntityStatsScope.newBuilder()
                                .setEntityList(EntityList.newBuilder().addAllEntities(entityIds))).addAllCommodityName(commodityTypes.stream()
                                .map(UICommodityType::fromType)
                                .map(UICommodityType::apiStr)
                                .collect(Collectors.toList())).setPaginationParams(params).build());
                response.getEntityStatsList().forEach(entityStats -> addPercentilesForEntity(data, entityStats));
                nextCursor = response.getPaginationResponse().getNextCursor();
            } while (!nextCursor.isEmpty() && maxNumIterations-- > 0);

            if (maxNumIterations <= 0) {
                logger.error("Exceeded maximum number of iterations."
                    + " Continuing with percentile data: {}", data.toString());
            }
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve projected percentile data for action percentiles.", e);
        }
        return data;
    }

    private void addPercentilesForEntity(TopologyPercentileData percentileData,
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

                if (statRecord.getProviderUuid().isEmpty()) {
                    statRecord.getHistUtilizationValueList().stream()
                        .filter(histValue -> histValue.getType()
                                .equalsIgnoreCase(StringConstants.PERCENTILE))
                        .findFirst()
                        .ifPresent(percentileUtilization -> {
                            final float capacity = percentileUtilization.getCapacity().getTotal();
                            final float usage = percentileUtilization.getUsage().getTotal();
                            if (capacity > 0) {
                                final float projectedPercentile = usage / capacity;
                                // Sold
                                percentileData.putSoldPercentile(oid, commType,
                                        projectedPercentile);
                            }
                        });
                }
            });
        });
    }
}
