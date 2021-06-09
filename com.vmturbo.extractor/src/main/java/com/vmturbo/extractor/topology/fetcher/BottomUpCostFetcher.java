package com.vmturbo.extractor.topology.fetcher;

import java.util.Iterator;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;

/**
 * Class to fetch bottom-up cost data from the cost component.
 */
public class BottomUpCostFetcher extends DataFetcher<BottomUpCostData> {
    private final BottomUpCostFetcherFactory fetcherFactory;
    private final long snapshotTime;
    private final CostServiceBlockingStub costService;

    /**
     * Constructor.
     *  @param timer         a {@link MultiStageTimer} to collect timing information for this
     *                       fetcher.
     * @param snapshotTime   topology snapshot time of cost data to fetch
     * @param consumer       the consumer which will consume the response of this fetcher
     * @param costService    cost service endpoint
     * @param fetcherFactory Which is used to access the cost data
     */
    public BottomUpCostFetcher(@Nonnull MultiStageTimer timer,
            final long snapshotTime, @Nonnull Consumer<BottomUpCostData> consumer,
            @Nonnull final CostServiceBlockingStub costService,
            @Nonnull final BottomUpCostFetcherFactory fetcherFactory) {
        super(timer, consumer);
        this.snapshotTime = snapshotTime;
        this.fetcherFactory = fetcherFactory;
        this.costService = costService;
    }

    @Override
    protected BottomUpCostData fetch() {
        synchronized (fetcherFactory) {
            try {
                return fetchCostData();
            } catch (StatusRuntimeException e) {
                logger.error("Failed to fetch bottom-up entity costs from cost component. Error: {}",
                        e.getLocalizedMessage());
                return null;
            }
        }
    }

    private BottomUpCostData fetchCostData() {
        final BottomUpCostData newCostData = new BottomUpCostData(snapshotTime);
        // TODO: Remove this ugly hack
        // During the sprint introducing this code, our build environment was still using
        // a MySQL V5.5 database server. This code is intended to retrieve new cost
        // data based on a specific topology's snapshot time, which is held at millisecond
        // granularity. However, the cost data stores snapshot times at one-second
        // granularity. An attempt to update the schema to use `timestamp(3)` columns
        // instead of `timestamp` failed because it would not build in Jenkins, since
        // fractional seconds were not introduced into MySQL until v5.6. For now, we'll
        // truncate the snapshot time before making the query, but ultimately this should
        // be changed to use the precise snapshot time for both start and end times in the
        // cost query.
        long start = snapshotTime - (snapshotTime % 1000);
        long end = start + 999;
        final Iterator<GetCloudCostStatsResponse> response = costService.getCloudCostStats(
                GetCloudCostStatsRequest.newBuilder()
                        .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
//                                .setStartDate(snapshotTime)
//                                .setEndDate(snapshotTime)
                                .setStartDate(start)
                                .setEndDate(end)
                                // exclude entity uptime discount to so we record actual cost
                                .setCostSourceFilter(CostSourceFilter.newBuilder()
                                        .addCostSources(CostSource.ENTITY_UPTIME_DISCOUNT)
                                        .setExclusionFilter(true))
                                .build())
                        .build());
        response.forEachRemaining(chunk ->
                chunk.getCloudStatRecordList().forEach(rec ->
                        rec.getStatRecordsList().forEach(newCostData::addEntityCost)));
        return newCostData;
    }
}
