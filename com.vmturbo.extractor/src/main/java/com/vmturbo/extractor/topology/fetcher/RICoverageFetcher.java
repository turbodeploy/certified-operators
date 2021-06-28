package com.vmturbo.extractor.topology.fetcher;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetProjectedEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory.RICoverageData;

/**
 * Class to fetch RI coverage data from the cost component.
 */
public class RICoverageFetcher extends DataFetcher<RICoverageData> {
    private final Long topologyContextId;
    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService;
    private final boolean requestProjected;

    /**
     * Constructor.
     *
     * @param timer a {@link MultiStageTimer} to collect timing information for this fetcher
     * @param consumer the consumer which will consume the response of this fetcher
     * @param riCoverageService rpc service for fetching RI coverage data
     * @param requestProjected whether this is for fetching projected cost or current cost
     * @param topologyContextId id of the topology context, like real time or plan topology
     */
    public RICoverageFetcher(@Nonnull MultiStageTimer timer, @Nonnull Consumer<RICoverageData> consumer,
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService,
            boolean requestProjected, @Nullable Long topologyContextId) {
        super(timer, consumer);
        this.riCoverageService = riCoverageService;
        this.requestProjected = requestProjected;
        this.topologyContextId = requestProjected ? Objects.requireNonNull(topologyContextId) : null;
    }

    @Override
    protected RICoverageData fetch() {
        final RICoverageData riCoverageData = new RICoverageData();
        try {
            Map<Long, EntityReservedInstanceCoverage> coverageByEntity;
            if (requestProjected) {
                // topologyContextId is needed for projected RI Coverage
                coverageByEntity = riCoverageService.getProjectedEntityReservedInstanceCoverageStats(
                        GetProjectedEntityReservedInstanceCoverageRequest.newBuilder()
                                .setTopologyContextId(topologyContextId).build())
                        .getCoverageByEntityIdMap();
            } else {
                coverageByEntity = riCoverageService.getEntityReservedInstanceCoverage(
                        GetEntityReservedInstanceCoverageRequest.newBuilder().build())
                        .getCoverageByEntityIdMap();
            }

            coverageByEntity.forEach((entityId, coverage) -> {
                double capacity = coverage.getEntityCouponCapacity();
                double used = coverage.getCouponsCoveredByRiMap().values().stream()
                        .mapToDouble(Double::doubleValue).sum();
                riCoverageData.addRICoveragePercentage(entityId, ExportUtils.toPercentage(used, capacity));
            });

            logger.info("Fetched {} RI coverage for {} entities",
                    requestProjected ? "projected" : "current", riCoverageData.size());
        } catch (StatusRuntimeException e) {
            logger.error("Failed to fetch RI coverage from cost component", e);
        }
        return riCoverageData;
    }
}
