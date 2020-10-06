package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentCapacity;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopologySegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateAnalysisDemand;
import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;

/**
 * This stage is responsible for retrieving ReservedInstances describes in the CCA protobuf and returning
 * a time series breakdown of utilization over the defined segment interval in the protobuf.
 */
public class CloudCommitmentInventoryResolverStage extends AbstractStage<AggregateAnalysisDemand, AnalysisTopology> {

    private final Logger logger = LogManager.getLogger();

    private final CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver;

    private static final String STAGE_NAME = "Inventory Resolver";

    /**
     * Constructor for the CloudCommitmentInventoryResolverStage.
     *
     * @param id The id of the analsyis.
     * @param config The cloud commitment analysis config.
     * @param context The cloud commitment analysis context.
     * @param cloudCommitmentBoughtResolver The cloud commitment bought resolver.
     */
    public CloudCommitmentInventoryResolverStage(long id,
            @Nonnull final CloudCommitmentAnalysisConfig config,
            @Nonnull final CloudCommitmentAnalysisContext context,
            @Nonnull final CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver) {
        super(id, config, context);
        this.cloudCommitmentBoughtResolver = cloudCommitmentBoughtResolver;
    }

    @Nonnull
    @Override
    public StageResult<AnalysisTopology> execute(AggregateAnalysisDemand aggregateAnalysisDemand) {

        final CloudCommitmentInventory cloudCommitmentInventory = analysisConfig.getCloudCommitmentInventory();
        final List<CloudCommitmentData> cloudCommitmentDataList = cloudCommitmentBoughtResolver.getCloudCommitment(cloudCommitmentInventory);

        final Map<Long, CloudCommitmentCapacity> cloudCommitmentCapacityMap =
                getCloudCommitmentCapacityByOid(cloudCommitmentDataList);

        final AnalysisTopology.Builder analysisTopologyBuilder = AnalysisTopology.builder()
                .putAllCloudCommitmentsByOid(cloudCommitmentDataList.stream()
                        .collect(ImmutableMap.toImmutableMap(
                                CloudCommitmentData::commitmentId,
                                Function.identity())));

        final TimeSeries<AnalysisTopologySegment> analysisSegments = aggregateAnalysisDemand.aggregateDemandSeries()
                .stream()
                .map(aggregateDemandSegment -> AnalysisTopologySegment.builder()
                        .timeInterval(aggregateDemandSegment.timeInterval())
                        .aggregateCloudTierDemandSet(aggregateDemandSegment.aggregateCloudTierDemand())
                        .cloudCommitmentByOid(cloudCommitmentCapacityMap)
                        .build())
                .collect(TimeSeries.toTimeSeries());

        analysisTopologyBuilder.segments(analysisSegments);
        return StageResult.<AnalysisTopology>builder()
                .output(analysisTopologyBuilder.build())
                .build();
    }


    private Map<Long, CloudCommitmentCapacity> getCloudCommitmentCapacityByOid(List<CloudCommitmentData> cloudCommitmentDataList) {
        // Currently this method does not filter the available capacity bu historical usage. That will be implemented
        // as part of OM-62160
        final Map<Long, CloudCommitmentCapacity> map = new HashMap<>();
        for (CloudCommitmentData ccData: cloudCommitmentDataList) {
            ReservedInstanceBoughtCoupons riCoupons = ccData.asReservedInstance().commitment().getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons();
            map.put(
                    ccData.commitmentId(),
                    CloudCommitmentCapacity.builder()
                            .capacityAvailable(riCoupons.getNumberOfCoupons())
                            .build());
        }
        return map;
    }

    @Nonnull
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * The CloudCommitmentInventoryResolverStageFactory is used to create the CloudCommitmentInventoryResolverStage.
     */
    public static class CloudCommitmentInventoryResolverStageFactory implements
            AnalysisStage.StageFactory<AggregateAnalysisDemand, AnalysisTopology> {

        private final CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver;

        /**
         * Constructor for the Cloud commitment inventory resolver stage.
         *
         * @param cloudCommitmentBoughtResolver The cloud commitment bought resolver to retrieve the list
         * of cloud commitments.
         */
        public CloudCommitmentInventoryResolverStageFactory(CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver) {
            this.cloudCommitmentBoughtResolver = cloudCommitmentBoughtResolver;
        }

        @Nonnull
        @Override
        public AnalysisStage<AggregateAnalysisDemand, AnalysisTopology> createStage(long id,
                @Nonnull CloudCommitmentAnalysisConfig config,
                @Nonnull CloudCommitmentAnalysisContext context) {
            return new CloudCommitmentInventoryResolverStage(id, config, context, cloudCommitmentBoughtResolver);
        }
    }
}
