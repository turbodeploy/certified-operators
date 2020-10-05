package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.inventory.AnalysisSegment;
import com.vmturbo.cloud.commitment.analysis.inventory.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.inventory.AnalysisTopology.Builder;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentCapacity;
import com.vmturbo.cloud.commitment.analysis.inventory.ImmutableCloudCommitmentCapacity;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateAnalysisDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandSegment;
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
        CloudCommitmentInventory cloudCommitmentInventory = analysisConfig.getCloudCommitmentInventory();
        Builder builder = AnalysisTopology.builder();
        List<CloudCommitmentData> cloudCommitmentDataList = cloudCommitmentBoughtResolver.getCloudCommitment(cloudCommitmentInventory);
        List<AnalysisSegment> analysisSegments = new ArrayList<>();
        Map<Long, CloudCommitmentCapacity> cloudCommitmentCapacityMap = getCloudCommitmentCapacityByOid(cloudCommitmentDataList);
        for (AggregateDemandSegment analysisDemandSegment: aggregateAnalysisDemand.aggregateDemandSeries()) {
            AnalysisSegment.Builder analysisSegmentBuilder = AnalysisSegment.builder();
            analysisSegmentBuilder.addAllAggregateCloudTierDemandSet(analysisDemandSegment.aggregateCloudTierDemand());
            analysisSegmentBuilder.putAllCloudCommitmentByOid(cloudCommitmentCapacityMap);
            analysisSegmentBuilder.timeInterval(analysisDemandSegment.timeInterval());
            analysisSegments.add(analysisSegmentBuilder.build());
        }
        TimeSeries<AnalysisSegment> timeSeriesOutput = TimeSeries.newTimeSeries(analysisSegments);
        Map<Long, CloudCommitmentData> cloudCommitmentByOidMap = cloudCommitmentDataList.stream().collect(Collectors.toMap(CloudCommitmentData::commitmentId,
                Function.identity()));
        builder.segment(timeSeriesOutput);
        builder.cloudCommitmentsByOid(cloudCommitmentByOidMap);
        return StageResult.<AnalysisTopology>builder().output(builder.build()).build();
    }


    private Map<Long, CloudCommitmentCapacity> getCloudCommitmentCapacityByOid(List<CloudCommitmentData> cloudCommitmentDataList) {
        // Currently this method does not filter the available capacity bu historical usage. That will be implemented
        // as part of OM-62160
        Map<Long, CloudCommitmentCapacity> map = new HashMap<>();
        for (CloudCommitmentData ccData: cloudCommitmentDataList) {
            ReservedInstanceBoughtCoupons riCoupons = ccData.asReservedInstance().commitment().getReservedInstanceBoughtInfo().getReservedInstanceBoughtCoupons();
            map.put(ccData.commitmentId(), ImmutableCloudCommitmentCapacity.builder().capacityAvailable(riCoupons.getNumberOfCoupons()).build());
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
