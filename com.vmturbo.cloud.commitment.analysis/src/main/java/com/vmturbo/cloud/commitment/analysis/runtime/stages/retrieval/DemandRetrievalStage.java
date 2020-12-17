package com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval;

import java.time.Duration;
import java.time.Instant;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReader;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.group.api.GroupAndMembers;

/**
 * The demand retrieval stage is responsible for querying the demand stores for appropriate demand,
 * based on selection filters in the CCA config. It wraps the {@link CloudCommitmentDemandReader}.
 */
public class DemandRetrievalStage extends AbstractStage<Void, EntityCloudTierDemandSet> {

    private static final String STAGE_NAME = "Demand Retrieval";

    private final CloudCommitmentDemandReader demandReader;

    private final HistoricalDemandSelection demandSelection;

    private final MinimalCloudTopology<MinimalEntity> cloudTopology;

    private final boolean logDetailedSummary;

    /**
     * Constructs a new demand selection stage instance with the appropriate stores and analysis info.
     * @param id The ID of the stage.
     * @param config The analysis config.
     * @param context The analysis context.
     * @param demandReader The demand reader
     */
    public DemandRetrievalStage(long id,
                          @Nonnull final CloudCommitmentAnalysisConfig config,
                          @Nonnull final CloudCommitmentAnalysisContext context,
                          @Nonnull final CloudCommitmentDemandReader demandReader) {
        super(id, config, context);

        this.demandReader = Objects.requireNonNull(demandReader);
        this.demandSelection = Objects.requireNonNull(config.getDemandSelection());
        this.cloudTopology = context.getSourceCloudTopology();
        this.logDetailedSummary = demandSelection.getLogDetailedSummary();
    }

    /**
     * Queries demand from the {@link CloudCommitmentDemandReader}, based on the analysis config. A
     * summary of the selected demand is built as part of processing the data returned from the
     * demand reader
     * @param aVoid This argument is not used.
     * @return A stage result, containing the trimmed demand based on demand selection.
     */
    @Override
    public AnalysisStage.StageResult<EntityCloudTierDemandSet> execute(final Void aVoid) {

        final TimeInterval analysisWindow = analysisContext.getAnalysisWindow()
                .orElseThrow(() -> new IllegalStateException("Analysis window must be set"));

        final DemandScope allocatedDemandScope = demandSelection.getAllocatedSelection()
                .getDemandSelection()
                .getScope();
        final Stream<EntityCloudTierMapping> persistedDemandStream = demandReader.getAllocationDemand(
                demandSelection.getCloudTierType(),
                allocatedDemandScope,
                analysisWindow);

        final DemandSummary demandSummary = DemandSummary.newSummary(logDetailedSummary);
        final Set<EntityCloudTierMapping> selectedDemand = persistedDemandStream
                // The demand reader will return demand which overlaps with the analysis windows. This may
                // mean that some of the entries either start before the analysis start time or end
                // after it. In this case, we trim the demand to only the analysis window so that any
                // downstream stages only consider demand within the window
                .map(m -> trimDemandToAnalysisWindow(m, analysisWindow))
                // Billing family is not stored with the demand, given it can fluctuate with discovery
                // (e.g. if AWS org access is added after discovery). Therefore, we resolve the billing
                // family after demand retrieval
                .map(this::addBillingFamily)
                .peek(demandSummary.toSummaryCollector())
                .collect(ImmutableSet.toImmutableSet());

        return StageResult.<EntityCloudTierDemandSet>builder()
                .output(ImmutableEntityCloudTierDemandSet.builder()
                        .addAllAllocatedDemand(selectedDemand)
                        .build())
                .resultSummary(demandSummary.toString())
                .build();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * If cloud tier mapping stretches over the target look back start time, this method will update
     * the star time of the mapping to the look back start time. If the mapping's start time is after
     * the look back start time, the mapping will be directly returned.
     * @param cloudTierMapping The entity cloud tier mapping to process.
     * @param analysisWindow The analysis window.
     * @return A normalized entity cloud tier mapping instance.
     */
    private EntityCloudTierMapping trimDemandToAnalysisWindow(@Nonnull EntityCloudTierMapping cloudTierMapping,
                                                              @Nonnull TimeInterval analysisWindow) {

        final Instant startTime = cloudTierMapping.timeInterval().startTime();
        final Instant endTime = cloudTierMapping.timeInterval().endTime();

        if (startTime.isBefore(analysisWindow.startTime()) || endTime.isAfter(analysisWindow.endTime())) {

            return ImmutableEntityCloudTierMapping.builder()
                    .from(cloudTierMapping)
                    .timeInterval(TimeInterval.builder()
                            .startTime(startTime.isBefore(analysisWindow.startTime())
                                    ? analysisWindow.startTime()
                                    : startTime)
                            .endTime(endTime.isAfter(analysisWindow.endTime())
                                    ? analysisWindow.endTime()
                                    : endTime)
                            .build())
                    .build();
        } else {
            return cloudTierMapping;
        }
    }

    private EntityCloudTierMapping addBillingFamily(@Nonnull EntityCloudTierMapping cloudTierMapping) {
        return ImmutableEntityCloudTierMapping.copyOf(cloudTierMapping)
                .withBillingFamilyId(cloudTopology.getBillingFamilyForAccount(cloudTierMapping.accountOid())
                        .map(GroupAndMembers::group)
                        .map(Grouping::getId));
    }

    /**
     * Static class representing the detailed summary of the demand.
     */
    @NotThreadSafe
    private static class DemandSummary {

        private static final String DEMAND_SELECTION_SUMMARY_TEMPLATE =
                "Earliest Start Time: <earliestStartTime>\n"
                        + "Duration:\n"
                        + "    Total: <totalDuration>\n"
                        + "    Avg: <averageDuration>\n"
                        + "    Max: <maxDuration>\n"
                        + "    Count: <durationCount>\n";
        private static final String DEMAND_SELECTION_DETAILED_SUMMARY_TEMPLATE =
                DEMAND_SELECTION_SUMMARY_TEMPLATE
                        + "Unique Entities: <uniqueEntities>\n"
                        + "Unique Demand: <uniqueDemand>\n";

        private final boolean detailedSummary;

        private Instant earliestStartTime;

        private final LongSummaryStatistics durationStats = new LongSummaryStatistics();

        private final Set<Long> uniqueEntities = Sets.newConcurrentHashSet();

        private final Map<CloudTierDemand, LongSummaryStatistics> durationStatsByDemand = Maps.newConcurrentMap();

        private DemandSummary(boolean detailedSummary) {
            this.detailedSummary = detailedSummary;
        }

        public static DemandSummary newSummary(boolean detailedSummary) {
            return new DemandSummary(detailedSummary);
        }

        public Consumer<EntityCloudTierMapping> toSummaryCollector() {

            return (entityCloudTierMapping) -> {

                final TimeInterval mappingInterval = entityCloudTierMapping.timeInterval();
                final Instant startTime = mappingInterval.startTime();
                if (earliestStartTime == null
                        || startTime.isBefore(earliestStartTime)) {
                    earliestStartTime = startTime;
                }

                final long durationMillis = mappingInterval.duration().toMillis();
                durationStats.accept(durationMillis);

                if (detailedSummary) {
                    uniqueEntities.add(entityCloudTierMapping.entityOid());

                    durationStatsByDemand.computeIfAbsent(
                            entityCloudTierMapping.cloudTierDemand(),
                            demand -> new LongSummaryStatistics())
                                .accept(durationMillis);
                }
            };
        }

        @Override
        public String toString() {
            final ST template = new ST(detailedSummary
                    ? DEMAND_SELECTION_DETAILED_SUMMARY_TEMPLATE
                    : DEMAND_SELECTION_SUMMARY_TEMPLATE);

            template.add("earliestStartTime", earliestStartTime);

            template.add("totalDuration", Duration.ofMillis(durationStats.getSum()));
            template.add("averageDuration", Duration.ofMillis((long)durationStats.getAverage()));
            template.add("maxDuration", Duration.ofMillis(durationStats.getMax()));
            template.add("durationCount", durationStats.getCount());

            if (detailedSummary) {
                template.add("uniqueEntities", uniqueEntities.size());
                template.add("uniqueDemand", durationStatsByDemand.size());
            }

            return template.render();
        }

    }

    /**
     * A factory class for creating instances of {@link DemandRetrievalStage}.
     */
    public static class DemandRetrievalFactory implements AnalysisStage.StageFactory<Void, EntityCloudTierDemandSet> {

        private final CloudCommitmentDemandReader demandReader;

        /**
         * Construct a demand selection factory.
         * @param demandReader The demand reader
         */
        public DemandRetrievalFactory(@Nonnull CloudCommitmentDemandReader demandReader) {
            this.demandReader = Objects.requireNonNull(demandReader);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public AnalysisStage<Void, EntityCloudTierDemandSet> createStage(
                final long id,
                @Nonnull final CloudCommitmentAnalysisConfig config,
                @Nonnull final CloudCommitmentAnalysisContext context) {

            return new DemandRetrievalStage(id, config, context, demandReader);
        }
    }
}
