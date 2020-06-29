package com.vmturbo.cloud.commitment.analysis.runtime.stages;

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

import org.stringtemplate.v4.ST;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReader;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStageResult;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;

/**
 * The demand selection stage is responsible for querying the demand stores for appropriate demand,
 * based on selection filters in the CCA config. It wraps the {@link CloudCommitmentDemandReader}.
 */
public class DemandSelectionStage extends AbstractStage<Void, Set<EntityCloudTierMapping<?>>> {

    private static final String STAGE_NAME = "Demand Selection";

    private final CloudCommitmentDemandReader demandReader;

    private final HistoricalDemandSelection demandSelection;

    private final boolean logDetailedSummary;

    /**
     * Constructs a new demand selection stage instance with the appropriate stores and analysis info.
     * @param id The ID of the stage.
     * @param config The analysis config.
     * @param context The analysis context.
     * @param demandReader The demand reader
     */
    public DemandSelectionStage(long id,
                          @Nonnull final CloudCommitmentAnalysisConfig config,
                          @Nonnull final CloudCommitmentAnalysisContext context,
                          @Nonnull final CloudCommitmentDemandReader demandReader) {
        super(id, config, context);

        this.demandReader = Objects.requireNonNull(demandReader);
        this.demandSelection = Objects.requireNonNull(config.getDemandSelection());
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
    public AnalysisStage.StageResult<Set<EntityCloudTierMapping<?>>> execute(final Void aVoid) {

        final Instant lookBackStartTime = analysisContext.getAnalysisStartTime()
                .orElseThrow(() -> new IllegalStateException("Analysis start time must be set"));

        final Stream<EntityCloudTierMapping<?>> persistedDemandStream = demandReader.getDemand(
                demandSelection.getCloudTierType(),
                demandSelection.getDemandSegmentList(),
                lookBackStartTime);

        final DemandSummary demandSummary = DemandSummary.newSummary(logDetailedSummary);
        final Set<EntityCloudTierMapping<?>> selectedDemand = persistedDemandStream
                // Some demand may end after the lookback start time but start before it. In these
                // cases, we trim the demand to start on the lookback start time
                .map(m -> trimDemandStartTime(m, lookBackStartTime))
                .peek(demandSummary.toSummaryCollector())
                .collect(ImmutableSet.toImmutableSet());

        return ImmutableStageResult.<Set<EntityCloudTierMapping<?>>>builder()
                .output(selectedDemand)
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
     * @param lookBackStartTime The minimum start time to allow for {@code cloudTierMapping}.
     * @return A normalized entity cloud tier mapping instance.
     */
    private EntityCloudTierMapping<?> trimDemandStartTime(@Nonnull EntityCloudTierMapping cloudTierMapping,
                                                          @Nonnull Instant lookBackStartTime) {

        if (cloudTierMapping.startTime().isBefore(lookBackStartTime)) {
            return ImmutableEntityCloudTierMapping.builder()
                    .from(cloudTierMapping)
                    .startTime(lookBackStartTime)
                    .build();
        } else {
            return cloudTierMapping;
        }
    }

    @NotThreadSafe
    private static class DemandSummary {

        private static final String DEMAND_SELECTION_SUMMARY_TEMPLATE =
                "Earliest Start Time: <earliestStartTime>\n" +
                "Duration:\n" +
                "    Total: <totalDuration>\n" +
                "    Avg: <averageDuration>\n" +
                "    Max: <maxDuration>\n" +
                "    Count: <durationCount>\n" +
                "Unique Entities: <uniqueEntities>\n" +
                "Unique Demand: <uniqueDemand>\n";

        private final boolean detailedSummary;

        private Instant earliestStartTime;

        private final LongSummaryStatistics durationStats = new LongSummaryStatistics();

        private final Set<Long> uniqueEntities = Sets.newConcurrentHashSet();

        private final Set uniqueDemand = Sets.newConcurrentHashSet();

        private final Map<?, LongSummaryStatistics> durationStatsByDemand = Maps.newConcurrentMap();

        private DemandSummary(boolean detailedSummary) {
            this.detailedSummary = detailedSummary;
        }

        public static DemandSummary newSummary(boolean detailedSummary) {
            return new DemandSummary(detailedSummary);
        }

        public Consumer<EntityCloudTierMapping<?>> toSummaryCollector() {

            return (entityCloudTierMapping) -> {

                final Instant startTime = entityCloudTierMapping.startTime();
                if (earliestStartTime == null  ||
                        startTime.isBefore(earliestStartTime)) {
                    earliestStartTime = startTime;
                }

                final Instant endTime = entityCloudTierMapping.endTime();
                final long durationMillis = Duration.between(startTime, endTime).toMillis();
                durationStats.accept(durationMillis);

                if (detailedSummary) {
                    uniqueEntities.add(entityCloudTierMapping.entityOid());
                    uniqueDemand.add(entityCloudTierMapping.cloudTierDemand());
                }
            };
        }

        @Override
        public String toString() {
            final ST template = new ST(DEMAND_SELECTION_SUMMARY_TEMPLATE);

            template.add("earliestStartTime", earliestStartTime);

            template.add("totalDuration", Duration.ofMillis(durationStats.getSum()));
            template.add("averageDuration", Duration.ofMillis((long)durationStats.getAverage()));
            template.add("maxDuration", Duration.ofMillis(durationStats.getMax()));
            template.add("durationCount", durationStats.getCount());
            template.add("uniqueEntities", uniqueEntities.size());
            template.add("uniqueDemand", uniqueDemand.size());

            return template.render();
        }

    }

    /**
     * A factory class for creating instances of {@link DemandSelectionStage}.
     */
    public static class DemandSelectionFactory implements AnalysisStage.StageFactory<Void,Set<EntityCloudTierMapping<?>>> {

        private final CloudCommitmentDemandReader demandReader;

        /**
         * Construct a demand selection factory.
         * @param demandReader The demand reader
         */
        public DemandSelectionFactory(@Nonnull CloudCommitmentDemandReader demandReader) {
            this.demandReader = Objects.requireNonNull(demandReader);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public AnalysisStage<Void,Set<EntityCloudTierMapping<?>>> createStage(
                final long id,
                @Nonnull final CloudCommitmentAnalysisConfig config,
                @Nonnull final CloudCommitmentAnalysisContext context) {

            return new DemandSelectionStage(id, config, context, demandReader);
        }
    }
}
