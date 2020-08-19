package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.ImmutableStageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.AbstractStage;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.AllocatedDemandClassifier.AllocatedDemandClassifierFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.CloudTierFamilyMatcher.CloudTierFamilyMatcherFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.selection.EntityCloudTierDemandSet;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification;

/**
 * This stage is responsible for taking in a set of {@link EntityCloudTierMapping} instances, grouping
 * the mappings by the associated entity, and classifying each group of mappings. The classifications are
 * used to ignore demand entirely (e.g stale allocated demand) and filter out demand for a potential
 * recommendation (e.g. stable projected demand in real time, in which we only want recommendations for
 * optimized workloads).
 */
public class DemandClassificationStage extends AbstractStage<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> {

    private static final String STAGE_NAME = "Demand Classification";

    private final Logger logger = LogManager.getLogger();

    private final AllocatedDemandClassifier allocatedDemandClassifier;

    private final DemandClassification demandClassification;

    protected DemandClassificationStage(final long id,
                                        @Nonnull final CloudCommitmentAnalysisConfig analysisConfig,
                                        @Nonnull final CloudCommitmentAnalysisContext analysisContext,
                                        @Nonnull AllocatedDemandClassifierFactory allocatedDemandClassifierFactory,
                                        @Nonnull CloudTierFamilyMatcherFactory cloudTierFamilyMatcherFactory) {
        super(id, analysisConfig, analysisContext);

        this.demandClassification = analysisConfig.getDemandClassification();
        this.allocatedDemandClassifier = allocatedDemandClassifierFactory.newClassifier(
                cloudTierFamilyMatcherFactory.newFamilyMatcher(analysisContext.getCloudCommitmentSpecMatcher()),
                // defaults to zero if no allocated classification settings are sent
                demandClassification.getAllocatedClassificationSettings().getMinEntityUptime());
    }

    /**
     * Accepts a set of {@link EntityCloudTierMapping} instances, grouping the mappings by the
     * associated entity and classifying the grouped demand, based on a demand classifier. For allocated
     * demand, the {@link AllocatedDemandClassifier} is used.
     *
     * @param demandSet The set of unclassified demand to consider for classification.
     * @return A stage result containing demand grouped by the associated entity and classified.
     */
    @Nonnull
    @Override
    public StageResult<ClassifiedEntityDemandSet> execute(final EntityCloudTierDemandSet demandSet) {
        final Map<Long, TimeSeries<EntityCloudTierMapping>> allocationDemandByEntityOid = demandSet.allocatedDemand()
                .stream()
                .collect(Collectors.groupingBy(
                        EntityCloudTierMapping::entityOid,
                        TimeSeries.toTimeSeries()));


        logger.info("{} Classifying allocated demand for {} entities", logPrefix, allocationDemandByEntityOid.size());

        final DemandClassificationSummary classificationSummary =
                DemandClassificationSummary.newSummary(
                        analysisContext.getSourceCloudTopology(), demandClassification.getLogDetailedSummary());
        final Set<ClassifiedEntityDemandAggregate<AllocatedDemandClassification>> classifiedAllocationDemand =
                allocationDemandByEntityOid.values()
                        .stream()
                        .map(allocationTimeSeries -> {
                            // It is assumed that the scope information of an entity is immutable.
                            // For example, it is not possible to change the region of a VM. A representative
                            // mapping is selected for the entity to populate the scope information.
                            final EntityCloudTierMapping representativeMapping = allocationTimeSeries.first();
                            return ImmutableClassifiedEntityDemandAggregate.<AllocatedDemandClassification>builder()
                                    .entityOid(representativeMapping.entityOid())
                                    .accountOid(representativeMapping.accountOid())
                                    .regionOid(representativeMapping.regionOid())
                                    .availabilityZoneOid(representativeMapping.availabilityZoneOid())
                                    .serviceProviderOid(representativeMapping.serviceProviderOid())
                                    .putAllClassifiedCloudTierDemand(
                                            allocatedDemandClassifier.classifyEntityDemand(allocationTimeSeries))
                                    .build();
                        }).peek(classificationSummary.toAllocatedSummaryCollector())
                        .collect(ImmutableSet.toImmutableSet());

        return ImmutableStageResult.<ClassifiedEntityDemandSet>builder()
                .output(ImmutableClassifiedEntityDemandSet.builder()
                        .addAllClassifiedAllocatedDemand(classifiedAllocationDemand)
                        .build())
                .resultSummary(classificationSummary.toString())
                .build();
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public String stageName() {
        return STAGE_NAME;
    }

    /**
     * A factory class for creating {@link DemandClassificationStage} instances.
     */
    public static class DemandClassificationFactory implements
            AnalysisStage.StageFactory<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> {

        private final AllocatedDemandClassifierFactory allocatedDemandClassifierFactory;

        private final CloudTierFamilyMatcherFactory cloudTierFamilyMatcherFactory;

        /**
         * Constructs a new factory instance.
         * @param allocatedDemandClassifierFactory A factory for creating {@link AllocatedDemandClassifier}
         *                                         instances.
         * @param cloudTierFamilyMatcherFactory A factory for creating {@link CloudTierFamilyMatcher}
         *                                      instances.
         */
        public DemandClassificationFactory(@Nonnull AllocatedDemandClassifierFactory allocatedDemandClassifierFactory,
                                           @Nonnull CloudTierFamilyMatcherFactory cloudTierFamilyMatcherFactory) {

            this.allocatedDemandClassifierFactory = Objects.requireNonNull(allocatedDemandClassifierFactory);
            this.cloudTierFamilyMatcherFactory = Objects.requireNonNull(cloudTierFamilyMatcherFactory);
        }

        /**
         * Constructs a new instance of {@link DemandClassificationStage}.
         * @param id The unique ID of the stage.
         * @param config The configuration of the analysis.
         * @param context The context of the analysis, used to share context data across stages
         * @return The newly constructed instance of {@link DemandClassificationStage}.
         */
        @Nonnull
        @Override
        public AnalysisStage<EntityCloudTierDemandSet, ClassifiedEntityDemandSet> createStage(
                final long id,
                @Nonnull final CloudCommitmentAnalysisConfig config,
                @Nonnull final CloudCommitmentAnalysisContext context) {

            return new DemandClassificationStage(
                    id,
                    config,
                    context,
                    allocatedDemandClassifierFactory,
                    cloudTierFamilyMatcherFactory);
        }
    }

}
