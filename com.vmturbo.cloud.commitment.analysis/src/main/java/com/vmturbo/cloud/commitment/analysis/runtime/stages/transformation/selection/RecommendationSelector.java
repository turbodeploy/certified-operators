package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassificationFilter.ClassificationFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.CloudTierFilter.CloudTierFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.EntityStateFilter.EntityStateFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ScopedEntityFilter.ScopedEntityFilterFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandSelection;

/**
 * The recommendation selector is responsible for filtering demand based on all the selection
 * criteria for purchase recommendations (entity scope, entity state, demand classification, cloud
 * tier). Unlike the {@link AnalysisSelector}, its filtering does not drop the demand, but rather
 * marks it as a recommendation candidate. Demand not marked as a recommendation candidate will still
 * be used for uncovered demand analysis, but it will not be used in calculating potential savings
 * from purchase recommendations.
 */
public class RecommendationSelector {

    private final Logger logger = LogManager.getLogger();

    private final ScopedEntityFilter scopedEntityFilter;

    private final EntityStateFilter entityStateFilter;

    private final CloudTierFilter cloudTierFilter;

    private final ClassificationFilter classificationFilter;

    private RecommendationSelector(@Nonnull ScopedEntityFilter scopedEntityFilter,
                                   @Nonnull EntityStateFilter entityStateFilter,
                                   @Nonnull CloudTierFilter cloudTierFilter,
                                   @Nonnull ClassificationFilter classificationFilter) {

        this.scopedEntityFilter = scopedEntityFilter;
        this.entityStateFilter = entityStateFilter;
        this.cloudTierFilter = cloudTierFilter;
        this.classificationFilter = classificationFilter;
    }

    /**
     * Based on the configured recommendation selection criteria (scope, classification, entity state,
     * etc) the {@code entityAggregate} will be broken into (entity, cloud tier) tuples (represented
     * as {@link ClassifiedEntitySelection}) and each will be conditionally marked as a recommendation
     * candiddate.
     * @param entityAggregate The entity demand aggregate to process.
     * @return A set of (entity,classification) tuples, each indicating whether it is a recommendation
     * candidate.
     */
    @Nonnull
    public Set<ClassifiedEntitySelection> selectRecommendationDemand(
            @Nonnull ClassifiedEntityDemandAggregate entityAggregate) {

        final boolean passesScopeFilter = scopedEntityFilter.filter(entityAggregate);
        final boolean passesStateFilter = entityStateFilter.filterDemand(entityAggregate);
        if (passesScopeFilter && passesStateFilter) {
            return convertToEntitySelection(
                    entityAggregate, classificationFilter, cloudTierFilter);
        } else {
            logger.trace("Skipping entity for recommendation demand"
                            + "(Entity OID={}, Scoped Entity Filter={}, State Filter={})",
                        entityAggregate.entityOid(), passesScopeFilter, passesStateFilter);

            return convertToEntitySelection(
                    entityAggregate,
                    ClassificationFilter.BLOCKING_FILTER,
                    CloudTierFilter.BLOCKING_FILTER);
        }
    }

    /**
     * Responsible for filtering all demand contained within {@code entityAggregate}, based on the
     * provided classification and cloud tier filters. Any demand passing both classification and
     * cloud tier filters will be marked as a recommendation candidate.
     * @param entityAggregate The entity aggregate to process.
     * @param classificationFilter The classification filter.
     * @param cloudTierFilter The cloud tier filter.
     * @return The set of (entity, cloud tier) tuples
     */
    private Set<ClassifiedEntitySelection> convertToEntitySelection(
            @Nonnull ClassifiedEntityDemandAggregate entityAggregate,
            @Nonnull ClassificationFilter classificationFilter,
            @Nonnull CloudTierFilter cloudTierFilter) {


        final ClassifiedEntitySelection.Builder selectionBuilder = ClassifiedEntitySelection.builder()
                .entityOid(entityAggregate.entityOid())
                .accountOid(entityAggregate.accountOid())
                .billingFamilyId(entityAggregate.billingFamilyId())
                .regionOid(entityAggregate.regionOid())
                .availabilityZoneOid(entityAggregate.availabilityZoneOid())
                .serviceProviderOid(entityAggregate.serviceProviderOid())
                .isSuspended(entityAggregate.isSuspended())
                .isTerminated(entityAggregate.isTerminated());

        return entityAggregate.classifiedCloudTierDemand()
                .entrySet()
                .stream()
                .map(classificationEntry -> {

                    final DemandClassification demandClassification = classificationEntry.getKey();
                    final boolean passesClassificationFilter = classificationFilter.filter(demandClassification);

                    return classificationEntry.getValue()
                            .stream()
                            .map(demandSeries -> selectionBuilder
                                    .classification(demandClassification)
                                    .cloudTierDemand(demandSeries.cloudTierDemand())
                                    .demandTimeline(demandSeries.demandIntervals())
                                    .isRecommendationCandidate(passesClassificationFilter
                                            && cloudTierFilter.filter(demandSeries.cloudTierDemand()))
                                    .build())
                            .collect(Collectors.toList());

                }).flatMap(List::stream)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * A factory class for producing {@link RecommendationSelector} instances.
     */
    public static class RecommendationSelectorFactory {

        private final CloudTierFilterFactory cloudTierFilterFactory;

        private final ScopedEntityFilterFactory scopedEntityFilterFactory;

        private final EntityStateFilterFactory entityStateFilterFactory;

        private final ClassificationFilterFactory classificationFilterFactory;

        /**
         * Constructs a new recommendation selector factory.
         * @param cloudTierFilterFactory A factory for producing cloud tier filters.
         * @param scopedEntityFilterFactory A factory for producing scoped entity filters.
         * @param entityStateFilterFactory A factory fro producing entity state filters.
         * @param classificationFilterFactory A factory for producing demand classification filters.
         */
        public RecommendationSelectorFactory(@Nonnull CloudTierFilterFactory cloudTierFilterFactory,
                                             @Nonnull ScopedEntityFilterFactory scopedEntityFilterFactory,
                                             @Nonnull EntityStateFilterFactory entityStateFilterFactory,
                                             @Nonnull ClassificationFilterFactory classificationFilterFactory) {

            this.cloudTierFilterFactory = Objects.requireNonNull(cloudTierFilterFactory);
            this.scopedEntityFilterFactory = Objects.requireNonNull(scopedEntityFilterFactory);
            this.entityStateFilterFactory = Objects.requireNonNull(entityStateFilterFactory);
            this.classificationFilterFactory = Objects.requireNonNull(classificationFilterFactory);
        }

        /**
         * Creates a new {@link RecommendationSelector}, based on the supplied demand selection config.
         * The config is expected to come from from the purchase profile of the analysis request.
         * @param allocatedSelection The allocated demand selection config.
         * @return A newly created {@link RecommendationSelector} instance.
         */
        @Nonnull
        public RecommendationSelector fromDemandSelection(@Nonnull AllocatedDemandSelection allocatedSelection) {

            final DemandSelection demandSelection = allocatedSelection.getDemandSelection();

            final EntityStateFilter entityStateFilter = entityStateFilterFactory.newFilter(
                    demandSelection.getIncludeSuspendedEntities(),
                    demandSelection.getIncludeTerminatedEntities());
            final ClassificationFilter classificationFilter = classificationFilterFactory.newFilter(
                    allocatedSelection);
            final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(
                    demandSelection.getScope());
            final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(
                    demandSelection.getScope());

            return new RecommendationSelector(
                    scopedEntityFilter,
                    entityStateFilter,
                    cloudTierFilter,
                    classificationFilter);
        }
    }
}
