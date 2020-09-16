package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassificationFilter.ClassificationFilterFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.EntityStateFilter.EntityStateFilterFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandSelection;

/**
 * The analysis selector is responsible for the classification and entity state filtering of demand selection
 * for the analysis. While the entities will be scoped through demand retrieval (as a filter to the
 * demand store), classification and entity state cannot be determined until after the demand is
 * retrieved. Therefore, this selector is responsible for dropping any analysis that was not
 * selected as part of the analysis config.
 */
public class AnalysisSelector {

    private final Logger logger = LogManager.getLogger();

    private final DemandTransformationJournal transformationJournal;

    private final EntityStateFilter entityStateFilter;

    private final ClassificationFilter classificationFilter;

    private AnalysisSelector(@Nonnull DemandTransformationJournal transformationJournal,
                             @Nonnull EntityStateFilter entityStateFilter,
                             @Nonnull ClassificationFilter classificationFilter) {

        this.transformationJournal = Objects.requireNonNull(transformationJournal);
        this.entityStateFilter = Objects.requireNonNull(entityStateFilter);
        this.classificationFilter = Objects.requireNonNull(classificationFilter);
    }

    /**
     * Filters the entity aggregate based on the entity state and demand classification. If the entity
     * does not past the entity state filter (suspension/termination filter), all entity demand will be
     * removed from the returned aggregate. If the entity passes the state filter, any demand classification
     * not passing the classification filter (based on teh requested classifications) will be dropped.
     * @param entityAggregate The entity aggregate to filter.
     * @return The filtered entity aggregate.
     */
    @Nonnull
    public ClassifiedEntityDemandAggregate filterEntityAggregate(
            @Nonnull ClassifiedEntityDemandAggregate entityAggregate) {

        if (entityStateFilter.filterDemand(entityAggregate)) {

            final Map<DemandClassification, Set<DemandTimeSeries>> classifiedDemand =
                    entityAggregate.classifiedCloudTierDemand();
            final Set<DemandClassification> ignoredClassifications = classifiedDemand.keySet()
                            .stream()
                            .filter(Predicates.not(classificationFilter::filter))
                            .collect(Collectors.toSet());

            if (ignoredClassifications.isEmpty()) {
                logger.trace("No ignored classifications. Returning entity (Entity OID={})",
                        entityAggregate.entityOid());
                return entityAggregate;
            } else {

                ignoredClassifications.forEach(classification ->
                        transformationJournal.recordSkippedDemand(entityAggregate,
                                classification,
                                classifiedDemand.getOrDefault(classification, Collections.EMPTY_SET)));

                return ClassifiedEntityDemandAggregate.builder()
                        .from(entityAggregate)
                        .classifiedCloudTierDemand(
                                Maps.filterKeys(
                                        classifiedDemand,
                                        Predicates.not(ignoredClassifications::contains)))
                        .build();

            }
        } else {
            transformationJournal.recordSkippedEntity(entityAggregate);

            return ClassifiedEntityDemandAggregate.builder()
                    .from(entityAggregate)
                    .classifiedCloudTierDemand(Collections.emptyMap())
                    .build();
        }
    }

    /**
     * A factory class for creating {@link AnalysisSelector} instances.
     */
    public static class AnalysisSelectorFactory {

        private final ClassificationFilterFactory classificationFilterFactory;

        private final EntityStateFilterFactory entityStateFilterFactory;

        /**
         * Constructs a new analysis selector factory instance.
         * @param classificationFilterFactory The classification filter factory.
         * @param entityStateFilterFactory The entity state filter factory.
         */
        public AnalysisSelectorFactory(@Nonnull ClassificationFilterFactory classificationFilterFactory,
                                       @Nonnull EntityStateFilterFactory entityStateFilterFactory) {
            this.classificationFilterFactory = Objects.requireNonNull(classificationFilterFactory);
            this.entityStateFilterFactory = Objects.requireNonNull(entityStateFilterFactory);
        }


        /**
         * Constructs a new analysis selector, based on the allocated demand selection from the
         * analysis config request.
         * @param transformationJournal The transformation journal, used to record any skipped/dropped
         *                              demand.
         * @param allocatedSelection The allocated demand selection config,
         * @return The newly created analysis selector instance.
         */
        public AnalysisSelector newSelector(@Nonnull DemandTransformationJournal transformationJournal,
                                            @Nonnull AllocatedDemandSelection allocatedSelection) {

            final DemandSelection demandSelection = allocatedSelection.getDemandSelection();
            final EntityStateFilter entityStateFilter = entityStateFilterFactory.newFilter(
                    demandSelection.getIncludeSuspendedEntities(),
                    demandSelection.getIncludeTerminatedEntities());
            final ClassificationFilter classificationFilter =
                    classificationFilterFactory.newFilter(allocatedSelection);

            return new AnalysisSelector(transformationJournal, entityStateFilter, classificationFilter);
        }

    }
}
