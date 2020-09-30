package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;

/**
 * An implementation of {@link DemandTransformer} for transforming flexible compute tier demand based on
 * RI recommendations. The transformation will determine the "conservative" demand to consider for the
 * analysis, choosing the smallest demand between the flexible tier and the associated entity's allocated
 * tier.
 */
public class FlexibleRIComputeTransformer implements DemandTransformer {

    private static final DemandClassification FLEXIBLY_ALLOCATED_CLASSIFICATION =
            DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED);

    private final Logger logger = LogManager.getLogger();

    private final DemandTransformationJournal demandTransformationJournal;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private FlexibleRIComputeTransformer(@Nonnull DemandTransformationJournal demandTransformationJournal,
                                         @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver) {

        this.demandTransformationJournal = Objects.requireNonNull(demandTransformationJournal);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public ClassifiedEntityDemandAggregate transformDemand(
            @Nonnull final ClassifiedEntityDemandAggregate entityAggregate) {

        return entityAggregate.allocatedCloudTierDemand()
                .map(allocatedDemand -> {

                    final CloudTierDemand allocatedTier = allocatedDemand.cloudTierDemand();

                    final Set<DemandTimeSeries> flexibleAllocations =
                            entityAggregate.classifiedCloudTierDemand().getOrDefault(
                                    FLEXIBLY_ALLOCATED_CLASSIFICATION,
                                    Collections.emptySet());
                    if (!flexibleAllocations.isEmpty()) {

                        final Map<DemandClassification, Set<DemandTimeSeries>> classifiedDemandUpdate =
                                Maps.newHashMap(entityAggregate.classifiedCloudTierDemand());

                        classifiedDemandUpdate.put(
                                FLEXIBLY_ALLOCATED_CLASSIFICATION,
                                flexibleAllocations.stream()
                                        .map(demandSeries -> transformFlexibleDemand(demandSeries, allocatedTier))
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toSet()));

                        return ClassifiedEntityDemandAggregate.builder()
                                .from(entityAggregate)
                                .classifiedCloudTierDemand(classifiedDemandUpdate)
                                .build();
                    } else {
                        return entityAggregate;
                    }
                }).orElseGet(() -> {
                    logger.debug("Ignoring entity without allocation (Entity OID={})",
                            entityAggregate.entityOid());
                    return entityAggregate;
                });
    }

    @Nullable
    private DemandTimeSeries transformFlexibleDemand(@Nonnull DemandTimeSeries demandSeries,
                                                     @Nonnull CloudTierDemand allocatedTier) {

        final long allocatedOid = allocatedTier.cloudTierOid();
        final long flexibleOid = demandSeries.cloudTierDemand().cloudTierOid();
        try {

            return (computeTierFamilyResolver.compareTiers(flexibleOid, allocatedOid) <= 0)
                    ? demandSeries
                    : DemandTimeSeries.builder()
                        .from(demandSeries)
                        .cloudTierDemand(allocatedTier)
                        .build();
        } catch (Exception e) {
            logger.error("Error transforming flexible demand (Allocated OID={}, Flexible OID={})",
                    allocatedOid, flexibleOid, e);
            return null;
        }
    }

    /**
     * A factory class for producing {@link FlexibleRIComputeTransformer} instances.
     */
    public static class FlexibleRIComputeTransformerFactory {

        /**
         * Creates and returns a new {@link FlexibleRIComputeTransformer} instance.
         * @param demandTransformationJournal The demand transformation journal, used to record
         *                                    transformations of flexible demand.
         * @param computeTierFamilyResolver The compute tier resolver, used for transforming
         *                                  flexible demand.
         * @return THe newly created {@link FlexibleRIComputeTransformer} instance.
         */
        @Nonnull
        public FlexibleRIComputeTransformer newTransformer(
                @Nonnull DemandTransformationJournal demandTransformationJournal,
                @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver) {

            return new FlexibleRIComputeTransformer(
                    demandTransformationJournal,
                    computeTierFamilyResolver);
        }
    }
}
