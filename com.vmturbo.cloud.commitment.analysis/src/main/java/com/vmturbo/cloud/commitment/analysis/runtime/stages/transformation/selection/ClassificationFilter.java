package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;

/**
 *  A filter for {@link DemandClassification} instances.
 */
public class ClassificationFilter {

    /**
     * A {@link ClassificationFilter} that will reject all classifications.
     */
    public static final ClassificationFilter BLOCKING_FILTER =
            new ClassificationFilter(Collections.emptySet());

    private final Set<DemandClassification> allowedClassifications;

    private ClassificationFilter(@Nonnull Set<DemandClassification> allowedClassifications) {
        this.allowedClassifications = ImmutableSet.copyOf(Objects.requireNonNull(allowedClassifications));
    }

    /**
     * Filters the {@code classification}, based on a classification white list.
     * @param classification The classification to filter.
     * @return True, if the classification passes the white list or if the white list is empty. False otherwise.
     */
    public boolean filter(@Nonnull DemandClassification classification) {
        return allowedClassifications.contains(classification);
    }

    /**
     * A filter class for {@link ClassificationFilter} instances.
     */
    public static class ClassificationFilterFactory {

        /**
         * Constructs a new {@link ClassificationFilter} instance, based on the {code allocatedSelection}
         * config. The classification filter will always allow allocated demand. Flexibly allocated
         * demand will be conditional based on the selection config.
         * @param allocatedSelection The allocated demand selection config.
         * @return The newly created {@link ClassificationFilter}.
         */
        @Nonnull
        public ClassificationFilter newFilter(AllocatedDemandSelection allocatedSelection) {

            final Set<DemandClassification> classifications = Sets.newHashSet(
                    DemandClassification.of(AllocatedDemandClassification.ALLOCATED));

            if (allocatedSelection.getIncludeFlexibleDemand()) {
                classifications.add(DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED));
            }

            return new ClassificationFilter(classifications);
        }
    }
}
