package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.ProjectedDemandClassification;

/**
 * A wrapper class for {@link AllocatedDemandClassification} and {@link ProjectedDemandClassification}.
 * This wrapper allows data classes to contain a single classification attribute, instead of requiring
 * generics and qualification of each data type. An instance of this class may contain exactly one of
 * either an allocated or projected classification (not both).
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable(lazyhash = true)
public interface DemandClassification {

    /**
     * An enum for the classification type (either allocated or projected).
     */
    enum DemandClassificationType {
        ALLOCATED_CLASSIFICATION,
        PROJECTED_CLASSIFICATION
    }

    /**
     * The allocated classification. It may be null, if the type of this classification is
     * {@link DemandClassificationType#PROJECTED_CLASSIFICATION}.
     * @return The allocated classification. It may be null, if the type of this classification is
     * {@link DemandClassificationType#PROJECTED_CLASSIFICATION}.
     */
    @Nullable
    AllocatedDemandClassification allocatedClassification();

    /**
     * The projected classification. It may be null, if the type of this classification is
     * {@link DemandClassificationType#ALLOCATED_CLASSIFICATION}.
     * @return The projected classification. It may be null, if the type of this classification is
     * {@link DemandClassificationType#ALLOCATED_CLASSIFICATION}.
     */
    @Nullable
    ProjectedDemandClassification projectedClassification();

    /**
     * Checks whether this classification wrap contains an allocated classification.
     * @return True, if the allocated classification is set. False, otherwise.
     */
    @Derived
    default boolean hasAllocatedClassification() {
        return allocatedClassification() != null;
    }

    /**
     * Checks whether this classification wrap contains a projected classification.
     * @return True, if the projected classification is set. False, otherwise.
     */
    @Derived
    default boolean hasProjectedClassification() {
        return projectedClassification() != null;
    }

    /**
     * Returns the classification type of this wrapper.
     * @return The classification type of this wrapper.
     */
    @Derived
    @Nonnull
    default DemandClassificationType type() {
        if (hasAllocatedClassification()) {
            return DemandClassificationType.ALLOCATED_CLASSIFICATION;
        } else if (hasProjectedClassification()) {
            return DemandClassificationType.PROJECTED_CLASSIFICATION;
        } else {
            throw new IllegalStateException("Either allocated or projected classification must be set");
        }
    }

    /**
     * Validates that either the allocated or projected classification attributes of this wrapper
     * are set (but not both).
     */
    @Check
    default void validate() {
        Preconditions.checkArgument(
                allocatedClassification() != null ^ projectedClassification() != null);
    }

    /**
     * Constructs a classification wrapper from an allocated classification.
     * @param classification The allocated classification.
     * @return The newly created classification wrapper instance.
     */
    @Nonnull
    static DemandClassification of(@Nonnull AllocatedDemandClassification classification) {
        return DemandClassification.builder()
                .allocatedClassification(classification)
                .build();
    }

    /**
     * Constructs a classification wrapper from a projected classification.
     * @param classification The projected classification.
     * @return The newly created classification wrapper instance.
     */
    @Nonnull
    static DemandClassification of(@Nonnull ProjectedDemandClassification classification) {
        return DemandClassification.builder()
                .projectedClassification(classification)
                .build();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return A newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link DemandClassification}.
     */
    class Builder extends ImmutableDemandClassification.Builder {}
}
