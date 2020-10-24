package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Represents an aggregate of {@link com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping},
 * grouping all demand across entities with the same scope, demand, and classification.
 */
@HiddenImmutableImplementation
@Immutable(lazyhash = true)
public interface AggregateCloudTierDemand {

    /**
     * The cloud tier information, including scope info.
     * @return The cloud tier information, including scope info.
     */
    @Nonnull
    ScopedCloudTierInfo cloudTierInfo();

    /**
     * A map of the demand amount (e.g. hours) by entity.
     * @return An immutable map of the demand amount by {@link EntityInfo}.
     */
    @Auxiliary
    @Nonnull
    Map<EntityInfo, Double> demandByEntity();

    /**
     * The classification of this demand.
     * @return The classification of this demand.
     */
    @Nonnull
    DemandClassification classification();

    /**
     * The amount of this demand. Represents a normalization of demand for entities which may have
     * differing time intervals. The normalization will be based on the time window of the aggregate.
     * For example, if the time window is over a specific hour and each of 3 VMs are up for half an
     * hour within that time window, the demand amount will equal 1.5.
     * @return The amount of this demand, normalized to the parent time window of this aggregate. The
     * demand amount is not relative to the demand type/size.
     */
    @Auxiliary
    @Derived
    @Nonnull
    default double demandAmount() {
        return demandByEntity().values()
                .stream()
                .mapToDouble(Double::valueOf)
                .sum();
    }

    /**
     * Whether the demand contained within this aggregate can be considered in justifying a
     * purchase recommendation. If false, the contained demand should be used for uncovered demand
     * analysis only.
     * @return True, if the contained demand can be used to justify a purchase recommendation.
     */
    @Default
    default boolean isRecommendationCandidate() {
        return false;
    }

    /**
     * The coverage info, representing the output of the uncovered demand calculation.
     * @return The coverage info, representing the output of the uncovered demand calculation.
     */
    @Auxiliary
    @Default
    default Set<CoverageInfo> coverageInfo() {
        return Collections.emptySet();
    }

    /**
     * The coverage amount, as expressed in the same unit as demand.
     * @return The coverage amount.
     */
    @Auxiliary
    @Derived
    default double coverageAmount() {
        return coverageInfo().stream()
                .mapToDouble(CoverageInfo::coverageAmount)
                .sum();
    }

    /**
     * Converts this {@link AggregateCloudTierDemand} instance to a {@link Builder}.
     * @return A builder instance based on this aggregate demand instance.
     */
    default Builder toBuilder() {
        return AggregateCloudTierDemand.builder().from(this);
    }

    /**
     * Constructs and returns a new builder instance.
     * @return A newly constructed builder instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link AggregateCloudTierDemand}.
     */
    class Builder extends ImmutableAggregateCloudTierDemand.Builder {}


    /**
     * A data class containing relevant entity information for the aggregate demand contained
     * within {@link AggregateCloudTierDemand}.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface EntityInfo {

        /**
         * The entity OID.
         * @return The entity OID.
         */
        long entityOid();

        /**
         * Indicates whether the entity is currently considered suspended. Suspended entities are those
         * entities that exist in cloud topology and are not powered on.
         * @return Whether the entity is currently suspended.
         */
        @Default
        default boolean isSuspended() {
            return false;
        }

        /**
         * Indicates whether the entity is currently considered terminated. Terminated entities are those
         * entities which do not exist in the cloud topology.
         * @return Whether the entity is terminated.
         */
        @Default
        default boolean isTerminated() {
            return false;
        }

        /**
         * Constructs and returns a new builder instance.
         * @return A newly constructed builder instance.
         */
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link EntityInfo}.
         */
        class Builder extends ImmutableEntityInfo.Builder {}
    }

    /**
     * A data class representing demand coverage from a {@link CloudCommitmentAggregate}.
     */
    @HiddenImmutableImplementation
    @Immutable(lazyhash = true)
    interface CoverageInfo {

        /**
         * The cloud commitment aggregate providing coverage. Only the aggregate ID is used as a uniquely
         * identifying attribute of the {@link CoverageInfo}.
         * @return The cloud commitment aggregate.
         */
        @Nonnull
        @Auxiliary
        CloudCommitmentAggregate cloudCommitmentAggregate();

        /**
         * The coverage amount provided by the {@link #cloudCommitmentAggregate()}. The unit of the
         * coverage amount may vary depending on the cloud commitment type (e.g. for RIs, it will be
         * coupons).
         * @return The coverage amount.
         */
        double coverageAmount();

        /**
         * The {@link #cloudCommitmentAggregate()} ID.
         * @return The {@link #cloudCommitmentAggregate()} ID.
         */
        @Derived
        default long aggregateId() {
            return cloudCommitmentAggregate().aggregateId();
        }

        /**
         * Creates and returns a {@link Builder} instance.
         * @return A newly created builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link CoverageInfo} instances.
         */
        class Builder extends ImmutableCoverageInfo.Builder {}
    }
}
