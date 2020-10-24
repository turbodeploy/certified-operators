package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.calculator;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Modifiable;
import org.immutables.value.Value.Style;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudTierPricingData;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.cloud.common.data.TimeSeriesData;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * the input to a cloud commitment savings calculation, containing the relevant rates and demand. All
 * demand contained within the context is expected to be in scope of the recommended commitment.
 */
@HiddenImmutableImplementation
@Immutable
public interface SavingsCalculationContext {

    /**
     * The hourly (or really whatever time interval is represented by {@link DemandSegment}) amortized
     * rate of the cloud commitment.
     * @return The hourly amortized rate of the cloud commitment spec to recommend.
     */
    double amortizedCommitmentRate();

    /**
     * A series of demand, recorded across discrete time intervals (defaults to hourly intervals).
     * @return A set of demand segments, sorted by timestamp.
     */
    @Nonnull
    TimeSeries<DemandSegment> demandSegments();

    /**
     * A human-readable tag, used for logging purposes.
     * @return A human-readable tag, used for logging purposes.
     */
    @Default
    @Nonnull
    default String tag() {
        return StringUtils.EMPTY;
    }

    /**
     * Converts this {@link SavingsCalculationContext} to a {@link Builder} instance.
     * @return The builder instance, copied from this context instance.
     */
    @Nonnull
    default Builder toBuilder() {
        return SavingsCalculationContext.builder().from(this);
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link SavingsCalculationContext} instances.
     */
    class Builder extends ImmutableSavingsCalculationContext.Builder {}

    /**
     * Relevant info about cloud tier demand contained with {@link AggregateDemand} instances.
     */
    @HiddenImmutableImplementation
    @Immutable(lazyhash = true)
    interface CloudTierDemandInfo {

        /**
         * The cloud tier demand.
         * @return The cloud tier demand.
         */
        @Nonnull
        CloudTierDemand cloudTierDemand();

        /**
         * The cloud tier pricing data, used to derived the {@link #normalizedOnDemandRate()}.
         * @return The cloud tier pricing data, used to derived the {@link #normalizedOnDemandRate()}.
         */
        @Nonnull
        CloudTierPricingData tierPricingData();

        /**
         * The normalized on-demand rate to use in calculating savings over on-demand for potential
         * recommendations.
         * @return the normalized on-demand rate.
         */
        double normalizedOnDemandRate();

        double demandCommitmentRatio();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         *  A builder class for constructing {@link CloudTierDemandInfo} instances.
         */
        class Builder extends ImmutableCloudTierDemandInfo.Builder {}
    }

    /**
     * Represents normalized cloud tier demand. While the normalization unit will be dependent on the
     * cloud commitment type, it is expected that all {@link AggregateDemand} instances within a
     * {@link SavingsCalculationContext} have the same unit and are relative to each other.
     */
    @Style(typeModifiable = "Mutable*")
    @Modifiable
    @HiddenImmutableImplementation
    @Immutable
    interface AggregateDemand {

        /**
         * The total demand.
         * @return The total demand.
         */
        double totalDemand();

        /**
         * The coverage percentage from the cloud commitment inventory. This demand is sent to the savings calculator
         * in order to correctly calculate the maximum coverage threshold.
         * @return The coverage percentage from the cloud commitment inventory.
         */
        double inventoryCoverage();

        /**
         * The reserved demand, representing demand above the maximum coverage threshold that cannot
         * be covered by a potential recommendation. Reserved demand allows the calculator to generate
         * recommendations assuming some maximum coverage percentage.
         * @return The reserved demand amount.
         */
        @Default
        default double reservedDemand() {
            return 0.0;
        }

        /**
         * Demand that may be covered by a potential recommendation.
         * @return Demand that may be covered by a potential recommendation.
         */
        @Derived
        default double coverableDemand() {
            return Math.max(totalDemand() - (inventoryCoverage() + reservedDemand()), 0.0);
        }

        /**
         * The total uncovered demand, after {@link #inventoryCoverage()} is subtracted. This value
         * will include {@link #reservedDemand()}.
         * @return The total uncovered demand, after subtracting cloud commitment inventory coverage.
         */
        @Derived
        default double uncoveredDemand() {
            return Math.max(totalDemand() - inventoryCoverage(), 0.0);
        }

        /**
         * Converts this immutable {@link AggregateDemand} instance to a mutable {@link MutableAggregateDemand}
         * instance.
         * @return The newly constructed {@link MutableAggregateDemand} instance.
         */
        @Nonnull
        default MutableAggregateDemand asMutable() {
            return MutableAggregateDemand.create().from(this);
        }

        /**
         * Creates a new {@link Builder} instance, copied from this {@link AggregateDemand} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        default Builder toBuilder() {
            return AggregateDemand.builder().from(this);
        }

        /**
         * Combines this aggregate demand with {@code otherAggregate}.
         * @param otherAggregate The other {@link AggregateDemand} to combine.
         * @return A new {@link AggregateDemand} instance, representing the combination of this
         * instance with {@code otherAggregate}.
         */
        @Nonnull
        default AggregateDemand combine(@Nonnull AggregateDemand otherAggregate) {

            Preconditions.checkNotNull(otherAggregate);

            return AggregateDemand.builder()
                    .totalDemand(this.totalDemand() + otherAggregate.totalDemand())
                    .inventoryCoverage(this.inventoryCoverage() + otherAggregate.inventoryCoverage())
                    .reservedDemand(this.reservedDemand() + otherAggregate.reservedDemand())
                    .build();
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link AggregateDemand} instances.
         */
        class Builder extends ImmutableAggregateDemand.Builder {}
    }

    /**
     * A segment (time interval) of demand. A segment delineates when unused cloud commitment capacity
     * expires. This typically will be an hour interval, as RIs/Savings Plans are charged at a
     * use-it-or-lose-it rate on an hourly basis.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface DemandSegment extends TimeSeriesData {

        /**
         * A map containing cloud tier info to the associated demand amount. All demand contained
         * within the map is assumed to be within scope of the cloud commitment recommendation candidate.
         * @return An immutable map of {@link CloudTierDemandInfo} to {@link AggregateDemand}.
         */
        @Auxiliary
        Map<CloudTierDemandInfo, AggregateDemand> tierDemandMap();

        /**
         * The total demand contained within this demand segment. The unit will be based on the
         * type of cloud commitment recommended (e.g. for an RI, it will be coupons).
         * @return The total demand contained within this demand segment.
         */
        @Auxiliary
        @Derived
        default double totalDemand() {
            return tierDemandMap().values()
                    .stream()
                    .map(AggregateDemand::totalDemand)
                    .reduce(0.0, Double::sum);
        }

        /**
         * Converts this {@link DemandSegment} instance to a {@link Builder}.
         * @return The builder instance.
         */
        default Builder toBuilder() {
            return DemandSegment.builder().from(this);
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link DemandSegment} instances.
         */
        class Builder extends ImmutableDemandSegment.Builder {}
    }
}
