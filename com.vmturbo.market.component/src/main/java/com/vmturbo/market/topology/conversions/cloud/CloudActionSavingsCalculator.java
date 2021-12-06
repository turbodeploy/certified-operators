package com.vmturbo.market.topology.conversions.cloud;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Lazy;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrBuilder;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.utils.FuzzyDouble;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * Calculates cloud savings for an action.
 */
public interface CloudActionSavingsCalculator {

    /**
     * Calculates the action savings for {@code action}. If action is a builder instance, the builder
     * is expected to be initialized with all data required for calculating action savings (e.g. source
     * and destination for a move action).
     * @param action The target action.
     * @return The calculated action savings.
     */
    @Nonnull
    CalculatedSavings calculateSavings(@Nonnull ActionOrBuilder action);

    /**
     * The calculated savings (or investment) for an action, including savings details, if applicable
     * to the action type.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface CalculatedSavings {

        /**
         * A {@link CalculatedSavings} with no savings, in USD.
         */
        CalculatedSavings NO_SAVINGS_USD = CalculatedSavings.builder()
                .savingsPerHour(Trax.trax(0.0))
                .build();

        /**
         * The hourly savings for the action. The savings value may not be present, if the action
         * does not involve cloud entitites or if the action or topology data is malformed.
         * @return The savings per hour.
         */
        @Nonnull
        Optional<TraxNumber> savingsPerHour();

        /**
         * The savings details (e.g. source and destination rates) for the action. Savings details
         * are present for a subset of actions.
         * @return The action savings details.
         */
        @Nonnull
        Optional<TraxSavingsDetails> cloudSavingsDetails();

        /**
         * Applies this {@link CalculatedSavings} to the provided action builder.
         * @param action The action builder to initialize.
         * @return The input {@code action} for method chaining.
         */
        @Nonnull
        default Action.Builder applyToActionBuilder(@Nonnull Action.Builder action) {

            savingsPerHour().map(savings ->
                    CurrencyAmount.newBuilder()
                            .setAmount(savings.getValue())
                            .build()).ifPresent(action::setSavingsPerHour);

            // Move actions (from MPC) may have cloud savings details. However, the DTO
            // currently does not have an attribute to store it and therefore we drop it.
            cloudSavingsDetails().map(TraxSavingsDetails::toCloudSavingsDetails)
                    .ifPresent(savingsDetails -> {
                        final ActionInfo.Builder actionInfo = action.getInfoBuilder();
                        if (actionInfo.hasScale()) {
                            actionInfo.getScaleBuilder().setCloudSavingsDetails(savingsDetails);
                        } else if (actionInfo.hasAllocate()) {
                            actionInfo.getAllocateBuilder().setCloudSavingsDetails(savingsDetails);
                        } else if (actionInfo.hasMove()) {
                            actionInfo.getMoveBuilder().setCloudSavingsDetails(savingsDetails);
                        }
                    });

            return action;
        }

        /**
         * Applies this {@link CalculatedSavings} instance the provided {code action}, building a new
         * action instance and returning it.
         * @param action The {@link Action} to update.
         * @return A new {@link Action} instance, with the calculated savings applied.
         */
        @Nonnull
        default Action applyToAction(@Nonnull Action action) {
            return applyToActionBuilder(action.toBuilder()).build();
        }

        /**
         * Returns true if {@link #savingsPerHour()} is zero or not present.
         * @return True if {@link #savingsPerHour()} is zero or not present.
         */
        @Lazy
        default boolean isZeroSavings() {
            return savingsPerHour()
                    .map(savings -> FuzzyDouble.newFuzzy(savings.getValue()).isZero())
                    .orElse(true);
        }

        /**
         * Returns true if {@link #savingsPerHour()} is positive, indicating it is generating a savings
         * over the source (current) cost. Returns false if {@link #savingsPerHour()} is not present.
         * @return True if {@link #savingsPerHour()} is positive, indicating it is generating a savings
         * over the source (current) cost
         */
        @Lazy
        default boolean isSavings() {
            return savingsPerHour()
                    .map(savings -> FuzzyDouble.newFuzzy(savings.getValue()).isPositive())
                    .orElse(false);
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
         * A builder class for constructing {@link CalculatedSavings} instances.
         */
        class Builder extends ImmutableCalculatedSavings.Builder {}
    }

    /**
     * An implementation of {@link CloudSavingsDetails}, utilizing {@link TraxNumber} to track
     * the computation of savings values.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface TraxSavingsDetails {

        /**
         * The source tier cost details.
         * @return The source tier cost details.
         */
        @Default
        @Nonnull
        default TraxTierCostDetails sourceTierCostDetails() {
            return TraxTierCostDetails.EMPTY_DETAILS;
        }

        /**
         * The projected tier cost details.
         * @return The projected tier cost details.
         */
        @Default
        @Nonnull
        default TraxTierCostDetails projectedTierCostDetails() {
            return TraxTierCostDetails.EMPTY_DETAILS;
        }

        /**
         * The entity uptime.
         * @return The entity uptime.
         */
        @Nonnull
        Optional<EntityUptimeDTO> entityUptime();

        /**
         * Converts this {@link TraxSavingsDetails} to {@link CloudSavingsDetails}.
         * @return The constructed {@link CloudSavingsDetails} instance.
         */
        @Nonnull
        default CloudSavingsDetails toCloudSavingsDetails() {
            final CloudSavingsDetails.Builder savingsDetails = CloudSavingsDetails.newBuilder()
                    .setSourceTierCostDetails(sourceTierCostDetails().toTierCostDetails())
                    .setProjectedTierCostDetails(projectedTierCostDetails().toTierCostDetails());

            entityUptime().ifPresent(savingsDetails::setEntityUptime);

            return savingsDetails.build();
        }

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link TraxSavingsDetails} instances.
         */
        class Builder extends ImmutableTraxSavingsDetails.Builder {}

        /**
         * An implementation of {@link TierCostDetails}, utilizating {@link TraxNumber} to track
         * cost values.
         */
        @HiddenImmutableImplementation
        @Immutable
        interface TraxTierCostDetails {

            /**
             * An empty cost details. On-demand rate and cost will be zero.
             */
            TraxTierCostDetails EMPTY_DETAILS = TraxTierCostDetails.builder().build();

            /**
             * The on-demand rate.
             * @return The on-demand rate.
             */
            @Default
            @Nonnull
            default TraxNumber onDemandRate() {
                return Trax.trax(0.0);
            }

            /**
             * The on-demand cost.
             * @return The on-demand cost.
             */
            @Default
            @Nonnull
            default TraxNumber onDemandCost() {
                return Trax.trax(0.0);
            }

            /**
             * The cloud commitment coverage.
             * @return The cloud commitment coverage.
             */
            @Nonnull
            Optional<CloudCommitmentCoverage> cloudCommitmentCoverage();

            /**
             * The cost journal.
             * @return The cost journal.
             */
            @Nonnull
            Optional<CostJournal<TopologyEntityDTO>> costJournal();

            /**
             * Converts this {@link TraxTierCostDetails} to {@link TierCostDetails}.
             * @return The constructed {@link TierCostDetails} instance.
             */
            @Nonnull
            default TierCostDetails toTierCostDetails() {

                final TierCostDetails.Builder tierCostDetails = TierCostDetails.newBuilder()
                        .setOnDemandRate(CurrencyAmount.newBuilder().setAmount(onDemandRate().getValue()))
                        .setOnDemandCost(CurrencyAmount.newBuilder().setAmount(onDemandCost().getValue()));

                cloudCommitmentCoverage().ifPresent(tierCostDetails::setCloudCommitmentCoverage);

                return tierCostDetails.build();
            }

            /**
             * Constructs and returns a new {@link Builder} instance.
             * @return The newly constructed {@link Builder} instance.
             */
            @Nonnull
            static Builder builder() {
                return new Builder();
            }

            /**
             * A builder class for constructing {@link TraxTierCostDetails} instances.
             */
            class Builder extends ImmutableTraxTierCostDetails.Builder {}
        }
    }
}
