package com.vmturbo.cloud.common.commitment;

import static com.vmturbo.cloud.common.commitment.CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT;

import java.util.Collection;
import java.util.Map;
import java.util.function.BinaryOperator;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount.ValueCase;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A calculator for performing mathematical operations on instances of {@link CloudCommitmentAmount}.
 */
public class CommitmentAmountCalculator {

    /**
     * Zero coverage CloudCommitmentAmount.
     */
    public static final CloudCommitmentAmount ZERO_COVERAGE = CloudCommitmentAmount.newBuilder().build();

    private CommitmentAmountCalculator() {}

    /**
     * Subtracts {@code subtrahendAmount} from {@code minuendAmount}. {@link CloudCommitmentAmount} instances
     * that are not set are treated as zero.
     * @param minuendAmount The minuend amount.
     * @param subtrahendAmount The subtrahend amount.
     * @return The results of subtracting {@code subtrahendAmount} from {@code minuendAmount}.
     */
    @Nonnull
    public static CloudCommitmentAmount subtract(@Nonnull CloudCommitmentAmount minuendAmount,
                                                 @Nonnull CloudCommitmentAmount subtrahendAmount) {

        validateTypeCompatibility(minuendAmount, subtrahendAmount);

        switch (minuendAmount.getValueCase()) {

            case AMOUNT:
                return subtrahendAmount.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? CloudCommitmentAmount.newBuilder()
                                .setAmount(subtractCurrencyAmount(minuendAmount.getAmount(), subtrahendAmount.getAmount()))
                                .build()
                        : minuendAmount;
            case COUPONS:
                return subtrahendAmount.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? CloudCommitmentAmount.newBuilder()
                                .setCoupons(minuendAmount.getCoupons() - subtrahendAmount.getCoupons())
                                .build()
                        : minuendAmount;
            case VALUE_NOT_SET:
                return negate(subtrahendAmount);
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", minuendAmount.getValueCase()));

        }

    }

    /**
     * Sums all {@link CloudCommitmentAmount} instances in {@code amounts}. The amounts must all be
     * of the same coverage type or not set.
     * @param amounts The {@link CloudCommitmentAmount} instances to sum.
     * @return The sum of the {@code amounts}.
     */
    @Nonnull
    public static CloudCommitmentAmount sum(@Nonnull Collection<CloudCommitmentAmount> amounts) {

        return amounts.stream()
                .reduce(EMPTY_COMMITMENT_AMOUNT, CommitmentAmountCalculator::sum);
    }

    /**
     * Sums {@code amountA} and {@code amountB}.
     * @param amountA The first commitment amount.
     * @param amountB The second commitment amount.
     * @return The sum of {@code amountA} and {@code amountB}.
     */
    @Nonnull
    public static CloudCommitmentAmount sum(@Nonnull CloudCommitmentAmount amountA,
                                            @Nonnull CloudCommitmentAmount amountB) {

        validateTypeCompatibility(amountA, amountB);

        switch (amountA.getValueCase()) {

            case AMOUNT:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? CloudCommitmentAmount.newBuilder()
                                .setAmount(opCurrencyAmount(amountA.getAmount(), amountB.getAmount(), Double::sum))
                                .build()
                        : amountA;
            case COUPONS:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? CloudCommitmentAmount.newBuilder()
                                .setCoupons(amountA.getCoupons() + amountB.getCoupons())
                                .build()
                        : amountA;
            case VALUE_NOT_SET:
                return amountB;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", amountA.getValueCase()));

        }
    }

    /**
     * Subtracts {@code subtrahendAmount} from {@code minuendAmount}. The {@link CurrencyAmount} instances
     * must be of the same currency.
     * @param minuendAmount The minuend amount.
     * @param subtrahendAmount The subtrahend amount.
     * @return The result of subtracting {@code subtrahendAmount} from {@code minuendAmount}.
     */
    @Nonnull
    public static CurrencyAmount subtractCurrencyAmount(@Nonnull CurrencyAmount minuendAmount,
                                                        @Nonnull CurrencyAmount subtrahendAmount) {

        return opCurrencyAmount(minuendAmount, subtrahendAmount, (a, b) -> a - b);
    }

    /**
     * Negates {@code amount}.
     * @param amount The commitment amount.
     * @return The negated value of {@code amount}.
     */
    @Nonnull
    public static CloudCommitmentAmount negate(@Nonnull CloudCommitmentAmount amount) {

        Preconditions.checkNotNull(amount, "Amount cannot be null");

        switch (amount.getValueCase()) {
            case AMOUNT:
                return amount.toBuilder()
                        .setAmount(amount.getAmount()
                                .toBuilder()
                                .setAmount(-amount.getAmount().getAmount()))
                        .build();
            case COUPONS:
                return amount.toBuilder()
                        .setCoupons(-amount.getCoupons())
                        .build();

            case VALUE_NOT_SET:
                return amount;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", amount.getValueCase()));
        }
    }

    /**
     * Multiplies the {@code commitmentAmount} by {@code multiplier}.
     * @param commitmentAmount The commitment amount.
     * @param multiplier The multiplier.
     * @return The result of the multiplication.
     */
    @Nonnull
    public static CloudCommitmentAmount multiplyAmount(@Nonnull CloudCommitmentAmount commitmentAmount,
                                                       double multiplier) {


        final CloudCommitmentAmount.Builder productAmount = commitmentAmount.toBuilder();
        switch (commitmentAmount.getValueCase()) {
            case AMOUNT:
                productAmount.getAmountBuilder()
                        .setAmount(commitmentAmount.getAmount().getAmount() * multiplier);
                break;
            case COUPONS:
                productAmount.setCoupons(commitmentAmount.getCoupons() * multiplier);
                break;
            case VALUE_NOT_SET:
                // Noop
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", commitmentAmount.getValueCase()));
        }

        return productAmount.build();
    }

    private static void validateTypeCompatibility(@Nonnull CloudCommitmentAmount amountA,
                                                  @Nonnull CloudCommitmentAmount amountB) {

        final boolean valuesSet = amountA.getValueCase() != ValueCase.VALUE_NOT_SET
                && amountB.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(!valuesSet || amountA.getValueCase() == amountB.getValueCase(),
                "Commitment amounts must be of the same type");
    }

    @Nonnull
    private static CurrencyAmount opCurrencyAmount(@Nonnull CurrencyAmount amountA,
                                                   @Nonnull CurrencyAmount amountB,
                                                   BinaryOperator<Double> operator) {

        Preconditions.checkArgument(
                amountA.hasCurrency() == amountB.hasCurrency()
                        && (!amountA.hasCurrency() || amountA.getCurrency() == amountB.getCurrency()),
                "Currencies must match");


        return amountA.toBuilder()
                .setAmount(operator.apply(amountA.getAmount(), amountB.getAmount()))
                .build();
    }

    /**
     * Checks if amountA has resources which is strictly greater than the amountB.
     * amountB might have resources that amountA does not have.
     *
     * @param amountA the first commitment amount
     * @param amountB the second commitment amount
     * @param epsilon the epsilon used for comparison
     * @return true if amountA is a strict super set of amountB
     */
    public static boolean isStrictSuperSet(CloudCommitmentAmount amountA,
            CloudCommitmentAmount amountB, double epsilon) {
        final boolean valuesSet = amountA.getValueCase() != ValueCase.VALUE_NOT_SET
                && amountB.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(!valuesSet || amountA.getValueCase() == amountB.getValueCase(),
                "Commitment amounts must be of the same type");
        switch (amountA.getValueCase()) {
            case AMOUNT:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? amountA.getAmount().getAmount() - amountB.getAmount().getAmount() > epsilon
                        : true;
            case COUPONS:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? amountA.getCoupons() - amountB.getCoupons() > epsilon : true;
            case COMMODITIES_BOUGHT:
                if (amountB.getValueCase() == ValueCase.VALUE_NOT_SET) {
                    return true;
                }
                final Map<CommodityType, Double> commodityMapB =
                        amountB.getCommoditiesBought().getCommodityList().stream().collect(
                                ImmutableMap.toImmutableMap(
                                        CommittedCommodityBought::getCommodityType,
                                        CommittedCommodityBought::getCapacity));

                for (CommittedCommodityBought commInFirst : amountA.getCommoditiesBought()
                        .getCommodityList()) {
                    Double capacityInSecond = commodityMapB.get(commInFirst.getCommodityType());
                    // one commodity is lessthanequal or absent
                    if (capacityInSecond == null
                            || commInFirst.getCapacity() - capacityInSecond < epsilon) {
                        return false;
                    }
                }
                return true;
            case VALUE_NOT_SET:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                amountA.getValueCase()));
        }
    }

    /**
     * divide two cloud commitment amount.
     *
     * @param numerator the numerator cloudcommitment amount
     * @param denominator the denominator cloud commitment amount
     * @return convert the cloud commitment amount to a float and then take ratio.
     */
    public static float divide(CloudCommitmentAmount numerator, CloudCommitmentAmount denominator) {
        final boolean valuesSet = numerator.getValueCase() != ValueCase.VALUE_NOT_SET
                && denominator.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(
                !valuesSet || numerator.getValueCase() == denominator.getValueCase(),
                "Commitment amounts must be of the same type");
        switch (numerator.getValueCase()) {
            case AMOUNT:
                return denominator.getValueCase() != ValueCase.VALUE_NOT_SET ? (float)(
                        numerator.getAmount().getAmount() / denominator.getAmount().getAmount())
                        : 0.0f;
            case COUPONS:
                return denominator.getValueCase() != ValueCase.VALUE_NOT_SET ? (float)(
                        numerator.getCoupons() / denominator.getCoupons()) : 0.0f;
            case COMMODITIES_BOUGHT:
                return 0.0f;
            case VALUE_NOT_SET:
                return 0.0f;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                numerator.getValueCase()));
        }
    }

    /**
     * Checks if at least one resource in cloud commitment is positive.
     *
     * @param amountA the cloud commitment amout of interest.
     * @param epsilon the epsilon for floating point comparison
     * @return true if atleast one resource is positive.
     */
    public static boolean isPositive(CloudCommitmentAmount amountA, double epsilon) {
        switch (amountA.getValueCase()) {
            case AMOUNT:
                return amountA.getAmount().getAmount() > epsilon;
            case COUPONS:
                return amountA.getCoupons() > epsilon;
            case COMMODITIES_BOUGHT:
                for (CommittedCommodityBought comm : amountA.getCommoditiesBought()
                        .getCommodityList()) {
                    // at least one commodity is positive
                    if (comm.getCapacity() > epsilon) {
                        return true;
                    }
                }
                return false;
            case VALUE_NOT_SET:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                amountA.getValueCase()));
        }
    }

    /**
     * Find the minimum of two cloud commitment amounts. (intersection)
     *
     * @param amountA the first cloud commitment amount.
     * @param amountB the second cloud commitment amount.
     * @return return the min of the two cloud commitment amount.
     */
    public static CloudCommitmentAmount min(CloudCommitmentAmount amountA,
            CloudCommitmentAmount amountB) {
        final boolean valuesSet = amountA.getValueCase() != ValueCase.VALUE_NOT_SET
                && amountB.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(!valuesSet || amountA.getValueCase() == amountB.getValueCase(),
                "Commitment amounts must be of the same type");
        switch (amountA.getValueCase()) {

            case AMOUNT:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? CloudCommitmentAmount.newBuilder().setAmount(CurrencyAmount.newBuilder()
                        .setAmount(Math.min(amountA.getAmount().getAmount(),
                                amountB.getAmount().getAmount()))).build() : amountA;
            case COUPONS:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? CloudCommitmentAmount.newBuilder().setCoupons(
                        Math.min(amountA.getCoupons(), amountB.getCoupons())).build() : amountA;
            case COMMODITIES_BOUGHT:
                if (amountB.getValueCase() == ValueCase.VALUE_NOT_SET) {
                    return amountA;
                }
                final Map<CommodityType, Double> commodityMapB =
                        amountB.getCommoditiesBought().getCommodityList().stream().collect(
                                ImmutableMap.toImmutableMap(
                                        CommittedCommodityBought::getCommodityType,
                                        CommittedCommodityBought::getCapacity));

                CommittedCommoditiesBought.Builder commoditiesBought =
                        CommittedCommoditiesBought.newBuilder();
                for (CommittedCommodityBought commInFirst : amountA.getCommoditiesBought()
                        .getCommodityList()) {
                    Double capacityInSecond = commodityMapB.get(commInFirst.getCommodityType());
                    if (capacityInSecond != null) {
                        commoditiesBought.addCommodity(
                                (CommittedCommodityBought.newBuilder()
                                        .setCommodityType(commInFirst.getCommodityType())
                                        .setProviderType(commInFirst.getProviderType())
                                        .setCapacity(Math.min(commInFirst.getCapacity(),
                                                capacityInSecond))
                                        .build()));
                    }
                }
                return CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(commoditiesBought)
                        .build();
            case VALUE_NOT_SET:
                return amountB;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                amountA.getValueCase()));
        }
    }

    /**
     * Checks if the cloud commitment amount is zero.
     *
     * @param amountA the cloud commitment amount of interest.
     * @param epsilon the epsilon used for floating point comparison.
     * @return true if all resources in the cloud commitment amount is zero.
     */
    public static boolean isZero(CloudCommitmentAmount amountA, double epsilon) {
        switch (amountA.getValueCase()) {
            case AMOUNT:
                return Math.abs(amountA.getAmount().getAmount()) < epsilon;
            case COUPONS:
                return Math.abs(amountA.getCoupons()) < epsilon;
            case COMMODITIES_BOUGHT:
                for (CommittedCommodityBought commInFirst : amountA.getCommoditiesBought()
                        .getCommodityList()) {
                    if (Math.abs(commInFirst.getCapacity()) > epsilon) {
                        return false;
                    }
                }
                return true;
            case VALUE_NOT_SET:
                return true;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                amountA.getValueCase()));
        }
    }
}
