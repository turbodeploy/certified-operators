package com.vmturbo.cloud.common.commitment;

import static com.vmturbo.cloud.common.commitment.CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.tuple.Pair;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount.ValueCase;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A calculator for performing mathematical operations on instances of {@link
 * CloudCommitmentAmount}.
 */
public class CommitmentAmountCalculator {

    /**
     * Zero coverage CloudCommitmentAmount.
     */
    public static final CloudCommitmentAmount ZERO_COVERAGE =
            CloudCommitmentAmount.newBuilder().build();

    /**
     * Rounding value to get a consistant hash.
     */
    public static final float ROUNDING = 100000f;

    /**
     * CommitmentAmountCalculator constructor.
     */
    private CommitmentAmountCalculator() {}

    /**
     * Sums all {@link CloudCommitmentAmount} instances in {@code amounts}. The amounts must all be
     * of the same coverage type or not set.
     *
     * @param amounts The {@link CloudCommitmentAmount} instances to sum.
     * @return The sum of the {@code amounts}.
     */
    @Nonnull
    public static CloudCommitmentAmount sum(@Nonnull Collection<CloudCommitmentAmount> amounts) {

        return amounts.stream().reduce(EMPTY_COMMITMENT_AMOUNT, CommitmentAmountCalculator::sum);
    }

    /**
     * Sums {@code amountA} and {@code amountB}.
     *
     * @param amountA The first commitment amount.
     * @param amountB The second commitment amount.
     * @return The sum of {@code amountA} and {@code amountB}.
     */
    @Nonnull
    public static CloudCommitmentAmount sum(@Nonnull CloudCommitmentAmount amountA,
            @Nonnull CloudCommitmentAmount amountB) {

        validateTypeCompatibility(amountA, amountB);

        if (amountB.getValueCase() == ValueCase.VALUE_NOT_SET) {
            return amountA;
        }

        switch (amountA.getValueCase()) {

            case AMOUNT:
                return CloudCommitmentAmount.newBuilder()
                        .setAmount(opCurrencyAmount(amountA.getAmount(), amountB.getAmount(),
                                Double::sum)).build();
            case COUPONS:
                return CloudCommitmentAmount.newBuilder().setCoupons(
                        amountA.getCoupons() + amountB.getCoupons()).build();
            case COMMODITIES_BOUGHT:
                final Map<CommodityType, Double> commodityMapA =
                        amountA.getCommoditiesBought().getCommodityList().stream().collect(
                                ImmutableMap.toImmutableMap(
                                        CommittedCommodityBought::getCommodityType,
                                        CommittedCommodityBought::getCapacity));

                final Map<CommodityType, Double> commodityMapB =
                        amountB.getCommoditiesBought().getCommodityList().stream().collect(
                                ImmutableMap.toImmutableMap(
                                        CommittedCommodityBought::getCommodityType,
                                        CommittedCommodityBought::getCapacity));

                final List<CommittedCommodityBought> aggregateCommBought = Sets.union(
                        commodityMapA.keySet(), commodityMapB.keySet()).stream().map(
                        commType -> CommittedCommodityBought.newBuilder()
                                .setCommodityType(commType)
                                .setCapacity(commodityMapA.getOrDefault(commType, 0.0)
                                        + commodityMapB.getOrDefault(commType, 0.0))
                                .build()).collect(ImmutableList.toImmutableList());

                return CloudCommitmentAmount.newBuilder().setCommoditiesBought(
                        CommittedCommoditiesBought.newBuilder()
                                .addAllCommodity(aggregateCommBought)).build();
            case VALUE_NOT_SET:
                return amountB;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                amountA.getValueCase()));
        }
    }

    /**
     * Negates {@code amount}.
     *
     * @param amount The commitment amount.
     * @return The negated value of {@code amount}.
     */
    @Nonnull
    public static CloudCommitmentAmount negate(@Nonnull CloudCommitmentAmount amount) {

        Preconditions.checkNotNull(amount, "Amount cannot be null");
        return multiplyAmount(amount, -1.0d);
    }

    /**
     * Multiplies the {@code commitmentAmount} by {@code multiplier}.
     *
     * @param commitmentAmount The commitment amount.
     * @param multiplier The multiplier.
     * @return The result of the multiplication.
     */
    @Nonnull
    public static CloudCommitmentAmount multiplyAmount(
            @Nonnull CloudCommitmentAmount commitmentAmount, double multiplier) {

        final CloudCommitmentAmount.Builder productAmount = commitmentAmount.toBuilder();
        switch (commitmentAmount.getValueCase()) {
            case AMOUNT:
                productAmount.getAmountBuilder().setAmount(
                        commitmentAmount.getAmount().getAmount() * multiplier);
                break;
            case COUPONS:
                productAmount.setCoupons(commitmentAmount.getCoupons() * multiplier);
                break;
            case COMMODITIES_BOUGHT:
                CommittedCommoditiesBought.Builder commoditiesBought =
                        CommittedCommoditiesBought.newBuilder();
                for (CommittedCommodityBought commInFirst : commitmentAmount.getCommoditiesBought()
                        .getCommodityList()) {
                    commoditiesBought.addCommodity(
                            (CommittedCommodityBought.newBuilder()
                                    .setCommodityType(commInFirst.getCommodityType())
                                    .setProviderType(commInFirst.getProviderType())
                                    .setCapacity(multiplier * commInFirst.getCapacity())
                                    .build()));
                }
                return CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(commoditiesBought)
                        .build();
            case VALUE_NOT_SET:
                // Noop
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                commitmentAmount.getValueCase()));
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
            @Nonnull CurrencyAmount amountB, BinaryOperator<Double> operator) {

        Preconditions.checkArgument(
                amountA.hasCurrency() == amountB.hasCurrency() && (!amountA.hasCurrency()
                        || amountA.getCurrency() == amountB.getCurrency()),
                "Currencies must match");

        return amountA.toBuilder().setAmount(
                operator.apply(amountA.getAmount(), amountB.getAmount())).build();
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
     * Find if the two cloud commitment amounts are same.
     *
     * @param amountA the first cloud commitment amount.
     * @param amountB the second cloud commitment amount.
     * @param epsilon tolerance.
     * @return return true if the two cloud commitment amount are same.
     */
    public static boolean isSame(CloudCommitmentAmount amountA, CloudCommitmentAmount amountB,
            double epsilon) {
        {
            validateTypeCompatibility(amountA, amountB);
            if (amountB.getValueCase() == ValueCase.VALUE_NOT_SET) {
                return isZero(amountA, epsilon);
            }
            switch (amountA.getValueCase()) {
                case AMOUNT:
                    return (Math.abs(
                            amountA.getAmount().getAmount() - amountB.getAmount().getAmount())
                            < epsilon)
                            && (amountA.getAmount().hasCurrency() == amountB.getAmount().hasCurrency() && (!amountA.getAmount().hasCurrency()
                            || amountA.getAmount().getCurrency() == amountB.getAmount().getCurrency()));
                case COUPONS:
                    return Math.abs(
                            amountA.getCoupons() - amountB.getCoupons()) < epsilon;
                case COMMODITIES_BOUGHT:
                    final Map<CommodityType, Double> commodityMapA =
                            amountA.getCommoditiesBought().getCommodityList().stream().collect(
                                    ImmutableMap.toImmutableMap(
                                            CommittedCommodityBought::getCommodityType,
                                            CommittedCommodityBought::getCapacity));

                    final Map<CommodityType, Double> commodityMapB =
                            amountB.getCommoditiesBought().getCommodityList().stream().collect(
                                    ImmutableMap.toImmutableMap(
                                            CommittedCommodityBought::getCommodityType,
                                            CommittedCommodityBought::getCapacity));

                    for (CommittedCommodityBought commInFirst : amountA.getCommoditiesBought()
                            .getCommodityList()) {
                        Double capacityInSecond = commodityMapB.getOrDefault(
                                commInFirst.getCommodityType(), 0d);
                        if (Math.abs(commInFirst.getCapacity() - capacityInSecond) > epsilon) {
                            return false;
                        }
                    }
                    for (CommittedCommodityBought commInSecond : amountB.getCommoditiesBought()
                            .getCommodityList()) {
                        Double capacityInFirst = commodityMapA.getOrDefault(
                                commInSecond.getCommodityType(), 0d);
                        if (Math.abs(commInSecond.getCapacity() - capacityInFirst) > epsilon) {
                            return false;
                        }
                    }
                    return true;
                case VALUE_NOT_SET:
                    return isZero(amountB, epsilon);
                default:
                    throw new UnsupportedOperationException(
                            String.format("Cloud commitment amount case %s is not supported",
                                    amountA.getValueCase()));
            }
        }
    }

    /**
     * Find amountA - min(amountA, amountB).
     *
     * @param amountA the first cloud commitment amount.
     * @param amountB the second cloud commitment amount.
     * @return return the min of the two cloud commitment amount.
     */
    public static CloudCommitmentAmount subtract(CloudCommitmentAmount amountA,
            CloudCommitmentAmount amountB) {
        validateTypeCompatibility(amountA, amountB);
        CloudCommitmentAmount min = min(amountA, amountB);
        return sum(amountA, negate(min));
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
        validateTypeCompatibility(amountA, amountB);
        if (amountB.getValueCase() == ValueCase.VALUE_NOT_SET) {
            return CommitmentAmountCalculator.ZERO_COVERAGE;
        }
        switch (amountA.getValueCase()) {

            case AMOUNT:
                return CloudCommitmentAmount.newBuilder().setAmount(CurrencyAmount.newBuilder()
                        .setAmount(Math.min(amountA.getAmount().getAmount(),
                                amountB.getAmount().getAmount()))).build();
            case COUPONS:
                return CloudCommitmentAmount.newBuilder().setCoupons(
                        Math.min(amountA.getCoupons(), amountB.getCoupons())).build();
            case COMMODITIES_BOUGHT:
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
                return CommitmentAmountCalculator.ZERO_COVERAGE;
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

    /**
     * Find the hash value for the cloud commitment amount.
     * @param amountA the CloudCommitmentAmount of interest.
     * @return the hash value for the CloudCommitmentAmount.
     */
    public static long hash(CloudCommitmentAmount amountA) {
        switch (amountA.getValueCase()) {
            case AMOUNT:
                return (long)(amountA.getAmount().getAmount()
                        * CommitmentAmountCalculator.ROUNDING);
            case COUPONS:
                return (long)(amountA.getCoupons() * CommitmentAmountCalculator.ROUNDING);
            case COMMODITIES_BOUGHT:
                List<Pair<CommodityType, Long>> list = new ArrayList<>();
                for (CommittedCommodityBought commInFirst : amountA.getCommoditiesBought()
                        .getCommodityList()) {
                    list.add(Pair.of(commInFirst.getCommodityType(),
                            (long)(commInFirst.getCapacity()
                                    * CommitmentAmountCalculator.ROUNDING)));
                }
                list.sort(
                        (a, b) -> Integer.compare(a.getKey().getNumber(), b.getKey().getNumber()));
                return Objects.hash(list);
            case VALUE_NOT_SET:
                return 0L;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported",
                                amountA.getValueCase()));
        }
    }
}
