package com.vmturbo.cloud.common.commitment;

import static com.vmturbo.cloud.common.commitment.CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT;

import java.util.Collection;
import java.util.function.BinaryOperator;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount.ValueCase;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A calculator for performing mathematical operations on instances of {@link CloudCommitmentAmount}.
 */
public class CommitmentAmountCalculator {

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

        final boolean valuesSet = minuendAmount.getValueCase() != ValueCase.VALUE_NOT_SET
                && subtrahendAmount.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(!valuesSet || minuendAmount.getValueCase() == subtrahendAmount.getValueCase(),
                "The minuend and subtrahend commitment amounts must have the same value case");

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

        final boolean valuesSet = amountA.getValueCase() != ValueCase.VALUE_NOT_SET
                && amountB.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(!valuesSet || amountA.getValueCase() == amountB.getValueCase(),
                "Commitment amounts must be of the same type");

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

    @Nonnull
    private static CurrencyAmount opCurrencyAmount(@Nonnull CurrencyAmount minuendAmount,
                                                   @Nonnull CurrencyAmount subtrahendAmount,
                                                   BinaryOperator<Double> operator) {

        Preconditions.checkArgument(
                minuendAmount.hasCurrency() == subtrahendAmount.hasCurrency()
                        && (!minuendAmount.hasCurrency() || minuendAmount.getCurrency() == subtrahendAmount.getCurrency()),
                "Currencies must match");


        return minuendAmount.toBuilder()
                .setAmount(operator.apply(minuendAmount.getAmount(), subtrahendAmount.getAmount()))
                .build();
    }
}
