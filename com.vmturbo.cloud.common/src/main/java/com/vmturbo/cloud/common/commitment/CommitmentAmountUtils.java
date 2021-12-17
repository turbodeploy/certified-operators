package com.vmturbo.cloud.common.commitment;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount.ValueCase;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.components.common.utils.FuzzyDoubleUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Utility class for working with {@link CloudCommitmentAmount}.
 */
public class CommitmentAmountUtils {

    /**
     * Comparator for {@link CloudCommitmentAmount}. All compared amounts within a single stream must be either
     * unset and/or of the same {@link CloudCommitmentCoverageType}. Unset amounts will be ordered first
     * in the comparison.
     */
    public static final Comparator<CloudCommitmentAmount> COMMITMENT_AMOUNT_COMPARATOR = CommitmentAmountUtils::compareAmounts;

    /**
     * An empty {@link CloudCommitmentAmount}.
     */
    public static final CloudCommitmentAmount EMPTY_COMMITMENT_AMOUNT = CloudCommitmentAmount.getDefaultInstance();

    private CommitmentAmountUtils() {}

    /**
     * Checks whether {@code commitmentAmount} represents a positive finite amount (greater than 0), comparing
     * against zero using {@code tolerance}.
     * @param commitmentAmount The commitment amount.
     * @param tolerance The tolerance in the comparison.
     * @return True, if the commitment amount is a positive finite amount. False otherwise.
     */
    public static boolean isPositiveAmount(@Nonnull CloudCommitmentAmount commitmentAmount,
                                           double tolerance) {

        switch (commitmentAmount.getValueCase()) {

            case AMOUNT:
                return FuzzyDoubleUtils.isPositive(commitmentAmount.getAmount().getAmount(), tolerance)
                        && Double.isFinite(commitmentAmount.getAmount().getAmount());
            case COUPONS:
                return FuzzyDoubleUtils.isPositive(commitmentAmount.getCoupons(), tolerance)
                        && Double.isFinite(commitmentAmount.getCoupons());
            case VALUE_NOT_SET:
                return false;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", commitmentAmount.getValueCase()));
        }
    }

    /**
     * Compares the two {@link CloudCommitmentAmount} instances. If either is an empty amount, it will
     * be treated as a lower amount. If both amounts are set, they must be of the same coverage type.
     * @param amountA The first commitment amount.
     * @param amountB The second commitment amount.
     * @return A negative value if {@code amountA} comes before {@code amountB}, zero, if the amounts
     * are equal, and a positive amount, if {@code amountB} comes before {@code amountA}.
     */
    public static int compareAmounts(@Nonnull CloudCommitmentAmount amountA,
                                     @Nonnull CloudCommitmentAmount amountB) {

        final boolean valuesSet = amountA.getValueCase() != ValueCase.VALUE_NOT_SET
                && amountB.getValueCase() != ValueCase.VALUE_NOT_SET;
        Preconditions.checkArgument(!valuesSet || amountA.getValueCase() == amountB.getValueCase(),
                "Commitment amounts must be of the same type");

        switch (amountA.getValueCase()) {

            case AMOUNT:

                if (amountB.getValueCase() != ValueCase.VALUE_NOT_SET) {
                    final CurrencyAmount currencyAmountA = amountA.getAmount();
                    final CurrencyAmount currencyAmountB = amountB.getAmount();

                    Preconditions.checkArgument(
                            currencyAmountA.hasCurrency() == currencyAmountB.hasCurrency() && (!currencyAmountA.hasCurrency() || currencyAmountA.getCurrency()
                                    == currencyAmountB.getCurrency()),
                            "Currencies must match");

                    return Double.compare(currencyAmountA.getAmount(), currencyAmountB.getAmount());
                } else {
                    return 1;
                }
            case COUPONS:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? Double.compare(amountA.getCoupons(), amountB.getCoupons())
                        : 1;
            case VALUE_NOT_SET:
                return amountB.getValueCase() != ValueCase.VALUE_NOT_SET
                        ? -1
                        : 0;
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", amountA.getValueCase()));
        }
    }

    /**
     * Selects the minimum amount between {@code amountA} and {@code amountB}. Note that empty values
     * will be treated as lower than any other value type. {@code amountA} and {@code amountB} must be
     * of the same coverage type, if both are set.
     * @param amountA The first commitment amount.
     * @param amountB The second commitment amount.
     * @return The lower amount between {@code amountA} and {@code amountB}.
     */
    @Nonnull
    public static CloudCommitmentAmount min(@Nonnull CloudCommitmentAmount amountA,
                                            @Nonnull CloudCommitmentAmount amountB) {

        return compareAmounts(amountA, amountB) <= 0 ? amountA : amountB;
    }

    /**
     * Groups the coverage contained with {@code amounts} by the {@link CloudCommitmentCoverageTypeInfo} and sums
     * the amounts under each coverage type.
     * @param amounts The amounts to group and sum.
     * @return An immutable map of the coverage amounts, indexed and summed by the {@link CloudCommitmentCoverageTypeInfo}.
     */
    @Nonnull
    public static Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> groupAndSum(@Nonnull Collection<CloudCommitmentAmount> amounts) {

        return amounts.stream()
                .map(CommitmentAmountUtils::groupByKey)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        CommitmentAmountCalculator::sum));
    }

    /**
     * Groups individual coverage subtypes contained within {@code amount} by the {@link CloudCommitmentCoverageTypeInfo}. A
     * {@link CloudCommitmentAmount} may contain multiple coverage types in the case of commodity-based coverage, in which
     * a cloud commitment may be able to cover multiple commodities.
     * @param amount The commitment amount.
     * @return An immutable map of {@code amount}, broken down by {@link CloudCommitmentCoverageTypeInfo}.
     */
    @Nonnull
    public static Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> groupByKey(@Nonnull CloudCommitmentAmount amount) {

        final ImmutableMap.Builder<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> amountByType =
                ImmutableMap.builder();

        switch (amount.getValueCase()) {
            case COUPONS:
                amountByType.put(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO, amount);
                break;
            case AMOUNT:
                final CloudCommitmentCoverageTypeInfo coverageKey = CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .setCoverageSubtype(amount.getAmount().getCurrency())
                        .build();
                amountByType.put(coverageKey, amount);
                break;
            case COMMODITIES_BOUGHT:
                amount.getCommoditiesBought().getCommodityList().forEach(committedCommodityBought ->
                        amountByType.put(
                                CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(committedCommodityBought.getCommodityType().getNumber())
                                        .build(),
                                CloudCommitmentAmount.newBuilder()
                                        .setCommoditiesBought(
                                                CommittedCommoditiesBought.newBuilder()
                                                        .addCommodity(committedCommodityBought))
                                        .build()));
                break;

        }

        return amountByType.build();
    }

    /**
     * Extract the coverage type info set from the {@code commitmentAmount}.
     * @param commitmentAmount The commitment amount to analyze.
     * @return An immutable set of coverage type infos.
     */
    @Nonnull
    public static Set<CloudCommitmentCoverageTypeInfo> extractTypeInfo(@Nonnull CloudCommitmentAmount commitmentAmount) {
        switch (commitmentAmount.getValueCase()) {
            case COUPONS:
                return Collections.singleton(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO);
            case AMOUNT:
                return Collections.singleton(CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .setCoverageSubtype(commitmentAmount.getAmount().getCurrency())
                        .build());
            case COMMODITIES_BOUGHT:
                return commitmentAmount.getCommoditiesBought().getCommodityList()
                        .stream()
                        .map(commodityBought -> CloudCommitmentCoverageTypeInfo.newBuilder()
                                .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                .setCoverageSubtype(commodityBought.getCommodityType().getNumber())
                                .build())
                        .collect(ImmutableSet.toImmutableSet());
            case VALUE_NOT_SET:
                return Collections.emptySet();
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", commitmentAmount.getValueCase()));
        }
    }

    /**
     * Filters commitment amounts that match {@code coverageTypeInfo} from {@code commitmentAmounts}.
     * @param commitmentAmounts The commitment amounts to filter.
     * @param coverageTypeInfo The coverage type info to filter against.
     * @return An immutable list of {@link CloudCommitmentAmount} that match the {@code coverageTypeInfo}. These
     * commitment amounts may be a subset of individual {@link CloudCommitmentAmount} instances in {@code commitmentAmounts}.
     */
    @Nonnull
    public static List<CloudCommitmentAmount> filterByCoverageKey(@Nonnull Collection<CloudCommitmentAmount> commitmentAmounts,
                                                                  @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return commitmentAmounts.stream()
                .map(commitmentAmount -> CommitmentAmountUtils.groupByKey(commitmentAmount).get(coverageTypeInfo))
                .filter(Objects::nonNull)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Filters {@code commitmentAmount} against {@code coverageTypeInfo}.
     * @param commitmentAmount The commitment amount to filter.
     * @param coverageTypeInfo The coverage type info to filter against.
     * @return A {@link CloudCommitmentAmount} matching the {@code coverageTypeInfo}. This may be identical
     * to {@ocde commitmentAmount}, a subset of commitment amount (in cases where the coverage type supports
     * multiple subtypes), or an empty commitment amount (if {@code commitmentAmount} does not match
     * the filter).
     */
    @Nonnull
    public static CloudCommitmentAmount filterByCoverageKey(@Nonnull CloudCommitmentAmount commitmentAmount,
                                                            @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return filterByCoverageKey(Collections.singleton(commitmentAmount), coverageTypeInfo).get(0);
    }
}
