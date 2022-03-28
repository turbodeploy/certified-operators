package com.vmturbo.cloud.common.commitment;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Utility class for working with {@link CloudCommitmentAmount}.
 */
public class CommitmentAmountUtils {

    /**
     * An empty {@link CloudCommitmentAmount}.
     */
    public static final CloudCommitmentAmount EMPTY_COMMITMENT_AMOUNT = CloudCommitmentAmount.getDefaultInstance();

    private CommitmentAmountUtils() {}

    /**
     * Converts the coverage type information and amount to a {@link CloudCommitmentAmount}.
     * @param coverageTypeInfo The coverage type info.
     * @param amount The coverage amount.
     * @return The consolidated {@link CloudCommitmentAmount} instance.
     */
    @Nonnull
    public static CloudCommitmentAmount convertToAmount(@Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo,
                                                        double amount) {
        switch (coverageTypeInfo.getCoverageType()) {
            case COUPONS:
                return CloudCommitmentAmount.newBuilder()
                        .setCoupons(amount)
                        .build();
            case COMMODITY:
                return CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                .addCommodity(CommittedCommodityBought.newBuilder()
                                        .setCommodityType(CommodityType.forNumber(coverageTypeInfo.getCoverageSubtype()))
                                        .setCapacity(amount)))
                        .build();
            case SPEND_COMMITMENT:
                return CloudCommitmentAmount.newBuilder()
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(coverageTypeInfo.getCoverageSubtype())
                                .setAmount(amount))
                        .build();
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", coverageTypeInfo.getCoverageType()));
        }
    }


    /**
     * Groups the coverage contained with {@code amounts} by the {@link CloudCommitmentCoverageTypeInfo} and sums
     * the amounts under each coverage type.
     * @param amounts The amounts to group and sum.
     * @return An immutable map of the coverage amounts, indexed and summed by the {@link CloudCommitmentCoverageTypeInfo}.
     */
    @Nonnull
    public static Map<CloudCommitmentCoverageTypeInfo, Double> groupAndSum(@Nonnull Collection<CloudCommitmentAmount> amounts) {

        return amounts.stream()
                .map(CommitmentAmountUtils::groupByKey)
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(ImmutableMap.toImmutableMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        Double::sum));
    }

    /**
     * Groups individual coverage subtypes contained within {@code amount} by the {@link CloudCommitmentCoverageTypeInfo}. A
     * {@link CloudCommitmentAmount} may contain multiple coverage types in the case of commodity-based coverage, in which
     * a cloud commitment may be able to cover multiple commodities.
     * @param amount The commitment amount.
     * @return An immutable map of {@code amount}, broken down by {@link CloudCommitmentCoverageTypeInfo}.
     */
    @Nonnull
    public static Map<CloudCommitmentCoverageTypeInfo, Double> groupByKey(@Nonnull CloudCommitmentAmount amount) {

        switch (amount.getValueCase()) {
            case COUPONS:
                return ImmutableMap.of(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO, amount.getCoupons());
            case AMOUNT:
                final CloudCommitmentCoverageTypeInfo coverageKey = CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .setCoverageSubtype(amount.getAmount().getCurrency())
                        .build();
                return ImmutableMap.of(coverageKey, amount.getAmount().getAmount());
            case COMMODITIES_BOUGHT:
                return amount.getCommoditiesBought().getCommodityList()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                committedCommodityBought -> CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(committedCommodityBought.getCommodityType().getNumber())
                                        .build(),
                                CommittedCommodityBought::getCapacity));
            case VALUE_NOT_SET:
                return Collections.emptyMap();
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment amount case %s is not supported", amount.getValueCase()));
        }
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
    public static double sumCoverageType(@Nonnull Collection<CloudCommitmentAmount> commitmentAmounts,
                                         @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return commitmentAmounts.stream()
                .map(commitmentAmount -> CommitmentAmountUtils.groupByKey(commitmentAmount).getOrDefault(coverageTypeInfo, 0.0))
                .reduce(0.0, Double::sum);
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
    public static double filterByCoverageKey(@Nonnull CloudCommitmentAmount commitmentAmount,
                                             @Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return groupByKey(commitmentAmount).getOrDefault(coverageTypeInfo, 0.0);
    }
}
