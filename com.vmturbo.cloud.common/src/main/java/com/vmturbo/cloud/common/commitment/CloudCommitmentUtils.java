package com.vmturbo.cloud.common.commitment;

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Utility class for working with cloud commitments.
 */
public class CloudCommitmentUtils {

    /**
     * The coupon {@link CloudCommitmentCoverageTypeInfo}.
     */
    public static final CloudCommitmentCoverageTypeInfo COUPON_COVERAGE_TYPE_INFO = CloudCommitmentCoverageTypeInfo.newBuilder()
            .setCoverageType(CloudCommitmentCoverageType.COUPONS)
            .setCoverageSubtype(0)
            .build();

    private CloudCommitmentUtils() {}

    /**
     * Constructs a {@link CloudCommitmentAmount}, based on the provided table.
     * @param coverageType The coverage type (e.g. spend or coupon).
     * @param coverageSubType The coverage subtype, the meaning of which depends on the coverage type.
     * @param amount The amount of coverage.
     * @return The newly constructed {@link CloudCommitmentAmount}.
     */
    public static CloudCommitmentAmount buildCommitmentAmount(@Nonnull CloudCommitmentCoverageType coverageType,
                                                              int coverageSubType,
                                                              double amount) {

        switch (coverageType) {

            case SPEND_COMMITMENT:
                return CloudCommitmentAmount.newBuilder()
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(coverageSubType)
                                .setAmount(amount))
                        .build();
            case COUPONS:
                return CloudCommitmentAmount.newBuilder()
                        .setCoupons(amount)
                        .build();
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud commitment coverage type %s is not supported", coverageType));
        }
    }

    /**
     * Constructs a {@link CloudCommitmentAmount}, representing the capacity of the {@code commitmentEntity}.
     * @param commitmentEntity The cloud commitment represented as a {@link TopologyEntityDTO}.
     * @return The capacity of the cloud commitment.
     */
    @Nonnull
    public static CloudCommitmentAmount createCapacityAmount(@Nonnull TopologyEntityDTO commitmentEntity) {

        Preconditions.checkArgument(commitmentEntity.getTypeSpecificInfo().hasCloudCommitmentData());

        final CloudCommitmentInfo commitmentInfo = commitmentEntity.getTypeSpecificInfo().getCloudCommitmentData();
        switch (commitmentInfo.getCommitmentCase()) {

            case NUMBER_COUPONS:
                return CloudCommitmentAmount.newBuilder()
                        .setCoupons(commitmentInfo.getNumberCoupons())
                        .build();
            case SPEND:
                return CloudCommitmentAmount.newBuilder()
                        .setAmount(commitmentInfo.getSpend())
                        .build();
            case COMMODITIES_BOUGHT:
                return CloudCommitmentAmount.newBuilder()
                        .setCommoditiesBought(commitmentInfo.getCommoditiesBought())
                        .build();
            default:
                return CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT;
        }
    }

    /**
     * Converts a cloud commitment topology entity's capacity contained within its {@link CloudCommitmentInfo} to a map
     * of commitment capacity by coverage vector.
     * @param commitmentEntity The cloud commitment entity.
     * @return An immutable map of commitment capacity by coverage vector.
     */
    @Nonnull
    public static Map<CloudCommitmentCoverageTypeInfo, Double> resolveCapacityVectors(@Nonnull TopologyEntityDTO commitmentEntity) {

        Preconditions.checkArgument(commitmentEntity.getTypeSpecificInfo().hasCloudCommitmentData());

        final CloudCommitmentInfo commitmentInfo = commitmentEntity.getTypeSpecificInfo().getCloudCommitmentData();

        switch (commitmentInfo.getCommitmentCase()) {

            case NUMBER_COUPONS:
                return ImmutableMap.of(
                        COUPON_COVERAGE_TYPE_INFO,
                        (double)commitmentInfo.getNumberCoupons());
            case SPEND:
                return ImmutableMap.of(
                        CloudCommitmentCoverageTypeInfo.newBuilder()
                                .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                                .setCoverageSubtype(commitmentInfo.getSpend().getCurrency())
                                .build(),
                        commitmentInfo.getSpend().getAmount());
            case COMMODITIES_BOUGHT:
                return commitmentInfo.getCommoditiesBought().getCommodityList()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                committedCommodity -> CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(committedCommodity.getCommodityType().getNumber())
                                        .build(),
                                committedCommodity -> committedCommodity.getCapacity()));
            default:
                return Collections.emptyMap();
        }
    }
}
