package com.vmturbo.cloud.common.commitment;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
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
}
