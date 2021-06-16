package com.vmturbo.cloud.common.commitment;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A utility class for cloud commitment helper methods.
 */
public class CloudCommitmentUtils {

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
        if (coverageType == CloudCommitmentCoverageType.SPEND_COMMITMENT) {

            return CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(coverageSubType)
                            .setAmount(amount))
                    .build();
        } else { // assume coupons for now
            return CloudCommitmentAmount.newBuilder()
                    .setCoupons(amount)
                    .build();
        }
    }
}
