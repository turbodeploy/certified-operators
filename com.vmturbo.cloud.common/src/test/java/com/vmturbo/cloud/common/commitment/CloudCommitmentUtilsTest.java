package com.vmturbo.cloud.common.commitment;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Test class for {@link CloudCommitmentUtils}.
 */
public class CloudCommitmentUtilsTest {

    /**
     * Tests building a spend commitment through
     * {@link CloudCommitmentUtils#buildCommitmentAmount(CloudCommitmentCoverageType, int, double)}.
     */
    @Test
    public void testBuildSpendCommitmentAmount() {

        final CloudCommitmentAmount actualAmount = CloudCommitmentUtils.buildCommitmentAmount(
                CloudCommitmentCoverageType.SPEND_COMMITMENT,
                123,
                4.56);

        final CloudCommitmentAmount expectedCommitmentAmount = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setCurrency(123)
                        .setAmount(4.56))
                .build();

        assertThat(actualAmount, equalTo(expectedCommitmentAmount));
    }

    /**
     * Tests building a coupon commitment through
     * {@link CloudCommitmentUtils#buildCommitmentAmount(CloudCommitmentCoverageType, int, double)}.
     */
    @Test
    public void testBuildCouponCommitmentAmount() {

        final CloudCommitmentAmount actualAmount = CloudCommitmentUtils.buildCommitmentAmount(
                CloudCommitmentCoverageType.COUPONS,
                -1,
                4.56);

        final CloudCommitmentAmount expectedCommitmentAmount = CloudCommitmentAmount.newBuilder()
                .setCoupons(4.56)
                .build();

        assertThat(actualAmount, equalTo(expectedCommitmentAmount));
    }
}
