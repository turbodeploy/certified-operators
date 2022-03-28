package com.vmturbo.cloud.common.commitment;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Test class for {@link CommitmentAmountUtils}.
 */
public class CommitmentAmountUtilsTest {


    /**
     * Tests {@link CommitmentAmountUtils#groupByKey(CloudCommitmentAmount)} for coupons.
     */
    @Test
    public void testGroupByKeyCoupons() {

        final CloudCommitmentAmount amount = CloudCommitmentAmount.newBuilder()
                .setCoupons(5)
                .build();

        final Map<CloudCommitmentCoverageTypeInfo, Double> actualGrouping =
                CommitmentAmountUtils.groupByKey(amount);

        final Map<CloudCommitmentCoverageTypeInfo, Double> expectedGrouping = ImmutableMap.of(
                CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO,
                amount.getCoupons());
        assertThat(actualGrouping, equalTo(expectedGrouping));
    }

    /**
     * Tests {@link CommitmentAmountUtils#groupByKey(CloudCommitmentAmount)} for a spend amount.
     */
    @Test
    public void testGroupByKeySpendAmount() {

        final CloudCommitmentAmount amount = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setCurrency(123)
                        .setAmount(456)
                        .build())
                .build();

        final Map<CloudCommitmentCoverageTypeInfo, Double> actualGrouping =
                CommitmentAmountUtils.groupByKey(amount);

        final Map<CloudCommitmentCoverageTypeInfo, Double> expectedGrouping = ImmutableMap.of(
                CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .setCoverageSubtype(123)
                        .build(),
                amount.getAmount().getAmount());

        assertThat(actualGrouping, equalTo(expectedGrouping));
    }

    /**
     * Tests {@link CommitmentAmountUtils#groupByKey(CloudCommitmentAmount)} for a commodities
     * bought amount.
     */
    @Test
    public void testGroupByKeyCommoditiesBought() {

        final CloudCommitmentAmount amount = CloudCommitmentAmount.newBuilder()
                .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                        .addCommodity(CommittedCommodityBought.newBuilder()
                                .setCommodityType(CommodityType.NUM_VCORE)
                                .setCapacity(2)
                                .build())
                        .addCommodity(CommittedCommodityBought.newBuilder()
                                .setCommodityType(CommodityType.MEM_PROVISIONED)
                                .setCapacity(4)
                                .build())
                        .build())
                .build();

        final Map<CloudCommitmentCoverageTypeInfo, Double> actualGrouping =
                CommitmentAmountUtils.groupByKey(amount);

        final Map<CloudCommitmentCoverageTypeInfo, Double> expectedGrouping =
                ImmutableMap.<CloudCommitmentCoverageTypeInfo, Double>builder()
                        .put(CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(CommodityType.NUM_VCORE.getNumber())
                                        .build(),
                                2.0)
                        .put(CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(CommodityType.MEM_PROVISIONED.getNumber())
                                        .build(),
                                4.0)
                        .build();

        assertThat(actualGrouping, equalTo(expectedGrouping));
    }
}
