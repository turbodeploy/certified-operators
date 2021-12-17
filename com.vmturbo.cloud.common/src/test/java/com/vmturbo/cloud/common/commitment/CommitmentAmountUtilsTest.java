package com.vmturbo.cloud.common.commitment;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

    private static final double TOLERANCE = .00001;

    /**
     * Tests {@link CommitmentAmountUtils#isPositiveAmount(CloudCommitmentAmount, double)} with an
     * infinite spend amount.
     */
    @Test
    public void testPositiveAmountSpendInfinite() {

        final CloudCommitmentAmount commitmentAmount = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(Double.POSITIVE_INFINITY)
                        .build())
                .build();

        assertFalse(CommitmentAmountUtils.isPositiveAmount(commitmentAmount, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountUtils#isPositiveAmount(CloudCommitmentAmount, double)} with a
     * positive spend amount.
     */
    @Test
    public void testPositiveAmountSpendPositive() {

        final CloudCommitmentAmount commitmentAmount = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(123.0)
                        .build())
                .build();

        assertTrue(CommitmentAmountUtils.isPositiveAmount(commitmentAmount, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountUtils#isPositiveAmount(CloudCommitmentAmount, double)} with a
     * negative spend amount.
     */
    @Test
    public void testPositiveAmountSpendNegative() {

        final CloudCommitmentAmount commitmentAmount = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(-123.0)
                        .build())
                .build();

        assertFalse(CommitmentAmountUtils.isPositiveAmount(commitmentAmount, TOLERANCE));
    }

    /**
     * Test comparing spend commitment amounts in which the currencies differ. It is expected an
     * {@link IllegalArgumentException} will be thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCompareAmountsSpendCurrencyMismatch() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(123.0)
                        .setCurrency(1)
                        .build())
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(123.0)
                        .setCurrency(2)
                        .build())
                .build();

        CommitmentAmountUtils.compareAmounts(amountA, amountB);
    }

    /**
     * Tests comparing amounts in which first amount is spend-based and second amount
     * is not set.
     */
    @Test
    public void testCompareAmountsSpendNotSet() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(123.0)
                        .setCurrency(1)
                        .build())
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.getDefaultInstance();

        // amountB should come before amountA
        assertThat(CommitmentAmountUtils.compareAmounts(amountA, amountB), greaterThan(0));
    }

    /**
     * Tests comparing spend amounts in which the first argument is less than the second.
     */
    @Test
    public void testCompareAmountsSpend() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(123.0)
                        .build())
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(456.0)
                        .build())
                .build();

        // amountB should come before amountA
        assertThat(CommitmentAmountUtils.compareAmounts(amountA, amountB), lessThan(0));
    }

    /**
     * Test comparing amounts in which first amount is coupon-based and second is unset.
     */
    @Test
    public void testCompareAmountsCouponNotSet() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setCoupons(1)
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.getDefaultInstance();

        // amountB should come before amountA
        assertThat(CommitmentAmountUtils.compareAmounts(amountA, amountB), greaterThan(0));
    }

    /**
     * Test comparing coupon amounts in which first amount is less than the second.
     */
    @Test
    public void testCompareAmountsCoupons() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setCoupons(1)
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setCoupons(2)
                .build();

        // amountB should come before amountA
        assertThat(CommitmentAmountUtils.compareAmounts(amountA, amountB), lessThan(0));
    }

    /**
     * Tests comparing two unset amounts.
     */
    @Test
    public void testCompareAmountsUnset() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .build();

        // amounts should be equal
        assertThat(CommitmentAmountUtils.compareAmounts(amountA, amountB), equalTo(0));
    }

    /**
     * Test comparing amounts in which the first is unset and the second is set.
     */
    @Test
    public void testCompareAmountsUnsetSecondSet() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setCoupons(1)
                .build();

        // amountA should come first
        assertThat(CommitmentAmountUtils.compareAmounts(amountA, amountB), lessThan(0));
    }

    /**
     * Tests {@link CommitmentAmountUtils#min(CloudCommitmentAmount, CloudCommitmentAmount)} in which
     * first amount is lower than second.
     */
    @Test
    public void testMinFirstLower() {
        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setCoupons(2)
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setCoupons(3)
                .build();

        // amountA should come first
        assertThat(CommitmentAmountUtils.min(amountA, amountB), equalTo(amountA));
    }

    /**
     * Tests {@link CommitmentAmountUtils#min(CloudCommitmentAmount, CloudCommitmentAmount)} in which
     * first amount is higher than second.
     */
    @Test
    public void testMinFirstHigher() {
        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setCoupons(5)
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setCoupons(3)
                .build();

        // amountA should come first
        assertThat(CommitmentAmountUtils.min(amountA, amountB), equalTo(amountB));
    }

    /**
     * Tests {@link CommitmentAmountUtils#groupByKey(CloudCommitmentAmount)} for coupons.
     */
    @Test
    public void testGroupByKeyCoupons() {

        final CloudCommitmentAmount amount = CloudCommitmentAmount.newBuilder()
                .setCoupons(5)
                .build();

        final Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> actualGrouping =
                CommitmentAmountUtils.groupByKey(amount);

        final Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> expectedGrouping = ImmutableMap.of(
                CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.COUPONS)
                        .build(),
                amount);

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

        final Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> actualGrouping =
                CommitmentAmountUtils.groupByKey(amount);

        final Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> expectedGrouping = ImmutableMap.of(
                CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .setCoverageSubtype(123)
                        .build(),
                amount);

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

        final Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> actualGrouping =
                CommitmentAmountUtils.groupByKey(amount);

        final Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> expectedGrouping =
                ImmutableMap.<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount>builder()
                        .put(CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(CommodityType.NUM_VCORE.getNumber())
                                        .build(),
                                CloudCommitmentAmount.newBuilder()
                                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                                .addCommodity(CommittedCommodityBought.newBuilder()
                                                        .setCommodityType(CommodityType.NUM_VCORE)
                                                        .setCapacity(2)
                                                        .build()))
                                        .build())
                        .put(CloudCommitmentCoverageTypeInfo.newBuilder()
                                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                                        .setCoverageSubtype(CommodityType.MEM_PROVISIONED.getNumber())
                                        .build(),
                                CloudCommitmentAmount.newBuilder()
                                        .setCommoditiesBought(CommittedCommoditiesBought.newBuilder()
                                                .addCommodity(CommittedCommodityBought.newBuilder()
                                                        .setCommodityType(CommodityType.MEM_PROVISIONED)
                                                        .setCapacity(4)
                                                        .build()))
                                        .build())
                        .build();

        assertThat(actualGrouping, equalTo(expectedGrouping));
    }
}
