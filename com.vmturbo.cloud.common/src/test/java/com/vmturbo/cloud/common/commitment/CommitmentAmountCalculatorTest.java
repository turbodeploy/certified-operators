package com.vmturbo.cloud.common.commitment;

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Test class for {@link CommitmentAmountCalculator}
 */
public class CommitmentAmountCalculatorTest {

    private static final double TOLERANCE = 1.0e-6;

    /**
     * Tests {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with two spend amounts.
     */
    @Test
    public void testSubtractAmounts() {

        final CloudCommitmentAmount minuend = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(3.3)
                        .build())
                .build();
        final CloudCommitmentAmount subtrahend = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(2.3)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.subtract(minuend, subtrahend);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(1.0, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with amount minus empty.
     */
    @Test
    public void testSubtractEmptyFromAmount() {

        final CloudCommitmentAmount minuend = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(3.3)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.subtract(minuend, CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(minuend.getAmount().getAmount(), TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with empty minus amount.
     */
    @Test
    public void testSubtractAmountFromEmpty() {

        final CloudCommitmentAmount subtrahend = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(2.3)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.subtract(CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT, subtrahend);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(-2.3, TOLERANCE));
    }

    /**
     * Test {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with currency amounts in different currencies.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSubtractInvalidCurrencies() {

        final CloudCommitmentAmount minuend = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(3.3)
                        .setCurrency(1)
                        .build())
                .build();
        final CloudCommitmentAmount subtrahend = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(2.3)
                        .setCurrency(2)
                        .build())
                .build();

        CommitmentAmountCalculator.subtract(minuend, subtrahend);
    }

    /**
     * Tests {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with two coupon amounts.
     */
    @Test
    public void testSubtractCoupons() {

        final CloudCommitmentAmount minuend = CloudCommitmentAmount.newBuilder()
                .setCoupons(3.3)
                .build();
        final CloudCommitmentAmount subtrahend = CloudCommitmentAmount.newBuilder()
                .setCoupons(2.3)
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.subtract(minuend, subtrahend);
        assertTrue(actualAmount.hasCoupons());
        assertThat(actualAmount.getCoupons(), closeTo(1.0, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with a coupon amount minus an empty amount.
     */
    @Test
    public void testSubtractEmptyFromCoupon() {

        final CloudCommitmentAmount minuend = CloudCommitmentAmount.newBuilder()
                .setCoupons(3.3)
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.subtract(minuend, CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
        assertTrue(actualAmount.hasCoupons());
        assertThat(actualAmount.getCoupons(), closeTo(3.3, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#subtract(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with an empty amount minus a coupon amount.
     */
    @Test
    public void testSubtractCouponFromEmpty() {

        final CloudCommitmentAmount subtrahend = CloudCommitmentAmount.newBuilder()
                .setCoupons(2.3)
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.subtract(CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT, subtrahend);
        assertTrue(actualAmount.hasCoupons());
        assertThat(actualAmount.getCoupons(), closeTo(-2.3, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#sum(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with two currency amounts.
     */
    @Test
    public void testSumAmounts() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(3.3)
                        .build())
                .build();
        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(2.3)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.sum(amountA, amountB);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(5.6, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#sum(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with a currency amount and empty amount.
     */
    @Test
    public void testSumAmountAndEmpty() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(3.3)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.sum(amountA, CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(3.3, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#sum(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with an empty amount and a currency amount.
     */
    @Test
    public void testSumEmptyAndAmount() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(3.3)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.sum(CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT, amountA);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(3.3, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#sum(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with two coupon amounts.
     */
    @Test
    public void testSumCoupons() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setCoupons(1.1)
                .build();

        final CloudCommitmentAmount amountB = CloudCommitmentAmount.newBuilder()
                .setCoupons(2.2)
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.sum(amountA, amountB);
        assertTrue(actualAmount.hasCoupons());
        assertThat(actualAmount.getCoupons(), closeTo(3.3, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#sum(CloudCommitmentAmount, CloudCommitmentAmount)}
     * with a coupon and empty amount.
     */
    @Test
    public void testSumCouponAndEmpty() {

        final CloudCommitmentAmount amountA = CloudCommitmentAmount.newBuilder()
                .setCoupons(1.1)
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.sum(amountA, CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
        assertTrue(actualAmount.hasCoupons());
        assertThat(actualAmount.getCoupons(), closeTo(1.1, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#multiplyAmount(CloudCommitmentAmount, double)}
     * for a currency amount.
     */
    @Test
    public void testMultiplyCurrencyAmount() {

        final CloudCommitmentAmount amount = CloudCommitmentAmount.newBuilder()
                .setAmount(CurrencyAmount.newBuilder()
                        .setAmount(1.1)
                        .build())
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.multiplyAmount(amount, 3.0);
        assertTrue(actualAmount.hasAmount());
        assertThat(actualAmount.getAmount().getAmount(), closeTo(3.3, TOLERANCE));
    }

    /**
     * Tests {@link CommitmentAmountCalculator#multiplyAmount(CloudCommitmentAmount, double)}
     * for a coupon amount.
     */
    @Test
    public void testMultiplyCoupons() {

        final CloudCommitmentAmount amount = CloudCommitmentAmount.newBuilder()
                .setCoupons(1.1)
                .build();

        final CloudCommitmentAmount actualAmount = CommitmentAmountCalculator.multiplyAmount(amount, 3.0);
        assertTrue(actualAmount.hasCoupons());
        assertThat(actualAmount.getCoupons(), closeTo(3.3, TOLERANCE));
    }
}
