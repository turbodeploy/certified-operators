package com.vmturbo.cloud.common.commitment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;

/**
 * Test class for {@link CommitmentAmountJournal}.
 */
public class CommitmentAmountJournalTest {

    /**
     * Tests adding two coupon amounts to {@link CommitmentAmountJournal}.
     */
    @Test
    public void testAddingAmounts() {

        final CommitmentAmountJournal journal = CommitmentAmountJournal.create();

        // invoke journal
        journal.addAmount(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO, 1.0);
        journal.addAmount(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO, 2.0);

        // Assertions
        final Map<CloudCommitmentCoverageTypeInfo, Double> expectedVectorMap = ImmutableMap.of(
                CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO, 3.0);
        assertThat(journal.getVectorMap(), equalTo(expectedVectorMap));

        final CloudCommitmentAmount actualCommitmentAmount = journal.getCommitmentAmount();
        assertTrue(actualCommitmentAmount.hasCoupons());
        assertThat(actualCommitmentAmount.getCoupons(), equalTo(3.0));
    }

    /**
     * Tests adding two conflicting vector types to {@link CommitmentAmountJournal}.
     */
    @Test(expected = VerifyException.class)
    public void testIncompatibleAmounts() {
        final CommitmentAmountJournal journal = CommitmentAmountJournal.create();

        // invoke journal
        journal.addAmount(CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO, 1.0);
        journal.addAmount(
                CloudCommitmentCoverageTypeInfo.newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.SPEND_COMMITMENT)
                        .build(),
                2.0);
    }


}
