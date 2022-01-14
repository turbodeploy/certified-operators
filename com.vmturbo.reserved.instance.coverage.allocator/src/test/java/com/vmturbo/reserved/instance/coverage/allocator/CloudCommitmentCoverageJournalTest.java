package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.AWS_SP_INFO_TEST;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableTable;

import org.junit.Test;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

public class CloudCommitmentCoverageJournalTest {

    /**
     * Test {@link CloudCommitmentCoverageJournal#validateCoverages()} in which an RI is over
     * capacity
     */
    @Test(expected = VerifyException.class)
    public void testRIOverCapacityValidation() {

        final CoverageTopology mockCoverageTopology = mock(CoverageTopology.class);

        when(mockCoverageTopology.getCommitmentCapacity(2L, CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                .thenReturn(1.0);
        when(mockCoverageTopology.getCoverageCapacityForEntity(1L, CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                .thenReturn(2.0);

        // setup SUT
        final CloudCommitmentCoverageJournal coverageJournal =
                CloudCommitmentCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        mockCoverageTopology);

        final CloudCommitmentCoverageTypeInfo coverageTypeInfo = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COUPONS)
                .build();
        coverageJournal.addCoverageEntry(CoverageJournalEntry.builder()
                .cloudServiceProvider(AWS_SP_INFO_TEST)
                .sourceName("sourceNameTest")
                .entityOid(1)
                .cloudCommitmentOid(2)
                .coverageType(coverageTypeInfo)
                .requestedCoverage(2.0)
                .availableCoverage(2.0)
                .allocatedCoverage(2.0)
                .build());

        // invoke SUT
        coverageJournal.validateCoverages();
    }

    /**
     * Test {@link CloudCommitmentCoverageJournal#validateCoverages()} in which an entity is over
     * capacity
     */
    @Test(expected = VerifyException.class)
    public void testEntityOverCapacityValidation() {

        final CoverageTopology mockCoverageTopology = mock(CoverageTopology.class);
        when(mockCoverageTopology.getCommitmentCapacity(2L, CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                .thenReturn(2.0);
        when(mockCoverageTopology.getCoverageCapacityForEntity(1L, CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                .thenReturn(1.0);

        // setup SUT
        final CloudCommitmentCoverageJournal coverageJournal =
                CloudCommitmentCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        mockCoverageTopology);

        final CloudCommitmentCoverageTypeInfo coverageTypeInfo = CloudCommitmentCoverageTypeInfo.newBuilder()
                .setCoverageType(CloudCommitmentCoverageType.COUPONS)
                .build();
        coverageJournal.addCoverageEntry(CoverageJournalEntry.builder()
                .cloudServiceProvider(AWS_SP_INFO_TEST)
                .sourceName("sourceNameTest")
                .entityOid(1)
                .cloudCommitmentOid(2)
                .coverageType(coverageTypeInfo)
                .requestedCoverage(2.0)
                .availableCoverage(2.0)
                .allocatedCoverage(2.0)
                .build());

        // invoke SUT
        coverageJournal.validateCoverages();
    }
}
