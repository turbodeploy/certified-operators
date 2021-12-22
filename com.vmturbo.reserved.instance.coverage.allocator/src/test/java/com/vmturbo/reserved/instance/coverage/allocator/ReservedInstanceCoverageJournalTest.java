package com.vmturbo.reserved.instance.coverage.allocator;

import static com.vmturbo.reserved.instance.coverage.allocator.AwsAllocationTopologyTest.AWS_SP_INFO_TEST;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

import org.junit.Test;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

public class ReservedInstanceCoverageJournalTest {


    /**
     * Test {@link ReservedInstanceCoverageJournal#validateCoverages()} in which an RI is over
     * capacity
     */
    @Test(expected = VerifyException.class)
    public void testRIOverCapacityValidation() {

        final CoverageTopology mockCoverageTopology = mock(CoverageTopology.class);
        // Entity 1 with capacity 2
        when(mockCoverageTopology.getCommitmentCapacityByOid())
                .thenReturn(ImmutableMap.of(2L, 1.0));
        // RI 2 with capacity 1
        when(mockCoverageTopology.getCoverageCapacityForEntity(eq(1L))).thenReturn(2.0);

        // setup SUT
        final ReservedInstanceCoverageJournal coverageJournal =
                ReservedInstanceCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        mockCoverageTopology);

        coverageJournal.addCoverageEntry(CoverageJournalEntry.of(
                AWS_SP_INFO_TEST, "sourceNameTest",
                2, 1, 2, 2, 2));

        // invoke SUT
        coverageJournal.validateCoverages();
    }

    /**
     * Test {@link ReservedInstanceCoverageJournal#validateCoverages()} in which an entity is over
     * capacity
     */
    @Test(expected = VerifyException.class)
    public void testEntityOverCapacityValidation() {

        final CoverageTopology mockCoverageTopology = mock(CoverageTopology.class);
        // Entity 1 with capacity 2
        when(mockCoverageTopology.getCommitmentCapacityByOid())
                .thenReturn(ImmutableMap.of(2L, 1.0));
        // RI 2 with capacity 1
        when(mockCoverageTopology.getCoverageCapacityForEntity(eq(1L))).thenReturn(2.0);

        // setup SUT
        final ReservedInstanceCoverageJournal coverageJournal =
                ReservedInstanceCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        mockCoverageTopology);

        coverageJournal.addCoverageEntry(CoverageJournalEntry.of(
                AWS_SP_INFO_TEST, "sourceNameTest",
                2, 1, 2, 2, 2));

        // invoke SUT
        coverageJournal.validateCoverages();
    }
}
