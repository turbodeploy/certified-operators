package com.vmturbo.reserved.instance.coverage.allocator;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal.CoverageJournalEntry;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext.CloudServiceProvider;
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
        when(mockCoverageTopology.getReservedInstanceCapacityByOid())
                .thenReturn(ImmutableMap.of(2L, 1L));
        // RI 2 with capacity 1
        when(mockCoverageTopology.getRICoverageCapacityForEntity(eq(1L))).thenReturn(2L);

        // setup SUT
        final ReservedInstanceCoverageJournal coverageJournal =
                ReservedInstanceCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        mockCoverageTopology);

        coverageJournal.addCoverageEntry(CoverageJournalEntry.of(
                CloudServiceProvider.AWS, "sourceNameTest",
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
        when(mockCoverageTopology.getReservedInstanceCapacityByOid())
                .thenReturn(ImmutableMap.of(2L, 1L));
        // RI 2 with capacity 1
        when(mockCoverageTopology.getRICoverageCapacityForEntity(eq(1L))).thenReturn(2L);

        // setup SUT
        final ReservedInstanceCoverageJournal coverageJournal =
                ReservedInstanceCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        mockCoverageTopology);

        coverageJournal.addCoverageEntry(CoverageJournalEntry.of(
                CloudServiceProvider.AWS, "sourceNameTest",
                2, 1, 2, 2, 2));

        // invoke SUT
        coverageJournal.validateCoverages();
    }
}
