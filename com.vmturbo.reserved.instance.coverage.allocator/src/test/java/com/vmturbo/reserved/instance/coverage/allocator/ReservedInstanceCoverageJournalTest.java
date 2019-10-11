package com.vmturbo.reserved.instance.coverage.allocator;

import java.util.Collections;

import org.junit.Test;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;

import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal.CoverageJournalEntry;

public class ReservedInstanceCoverageJournalTest {


    /**
     * Test {@link ReservedInstanceCoverageJournal#validateCoverages()} in which an RI is over
     * capacity
     */
    @Test(expected = VerifyException.class)
    public void testRIOverCapacityValidation() {

        // setup SUT
        final ReservedInstanceCoverageJournal coverageJournal =
                ReservedInstanceCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        // Entity 1 with capacity 2
                        ImmutableMap.of(1L, 2.0),
                        // RI 2 with capacity 1
                        ImmutableMap.of(2L, 1.0));

        coverageJournal.addCoverageEntry(CoverageJournalEntry.of("",
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

        // setup SUT
        final ReservedInstanceCoverageJournal coverageJournal =
                ReservedInstanceCoverageJournal.createJournal(
                        ImmutableTable.of(),
                        // Entity 1 with capacity 2
                        ImmutableMap.of(1L, 1.0),
                        // RI 2 with capacity 1
                        ImmutableMap.of(2L, 2.0));

        coverageJournal.addCoverageEntry(CoverageJournalEntry.of("",
                2, 1, 2, 2, 2));

        // invoke SUT
        coverageJournal.validateCoverages();
    }
}
