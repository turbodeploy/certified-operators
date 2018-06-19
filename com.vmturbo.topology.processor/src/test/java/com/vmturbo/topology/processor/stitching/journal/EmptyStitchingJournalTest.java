package com.vmturbo.topology.processor.stitching.journal;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.vmturbo.stitching.StitchingEntity;

public class EmptyStitchingJournalTest {

    private final EmptyStitchingJournal<StitchingEntity> emptyStitchingJournal = new EmptyStitchingJournal<>();

    @Test
    public void testRecordChangeset() throws Exception {
        final AtomicBoolean changesetApplied = new AtomicBoolean(false);
        emptyStitchingJournal.recordChangeset("some preamble", changeset -> changesetApplied.set(true));
        assertTrue(changesetApplied.get());
    }
}