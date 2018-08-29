package com.vmturbo.topology.processor.probes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.probes.ProbeStitchingDependencyTracker.Builder;

/**
 * Tests for {@link ProbeStitchingDependencyTracker}.
 */
public class ProbeStitchingDependencyTrackerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSimpleLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(
                "Cycle found in generating stitching dependency graph for probes.");
        builder.requireThat(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThat(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.CLOUD_NATIVE)
                .build();
    }

    @Test
    public void testLongLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(
                "Cycle found in generating stitching dependency graph for probes.");
        builder.requireThat(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThat(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                .requireThat(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThat(ProbeCategory.STORAGE_BROWSING).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThat(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThat(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.CLOUD_NATIVE)
                .build();
    }

    @Test
    public void testComplexLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(
                "Cycle found in generating stitching dependency graph for probes.");
        builder.requireThat(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThat(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                .requireThat(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThat(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThat(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThat(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .build();
    }

    @Test
    public void testOrderingInSimpleSetup() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        ProbeStitchingDependencyTracker tracker =
                builder.requireThat(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThat(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .build();
        assertEquals(Sets.newHashSet(ProbeCategory.HYPERCONVERGED, ProbeCategory.PAAS,
                ProbeCategory.DATABASE_SERVER),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.HYPERVISOR));
        assertEquals(Sets.newHashSet(ProbeCategory.PAAS,
                ProbeCategory.DATABASE_SERVER),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.HYPERCONVERGED));
        assertEquals(Sets.newHashSet(ProbeCategory.DATABASE_SERVER),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.PAAS));
        assertTrue(tracker.getProbeCategoriesThatStitchBefore(
                ProbeCategory.DATABASE_SERVER).isEmpty());
    }

    @Test
    public void testOrderingInComplexSetup() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        ProbeStitchingDependencyTracker tracker =
                builder.requireThat(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                        .requireThat(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                        .requireThat(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                        .requireThat(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                        .requireThat(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                        .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                        .requireThat(ProbeCategory.PAAS).stitchAfter(ProbeCategory.STORAGE)
                        .build();
        assertEquals(Sets.newHashSet(ProbeCategory.DATABASE_SERVER, ProbeCategory.STORAGE,
                ProbeCategory.STORAGE_BROWSING, ProbeCategory.HYPERCONVERGED,
                ProbeCategory.PAAS),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.HYPERVISOR));
        assertEquals(Sets.newHashSet(ProbeCategory.PAAS,
                ProbeCategory.DATABASE_SERVER, ProbeCategory.STORAGE,
                ProbeCategory.STORAGE_BROWSING),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.HYPERCONVERGED));
        assertEquals(Sets.newHashSet(ProbeCategory.DATABASE_SERVER, ProbeCategory.STORAGE,
                ProbeCategory.STORAGE_BROWSING),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.PAAS));
        assertTrue(tracker.getProbeCategoriesThatStitchBefore(
                ProbeCategory.DATABASE_SERVER).isEmpty());
    }
}
