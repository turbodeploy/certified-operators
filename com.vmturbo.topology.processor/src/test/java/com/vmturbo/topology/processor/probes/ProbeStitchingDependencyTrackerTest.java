package com.vmturbo.topology.processor.probes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.probes.ProbeStitchingDependencyTracker.Builder;

import java.util.Set;

/**
 * Tests for {@link ProbeStitchingDependencyTracker}.
 */
public class ProbeStitchingDependencyTrackerTest {

    private final String categoryExceptionMsg = "Cycle found in generating stitching dependency " +
            "graph for different probe categories.";

    private final String typeExceptionMsg = "Cycle found in generating stitching dependency " +
            "graph for different probe types";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSimpleLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(categoryExceptionMsg);
        builder.requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.CLOUD_NATIVE)
                .build();
    }

    @Test
    public void testLongLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(categoryExceptionMsg);
        builder.requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThatProbeCategory(ProbeCategory.STORAGE_BROWSING).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeCategory(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.HYPERVISOR)
                .requireThatProbeCategory(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.CLOUD_NATIVE)
                .build();
    }

    @Test
    public void testComplexLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(categoryExceptionMsg);
        builder.requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThatProbeCategory(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeCategory(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .build();
    }

    @Test
    public void testProbeTypesWithLoop() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(typeExceptionMsg);
        builder.requireThatProbeType(SDKProbeType.VCENTER).stitchAfter(SDKProbeType.HYPERV)
                .requireThatProbeType(SDKProbeType.HYPERV).stitchAfter(SDKProbeType.AWS)
                .requireThatProbeType(SDKProbeType.AWS).stitchAfter(SDKProbeType.VCENTER)
                .build();
    }

    @Test
    public void testMixWithLoopInCategories() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(categoryExceptionMsg);
        builder.requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThatProbeCategory(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeCategory(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeType(SDKProbeType.VCENTER).stitchAfter(SDKProbeType.HYPERV)
                .requireThatProbeType(SDKProbeType.HYPERV).stitchAfter(SDKProbeType.AWS)
                .requireThatProbeType(SDKProbeType.AWS).stitchAfter(SDKProbeType.VCENTER)
                .build();
    }

    @Test
    public void testMixWithLoopInTypes() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        expectedException.expect(ProbeException.class);
        expectedException.expectMessage(typeExceptionMsg);
        builder.requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                .requireThatProbeCategory(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                .requireThatProbeCategory(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeType(SDKProbeType.VCENTER).stitchAfter(SDKProbeType.HYPERV)
                .requireThatProbeType(SDKProbeType.HYPERV).stitchAfter(SDKProbeType.AWS)
                .requireThatProbeType(SDKProbeType.AWS).stitchAfter(SDKProbeType.VCENTER)
                .build();
    }

    @Test
    public void testOrderingInSimpleSetup() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        ProbeStitchingDependencyTracker tracker =
                builder.requireThatProbeCategory(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                .requireThatProbeCategory(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                .build();
        assertEquals(Sets.newHashSet(ProbeCategory.HYPERCONVERGED,
                ProbeCategory.PAAS, ProbeCategory.DATABASE_SERVER),
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
                builder.requireThatProbeCategory(ProbeCategory.CLOUD_NATIVE).stitchAfter(ProbeCategory.CLOUD_MANAGEMENT)
                        .requireThatProbeCategory(ProbeCategory.CLOUD_MANAGEMENT).stitchAfter(ProbeCategory.STORAGE)
                        .requireThatProbeCategory(ProbeCategory.STORAGE).stitchAfter(ProbeCategory.STORAGE_BROWSING)
                        .requireThatProbeCategory(ProbeCategory.HYPERVISOR).stitchAfter(ProbeCategory.HYPERCONVERGED)
                        .requireThatProbeCategory(ProbeCategory.HYPERCONVERGED).stitchAfter(ProbeCategory.PAAS)
                        .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.DATABASE_SERVER)
                        .requireThatProbeCategory(ProbeCategory.PAAS).stitchAfter(ProbeCategory.STORAGE)
                        .build();
        assertEquals(Sets.newHashSet(ProbeCategory.DATABASE_SERVER,
                ProbeCategory.STORAGE, ProbeCategory.STORAGE_BROWSING, ProbeCategory.HYPERCONVERGED,
                ProbeCategory.PAAS), tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.HYPERVISOR));
        assertEquals(Sets.newHashSet(ProbeCategory.PAAS,
                ProbeCategory.DATABASE_SERVER, ProbeCategory.STORAGE,
                ProbeCategory.STORAGE_BROWSING),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.HYPERCONVERGED));
        assertEquals(Sets.newHashSet(ProbeCategory.DATABASE_SERVER,
                ProbeCategory.STORAGE, ProbeCategory.STORAGE_BROWSING),
                tracker.getProbeCategoriesThatStitchBefore(ProbeCategory.PAAS));
        assertTrue(tracker.getProbeCategoriesThatStitchBefore(
                ProbeCategory.DATABASE_SERVER).isEmpty());
    }

    @Test
    public void testProbeTypeOrdering() throws Exception {
        Builder builder = ProbeStitchingDependencyTracker.newBuilder();
        ProbeStitchingDependencyTracker tracker =
                builder.requireThatProbeType(SDKProbeType.CLOUD_FOUNDRY).stitchAfter(SDKProbeType.PIVOTAL_OPSMAN)
                        .requireThatProbeType(SDKProbeType.CLOUD_FOUNDRY).stitchAfter(SDKProbeType.VCENTER)
                        .build();
        assertEquals(Sets.newHashSet(SDKProbeType.PIVOTAL_OPSMAN, SDKProbeType.VCENTER),
                tracker.getProbeTypesThatStitchBefore(SDKProbeType.CLOUD_FOUNDRY));
        assertTrue(tracker.getProbeTypesThatStitchBefore(SDKProbeType.PIVOTAL_OPSMAN).isEmpty());
        assertTrue(tracker.getProbeTypesThatStitchBefore(SDKProbeType.VCENTER).isEmpty());
    }

    @Test
    public void testDefaultScopeForStorageProbes() {
        assertEquals(
                Sets.newHashSet(ProbeCategory.HYPERVISOR, ProbeCategory.CLOUD_MANAGEMENT),
                ProbeStitchingDependencyTracker
                        .getDefaultStitchingDependencyTracker()
                        .getProbeCategoriesThatStitchBefore(ProbeCategory.STORAGE)
        );
    }

    @Test
    public void testDefaultScopeForCloudManagementProbes() {
        assertEquals(
                Sets.newHashSet(),
                ProbeStitchingDependencyTracker
                        .getDefaultStitchingDependencyTracker()
                        .getProbeCategoriesThatStitchBefore(ProbeCategory.CLOUD_MANAGEMENT)
        );
    }

    @Test
    public void testProbeScopeForCustomCategory() {
        Set<ProbeCategory> probeScope = ProbeStitchingDependencyTracker
                .getDefaultStitchingDependencyTracker()
                .getProbeCategoriesThatStitchBefore(ProbeCategory.CUSTOM);
        Assert.assertTrue(probeScope.contains(ProbeCategory.APPLICATION_SERVER));
    }
}
