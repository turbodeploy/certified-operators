package com.vmturbo.components.common.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.junit.Test;

import com.vmturbo.common.protobuf.memory.MemoryVisitor.ClassHistogramSizeVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryGraphVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.MemoryPathVisitor;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.NamedObject;
import com.vmturbo.common.protobuf.memory.MemoryVisitor.TotalSizesAndCountsVisitor;

/**
 * Tests for WalkerBuilder.
 */
public class WalkBuilderTest {

    private final NamedObject testObj = new NamedObject("test", new Object());
    private final NamedObject other = new NamedObject("other", new Object());

    private final Set<Object> exclusions = Collections.newSetFromMap(new IdentityHashMap<>());

    /**
     * testWalkSizesAndCounts.
     */
    @Test
    public void testWalkSizesAndCounts() {
        final TotalSizesAndCountsVisitor totalSizesAndCountsVisitor = MemoryMetricsManager.newWalk()
            .withExclusions(exclusions, 1)
            .maxDepth(2)
            .walkSizesAndCounts(Arrays.asList(testObj.rootObj, other.rootObj));

        assertEquals(exclusions, totalSizesAndCountsVisitor.getExclusions());
        assertEquals(1, totalSizesAndCountsVisitor.getExclusionDepth());
        assertEquals(2, totalSizesAndCountsVisitor.getMaxDepth());
    }

    /**
     * testWalkClassHistogram.
     */
    @Test
    public void testWalkClassHistogram() {
        final ClassHistogramSizeVisitor classHistogramSizeVisitor = MemoryMetricsManager.newWalk()
            .withExclusions(exclusions, 1)
            .maxDepth(2)
            .walkClassHistogram(Arrays.asList(testObj.rootObj, other.rootObj));

        assertEquals(exclusions, classHistogramSizeVisitor.getExclusions());
        assertEquals(1, classHistogramSizeVisitor.getExclusionDepth());
        assertEquals(2, classHistogramSizeVisitor.getMaxDepth());
    }

    /**
     * testWalkMemoryGraph.
     */
    @Test
    public void testWalkMemoryGraph() {
        final MemoryGraphVisitor memoryGraphVisitor = MemoryMetricsManager.newWalk()
            .withExclusions(exclusions, 1)
            .maxDepth(2)
            .walkMemoryGraph(3, true, Arrays.asList(testObj, other));

        assertEquals(exclusions, memoryGraphVisitor.getExclusions());
        assertEquals(1, memoryGraphVisitor.getExclusionDepth());
        assertEquals(2, memoryGraphVisitor.getMaxDepth());
        assertEquals(3, memoryGraphVisitor.getLogDepth());
        assertEquals(true, memoryGraphVisitor.getRetainDescendants());
    }

    /**
     * testFindPaths.
     */
    @Test
    public void testFindPaths() {
        final MemoryPathVisitor pathVisitor = MemoryMetricsManager.newWalk()
            .maxDepth(2)
            .findPaths(3, Arrays.asList(testObj, other), Collections.singleton(String.class));

        assertEquals(2, pathVisitor.getMaxDepth());
        assertEquals(3, pathVisitor.getNumInstancesToFind());
        assertEquals(0, pathVisitor.getMinInstanceDepth());
        assertEquals(Collections.singleton(String.class), pathVisitor.getSearchClasses());
    }

    /**
     * testFindPathsWithMinInstanceDepth.
     */
    @Test
    public void testFindPathsWithMinInstanceDepth() {
        final MemoryPathVisitor pathVisitor = MemoryMetricsManager.newWalk()
            .maxDepth(2)
            .findPaths(3, 4, Arrays.asList(testObj, other), Collections.singleton(String.class));

        assertEquals(2, pathVisitor.getMaxDepth());
        assertEquals(3, pathVisitor.getNumInstancesToFind());
        assertEquals(4, pathVisitor.getMinInstanceDepth());
        assertEquals(Collections.singleton(String.class), pathVisitor.getSearchClasses());
    }
}