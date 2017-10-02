package com.vmturbo.topology.processor.group.filter;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Tests for property filters.
 */
public class PropertyFilterTest {

    private final PropertyFilter oidFilter = new PropertyFilter(vertex -> vertex.getOid() == 1L);
    private final Vertex vertex1 = Mockito.mock(Vertex.class);
    private final Vertex vertex2 = Mockito.mock(Vertex.class);
    private final TopologyGraph graph = Mockito.mock(TopologyGraph.class);

    @Before
    public void setup() {
        when(vertex1.getOid()).thenReturn(1L);
        when(vertex2.getOid()).thenReturn(2L);
    }

    @Test
    public void testTestPasses() {
        assertTrue(oidFilter.test(vertex1));
    }

    @Test
    public void testTestFails() {
        assertFalse(oidFilter.test(vertex2));
    }

    @Test
    public void testApplyIncludesPassing() {
        assertThat(
            oidFilter.apply(Stream.of(vertex1), graph).collect(Collectors.toList()),
            contains(vertex1));
    }

    @Test
    public void testApplyExcludesFailing() {
        assertThat(
            oidFilter.apply(Stream.of(vertex2), graph).collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testApplyIncludesPassingAndExcludesFailing() {
        assertThat(
            oidFilter.apply(Stream.of(vertex1, vertex2), graph).collect(Collectors.toList()),
            contains(vertex1));
    }
}
