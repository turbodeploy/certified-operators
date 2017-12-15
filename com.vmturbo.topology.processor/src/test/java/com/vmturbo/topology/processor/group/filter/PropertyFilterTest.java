package com.vmturbo.topology.processor.group.filter;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.topology.processor.topology.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;

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

    private final PropertyFilter oidFilter = new PropertyFilter(entity -> entity.getOid() == 1L);
    private final TopologyEntity entity1 = Mockito.mock(TopologyEntity.class);
    private final TopologyEntity entity2 = Mockito.mock(TopologyEntity.class);
    private final TopologyGraph graph = Mockito.mock(TopologyGraph.class);

    @Before
    public void setup() {
        when(entity1.getOid()).thenReturn(1L);
        when(entity2.getOid()).thenReturn(2L);
    }

    @Test
    public void testTestPasses() {
        assertTrue(oidFilter.test(entity1));
    }

    @Test
    public void testTestFails() {
        assertFalse(oidFilter.test(entity2));
    }

    @Test
    public void testApplyIncludesPassing() {
        assertThat(
            oidFilter.apply(Stream.of(entity1), graph).collect(Collectors.toList()),
            contains(entity1));
    }

    @Test
    public void testApplyExcludesFailing() {
        assertThat(
            oidFilter.apply(Stream.of(entity2), graph).collect(Collectors.toList()),
            is(empty()));
    }

    @Test
    public void testApplyIncludesPassingAndExcludesFailing() {
        assertThat(
            oidFilter.apply(Stream.of(entity1, entity2), graph).collect(Collectors.toList()),
            contains(entity1));
    }
}
