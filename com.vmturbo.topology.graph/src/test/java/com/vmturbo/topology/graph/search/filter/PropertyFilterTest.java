package com.vmturbo.topology.graph.search.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Tests for property filters.
 */
public class PropertyFilterTest {

    private final PropertyFilter<TestGraphEntity> oidFilter = new PropertyFilter<>(entity -> entity.getOid() == 1L);
    private final TestGraphEntity entity1 = TestGraphEntity.newBuilder(1L, ApiEntityType.VIRTUAL_MACHINE).build();
    private final TestGraphEntity entity2 = TestGraphEntity.newBuilder(2L, ApiEntityType.VIRTUAL_MACHINE).build();
    private final TopologyGraph<TestGraphEntity> graph = Mockito.mock(TopologyGraph.class);

    @Test
    public void testTestPasses() {
        assertTrue(oidFilter.test(entity1, graph));
    }

    @Test
    public void testTestFails() {
        assertFalse(oidFilter.test(entity2, graph));
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
