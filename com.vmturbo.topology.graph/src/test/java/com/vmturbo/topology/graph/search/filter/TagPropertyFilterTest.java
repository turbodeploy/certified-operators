package com.vmturbo.topology.graph.search.filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.Test;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.topology.graph.SearchableProps;
import com.vmturbo.topology.graph.TagIndex;
import com.vmturbo.topology.graph.TestGraphEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for the {@link TagPropertyFilter}.
 */
public class TagPropertyFilterTest {

    private static final MapFilter mapFilter = MapFilter.newBuilder()
            .setKey("KEY")
            .build();

    /**
     * Test that the right entities are retained when all matched entities are resolved using
     * a single global tag index.
     */
    @Test
    public void testMatchedEntitiesFromOneIndex() {

        TagPropertyFilter<TestGraphEntity> filter = new TagPropertyFilter<>(mapFilter);

        final TagIndex tagIndex = mockTagIndex(1L, 2L);
        final TestGraphEntity rightValueEntity = mockEntity(1, tagIndex);
        final TestGraphEntity rightValueEntity2 = mockEntity(2, tagIndex);
        final TestGraphEntity wrongValueEntity = mockEntity(3, tagIndex);

        List<TestGraphEntity> ret = filter.apply(Stream.of(rightValueEntity, rightValueEntity2, wrongValueEntity),
                (TopologyGraph<TestGraphEntity>)mock(TopologyGraph.class))
                .collect(Collectors.toList());
        assertThat(ret, containsInAnyOrder(rightValueEntity, rightValueEntity2));
    }

    /**
     * Test that the right entities are retained when matched entities are resolved using
     * entity-specific tag indices.
     */
    @Test
    public void testMatchedEntitiesFromMultipleIndices() {

        TagPropertyFilter<TestGraphEntity> filter = new TagPropertyFilter<>(mapFilter);

        final TagIndex rightTagIdx = mockTagIndex(1L);
        final TagIndex rightTagIdx2 = mockTagIndex(2L);
        final TagIndex wrongTagIdx = mockTagIndex();
        final TestGraphEntity rightValueEntity = mockEntity(1, rightTagIdx);
        final TestGraphEntity rightValueEntity2 = mockEntity(2, rightTagIdx2);
        final TestGraphEntity wrongValueEntity = mockEntity(3, wrongTagIdx);

        List<TestGraphEntity> ret = filter.apply(Stream.of(rightValueEntity, rightValueEntity2, wrongValueEntity),
                (TopologyGraph<TestGraphEntity>)mock(TopologyGraph.class))
            .collect(Collectors.toList());
        assertThat(ret, containsInAnyOrder(rightValueEntity, rightValueEntity2));

    }

    @Nonnull
    private TagIndex mockTagIndex(final long... matchingIds) {
        final TagIndex tagIndex = mock(TagIndex.class);
        LongSet matching = new LongOpenHashSet(matchingIds.length);
        for (long m : matchingIds) {
            matching.add(m);
        }
        when(tagIndex.getMatchingEntities(eq(mapFilter), any()))
                .thenReturn(matching);
        return tagIndex;
    }

    @Nonnull
    private TestGraphEntity mockEntity(final long id, final TagIndex tagIndex) {
        TestGraphEntity e = mock(TestGraphEntity.class);
        when(e.getOid()).thenReturn(id);
        SearchableProps p = mock(SearchableProps.class);
        when(p.getTagIndex()).thenReturn(tagIndex);
        when(e.getSearchableProps(any())).thenReturn(p);
        return e;
    }

}