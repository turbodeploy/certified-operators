package com.vmturbo.topology.graph;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.topology.graph.TagIndex.DefaultTagIndex;

/**
 * Unit tests for the {@link DefaultTagIndex}.
 */
public class TagIndexTest {

    /*
     * E1 tags:
     *     favouriteFood -> tomato, potato, apple
     *     favouriteFruit -> apple, orange
     * E2 tags:
     *     favouriteFood -> soup
     *     favouriteFruit -> apple
     * E3 tags:
     *     myTag -> apple
     */
    private static final String FAVOURITE_FOOD = "favouriteFood";
    private static final String FAVOURITE_FRUIT = "favouriteFruit";
    private static final String MY_TAG = "myTag";

    private final Tags e1Tags = Tags.newBuilder()
            .putTags(FAVOURITE_FOOD, TagValuesDTO.newBuilder()
                    .addValues("tomato")
                    .addValues("potato")
                    .addValues("apple")
                    .build())
            .putTags(FAVOURITE_FRUIT, TagValuesDTO.newBuilder()
                    .addValues("apple")
                    .build())
            .build();
    private final Tags e2Tags = Tags.newBuilder()
            .putTags(FAVOURITE_FOOD, TagValuesDTO.newBuilder()
                    .addValues("apple")
                    .addValues("orange")
                    .build())
            .build();
    private final Tags e3Tags = Tags.newBuilder()
            .putTags(MY_TAG, TagValuesDTO.newBuilder()
                    .addValues("apple")
                    .build())
            .build();

    private DefaultTagIndex tagIndex;

    /**
     * Set up the index.
     */
    @Before
    public void setup() {
        tagIndex = new DefaultTagIndex();
        tagIndex.addTags(1L, e1Tags);
        tagIndex.addTags(2L, e2Tags);
        tagIndex.addTags(3L, e3Tags);
        tagIndex.finish();
    }

    /**
     * Test retrieving tags for a specific entity.
     */
    @Test
    public void testTagsByEntity() {
        Map<String, Set<String>> e1Result = tagIndex.getTagsForEntity(1L);
        checkTagsEqual(e1Result, extractTags(e1Tags));
        Map<String, Set<String>> e2Result = tagIndex.getTagsForEntity(2L);
        checkTagsEqual(e2Result, extractTags(e2Tags));
        Map<String, Set<String>> e3Result = tagIndex.getTagsForEntity(3L);
        checkTagsEqual(e3Result, extractTags(e3Tags));
    }

    private void checkTagsEqual(Map<String, Set<String>> actual, Map<String, Set<String>> expected) {
        assertThat(actual.keySet(), is(expected.keySet()));
        expected.forEach((key, vals) -> {
            assertThat(actual.get(key), is(vals));
        });
    }

    private Map<String, Set<String>> extractTags(Tags... tags) {
        Map<String, Set<String>> ret = new HashMap<>();
        for (Tags t : tags) {
            t.getTagsMap().forEach((key, vals) -> {
                ret.computeIfAbsent(key, k -> new HashSet<>()).addAll(vals.getValuesList());
            });
        }
        return ret;
    }

    /**
     * Test the utility that constructs a single-entity index.
     */
    @Test
    public void testSingleEntityUtility() {
        TagIndex tagIndex = DefaultTagIndex.singleEntity(1L, e1Tags);
        LongSet result = tagIndex.getMatchingEntities(MapFilter.newBuilder()
                .setRegex(".*")
                .build());
        assertThat(result, containsInAnyOrder( 1L));
    }

    /**
     * Test getting tags from multiple entities.
     */
    @Test
    public void testGetTagsForEntities() {
        LongSet targetSet = new LongOpenHashSet();
        targetSet.add(1L);
        Map<String, Set<String>> result = tagIndex.getTagsForEntities(targetSet);
        checkTagsEqual(result, extractTags(e1Tags));

        targetSet.add(2L);
        result = tagIndex.getTagsForEntities(targetSet);
        checkTagsEqual(result, extractTags(e1Tags, e2Tags));

        targetSet.add(3L);
        result = tagIndex.getTagsForEntities(targetSet);
        checkTagsEqual(result, extractTags(e1Tags, e2Tags, e3Tags));
    }

    /**
     * Test a bulk-get with the "magic" regex - a single regex that's meant to match the "key=value"
     * joined string.
     */
    @Test
    public void testGetMatchingEntitiesMagicRegex() {
        LongSet result = tagIndex.getMatchingEntities(MapFilter.newBuilder()
                .setRegex("favourite.*=.*")
                .build());
        assertThat(result, containsInAnyOrder( 1L, 2L));
    }

    /**
     * Test a bulk-get for a specific key with a value regex.
     */
    @Test
    public void testGetMatchingEntitiesKeyValueRegex() {
        LongSet result = tagIndex.getMatchingEntities(MapFilter.newBuilder()
                .setKey(FAVOURITE_FOOD)
                .setRegex(".*")
                .build());
        assertThat(result, containsInAnyOrder(1L, 2L));
    }

    /**
     * Test a bulk-get for a specific key and specific values.
     */
    @Test
    public void testGetMatchingEntitiesKeyValueList() {
        LongSet result = tagIndex.getMatchingEntities(MapFilter.newBuilder()
                .setKey(FAVOURITE_FOOD)
                .addValues("orange")
                .build());
        assertThat(result, containsInAnyOrder(2L));
    }

    /**
     * Test a bulk-get for a key that does not exist.
     */
    @Test
    public void testGetMatchingEntitiesInvalidKeyValueList() {
        LongSet result = tagIndex.getMatchingEntities(MapFilter.newBuilder()
                .setKey("invalid")
                .addValues("orange")
                .build());
        assertTrue(result.isEmpty());
    }

    /**
     * Test a single-entity test for the magic "key=value" regex.
     */
    @Test
    public void testIsMatchingEntityMagicRegex() {
        MapFilter filter = MapFilter.newBuilder()
                .setRegex("favourite.*=.*")
                .build();
        assertTrue(tagIndex.isMatchingEntity(1L, filter));
        assertTrue(tagIndex.isMatchingEntity(2L, filter));
        assertFalse(tagIndex.isMatchingEntity(3L, filter));
    }

    /**
     * Test a single-entity test for a specific key and a regex value.
     */
    @Test
    public void testIsMatchingEntityKeyValueRegex() {
        MapFilter filter = MapFilter.newBuilder()
                .setKey(FAVOURITE_FOOD)
                .setRegex(".*")
                .build();
        assertTrue(tagIndex.isMatchingEntity(1L, filter));
        assertTrue(tagIndex.isMatchingEntity(2L, filter));
        assertFalse(tagIndex.isMatchingEntity(3L, filter));
    }

    /**
     * Test a single-entity test for a specific key and value.
     */
    @Test
    public void testIsMatchingEntityKeyValueList() {
        MapFilter filter = MapFilter.newBuilder()
                .setKey(FAVOURITE_FOOD)
                .addValues("orange")
                .build();
        assertFalse(tagIndex.isMatchingEntity(1L, filter));
        assertTrue(tagIndex.isMatchingEntity(2L, filter));
        assertFalse(tagIndex.isMatchingEntity(3L, filter));
    }

    /**
     * Test a single-entity test for an invalid key.
     */
    @Test
    public void testIsMatchingEntityInvalidKeyValueList() {
        MapFilter filter = MapFilter.newBuilder()
                .setKey("invalid")
                .addValues("apple")
                .build();
        assertFalse(tagIndex.isMatchingEntity(1L, filter));
        assertFalse(tagIndex.isMatchingEntity(2L, filter));
        assertFalse(tagIndex.isMatchingEntity(3L, filter));
    }

}
