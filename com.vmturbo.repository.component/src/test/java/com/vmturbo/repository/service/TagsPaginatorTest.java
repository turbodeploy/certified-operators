package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.vmturbo.common.protobuf.PaginationProtoUtil.PaginatedResults;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.SearchOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.commons.Pair;

/**
 * tests for TagsPaginator.
 */
public class TagsPaginatorTest {

    private final Pair<String, Set<String>> aaaPair = new Pair<>("aaaKey", new HashSet<String>(
            Arrays.asList("Product Trust", "PT", "Turbo", "Turbo1", "Turbo3")));

    private final Pair<String, Set<String>> bbbPair = new Pair<>("bbbKey", new HashSet<String>(
            Arrays.asList("Product Trust", "PT", "Turbo", "Turbo1", "Turbo3")));

    private final Pair<String, Set<String>> cccPair = new Pair<>("cccKey", new HashSet<String>(
            Arrays.asList("Product Trust", "PT", "Turbo", "Turbo1", "Turbo3")));

    private final Map<String, Set<String>> allResultsWithSetsOfValues =
            (Map<String, Set<String>>)new HashMap() {{
                put( bbbPair.first,  bbbPair.second);
                put( cccPair.first, cccPair.second);
                put( aaaPair.first, aaaPair.second);
            }};

    private final  List<String> allResultsKeysSorted =
            new ArrayList() {{
                add( aaaPair.first);
                add( bbbPair.first);
                add( cccPair.first);
            }};

    private final  List<String> allResultsKeysSortedReverse =
            new ArrayList() {{
                add( cccPair.first);
                add( bbbPair.first);
                add( aaaPair.first);
            }};

    private final List<Entry<String, Set<String>>> allResultsWithSetsOfValuesList = allResultsWithSetsOfValues.entrySet().stream().collect(
            Collectors.toList());

    /**
     * Tests that all entities are returned in descending order
     * if parameter is provided.
     *
     * @throws Exception on exception occured.
     */
    @Test
    public void testTagsPaginatorFullIterationDescending() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setLimit(2)
                .setAscending(false)
                .setOrderBy(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_NAME))
                .build();


        final TagsPaginator tagsPaginator = new TagsPaginator(5, 5);
        final List<String> paginatedEntities = new ArrayList<>();
        String nextCursor = "";
        do {
            PaginatedResults<Map.Entry<String, Set<String>>> results = tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters.toBuilder()
                    .setCursor(nextCursor)
                    .build());
            nextCursor = results.paginationResponse().getNextCursor();
            paginatedEntities.addAll(results.nextPageEntities().stream().map( e -> e.getKey()).collect(Collectors.toList()));
        } while (!StringUtils.isEmpty(nextCursor));

        assertEquals(paginatedEntities, allResultsKeysSortedReverse);
    }

    /**
     * Tests that all entities are returned in ascending order.
     *
     * @throws Exception on exception occured.
     */
    @Test
    public void testTagsPaginatorOrderByName() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setLimit(2)
                .setAscending(true)
                .setOrderBy(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_NAME))
                .build();


        final TagsPaginator tagsPaginator = new TagsPaginator(5, 5);
        final List<String> paginatedEntities = new ArrayList<>();
        String nextCursor = "";
        do {
            PaginatedResults<Map.Entry<String, Set<String>>> results = tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters.toBuilder()
                    .setCursor(nextCursor)
                    .build());
            nextCursor = results.paginationResponse().getNextCursor();
            paginatedEntities.addAll(results.nextPageEntities().stream().map( e -> e.getKey()).collect(Collectors.toList()));
        } while (!StringUtils.isEmpty(nextCursor));

        assertEquals(paginatedEntities, allResultsKeysSorted);
    }

    /**
     * Tests that number of entities returned matches
     * the defaultLimit when provided.
     *
     * @throws Exception on exception occured.
     */
    @Test
    public void testTagsPaginatorDefaultLimit() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setAscending(true)
                .setOrderBy(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_NAME))
                .build();

        // Max limit is 2
        final int defaultLimit = 2;
        final TagsPaginator tagsPaginator = new TagsPaginator(defaultLimit, 5);
        assertThat(tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters).nextPageEntities().size(), is(defaultLimit));
    }

    /**
     * Tests that number of entities returned matches
     * the maxLimit when provided.
     *
     * @throws Exception on exception occured.
     */
    @Test
    public void testTagsPaginatorEnforceMaxLimit() {
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setLimit(100)
                .setAscending(true)
                .setOrderBy(OrderBy.newBuilder()
                        .setSearch(SearchOrderBy.ENTITY_NAME))
                .build();

        // Max limit is 2
        final int maxLimit = 2;
        final TagsPaginator tagsPaginator = new TagsPaginator(1, maxLimit);
        assertThat(tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters).nextPageEntities().size(), is(maxLimit));
    }

    /**
     * Tests that if cursor is provided is invalid
     * an exception is thrown.
     *
     * @throws IllegalArgumentException when cursor is invalid
     * @throws Exception on exception occured
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTagsPaginatorInvalidCursor() {
        final PaginationParameters paginationParameters =  PaginationParameters.newBuilder()
                .setCursor("foo").build();

        final TagsPaginator tagsPaginator = new TagsPaginator(1, 1);
        tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters);
    }

    /**
     * Tests that if cursor is provided is negative
     * an exception is thrown.
     *
     * @throws IllegalArgumentException when cursor is invalid
     * @throws Exception on exception occured
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTagPaginatorCursorNegativeNumber() {
        final PaginationParameters paginationParameters =  PaginationParameters.newBuilder()
                .setCursor("-1").build();

        final TagsPaginator tagsPaginator = new TagsPaginator(1, 1);
        tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters);
    }

    /**
     * Tests that if pagination limit provided is invalid
     * an exception is thrown.
     *
     * @throws IllegalArgumentException when cursor is invalid
     * @throws Exception on exception occured
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTagsPaginatorInvalidLimit() {
        final PaginationParameters paginationParameters =  PaginationParameters.newBuilder()
                .setLimit(-1).build();

        final TagsPaginator tagsPaginator = new TagsPaginator(1, 1);
        tagsPaginator.paginate(allResultsWithSetsOfValuesList, paginationParameters);
    }


}
