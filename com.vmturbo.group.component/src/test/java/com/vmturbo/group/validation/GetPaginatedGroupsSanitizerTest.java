package com.vmturbo.group.validation;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.GroupOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.group.common.InvalidParameterException;
import com.vmturbo.group.group.pagination.GroupPaginationParams;

/**
 * Unit tests for {@link GetPaginatedGroupsSanitizer}.
 */
public class GetPaginatedGroupsSanitizerTest {

    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 500;

    private final GetPaginatedGroupsSanitizer sanitizer =
            new GetPaginatedGroupsSanitizer(new GroupPaginationParams(DEFAULT_LIMIT, MAX_LIMIT));

    /**
     * Tests that a default group filter is provided when it's missing from the request.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test
    public void testNoGroupFilter() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder().build();
        // WHEN
        GetPaginatedGroupsRequest result = sanitizer.sanitize(request);
        // THEN
        Assert.assertEquals(GroupFilter.getDefaultInstance(), result.getGroupFilter());
    }

    /**
     * Tests that default paginated parameters are provided when they are missing from the request.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test
    public void testNoPaginationParameters() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder().build();
        // WHEN
        GetPaginatedGroupsRequest result = sanitizer.sanitize(request);
        // THEN
        Assert.assertTrue(result.hasPaginationParameters());
        PaginationParameters params = result.getPaginationParameters();
        Assert.assertEquals("0", params.getCursor());
        Assert.assertTrue(params.getAscending());
        Assert.assertEquals(DEFAULT_LIMIT, params.getLimit());
        Assert.assertEquals(OrderBy.newBuilder().setGroupSearch(GroupOrderBy.GROUP_NAME).build(),
                params.getOrderBy());
    }

    /**
     * Tests that a default cursor (pointing to the beginning of the results) is added when it's
     * missing from the request.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test
    public void testNoCursor() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        // WHEN
        GetPaginatedGroupsRequest result = sanitizer.sanitize(request);
        // THEN
        Assert.assertEquals("0", result.getPaginationParameters().getCursor());
    }

    /**
     * Tests that an exception is being thrown when the request contains an invalid cursor.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test(expected = InvalidParameterException.class)
    public void testBadCursor() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setCursor("a")
                        .build())
                .build();
        // WHEN
        sanitizer.sanitize(request);
    }

    /**
     * Tests that a default orderBy is provided when it's missing from the request.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test
    public void testNoOrderBy() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        // WHEN
        GetPaginatedGroupsRequest result = sanitizer.sanitize(request);
        // THEN
        Assert.assertEquals(OrderBy.newBuilder().setGroupSearch(GroupOrderBy.GROUP_NAME).build(),
                result.getPaginationParameters().getOrderBy());
    }

    /**
     * Tests that a default limit is provided when it's missing from the request.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test
    public void testNoLimit() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setPaginationParameters(PaginationParameters.getDefaultInstance())
                .build();
        // WHEN
        GetPaginatedGroupsRequest result = sanitizer.sanitize(request);
        // THEN
        Assert.assertEquals(DEFAULT_LIMIT, result.getPaginationParameters().getLimit());
    }

    /**
     * Tests that an exception is being thrown when the request contains an invalid limit.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test(expected = InvalidParameterException.class)
    public void testBadLimit() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setLimit(-1)
                        .build())
                .build();
        // WHEN
        sanitizer.sanitize(request);
    }

    /**
     * Tests that when the request contains a page size limit that is higher that the allowed value,
     * it is replaced by the highest allowed value.
     *
     * @throws InvalidParameterException on invalid input
     */
    @Test
    public void testTooHighLimit() throws InvalidParameterException {
        // GIVEN
        GetPaginatedGroupsRequest request = GetPaginatedGroupsRequest.newBuilder()
                .setPaginationParameters(PaginationParameters.newBuilder()
                        .setLimit(MAX_LIMIT + 10)
                        .build())
                .build();
        // WHEN
        GetPaginatedGroupsRequest result = sanitizer.sanitize(request);
        // THEN
        Assert.assertEquals(MAX_LIMIT, result.getPaginationParameters().getLimit());
    }
}
