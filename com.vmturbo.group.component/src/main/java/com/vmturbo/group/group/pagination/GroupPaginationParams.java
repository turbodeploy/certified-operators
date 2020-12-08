package com.vmturbo.group.group.pagination;

/**
 * Wrapper class that holds pagination parameters for groups.
 */
public class GroupPaginationParams {
    /**
     * The default limit for pagination for group component, to be applied to paginated requests
     * that do not specify a pagination limit.
     */
    private final int groupPaginationDefaultLimit;

    /**
     * The maximum page limit for group component. If a paginated request has a limit that
     * exceeds this value, it will be discarded and this will be used instead.
     */
    private final int groupPaginationMaxLimit;

    /**
     * Constructor for pagination params.
     *
     * @param groupPaginationDefaultLimit The default limit for pagination for group component, to
     *                                    be applied to paginated requests that do not specify a
     *                                    pagination limit.
     * @param groupPaginationMaxLimit The maximum page limit for group component. If a paginated
     *                                request has a limit that exceeds this value, it will be
     *                                discarded and this will be used instead.
     */
    public GroupPaginationParams(final int groupPaginationDefaultLimit,
            final int groupPaginationMaxLimit) {
        this.groupPaginationDefaultLimit = groupPaginationDefaultLimit;
        this.groupPaginationMaxLimit = groupPaginationMaxLimit;
    }

    /**
     * Returns the default pagination limit for group requests.
     *
     * @return the default pagination limit.
     */
    public int getGroupPaginationDefaultLimit() {
        return groupPaginationDefaultLimit;
    }

    /**
     * Returns the max pagination limit for group requests.
     * @return the max pagination limit.
     */
    public int getGroupPaginationMaxLimit() {
        return groupPaginationMaxLimit;
    }
}
