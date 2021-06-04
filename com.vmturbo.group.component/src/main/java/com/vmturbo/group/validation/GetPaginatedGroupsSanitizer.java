package com.vmturbo.group.validation;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.GroupOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPaginatedGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.group.common.InvalidParameterException;
import com.vmturbo.group.group.pagination.GroupPaginationParams;

/**
 * Sanitizer for {@link GetPaginatedGroupsRequest} objects.
 */
public class GetPaginatedGroupsSanitizer implements InputSanitizer<GetPaginatedGroupsRequest> {

    private static final Logger logger = LogManager.getLogger();

    private final GroupPaginationParams defaultPaginationParams;

    /**
     * Constructor.
     *
     * @param defaultPaginationParams the default parameters to be used if the input must be
     *                                sanitized.
     */
    public GetPaginatedGroupsSanitizer(GroupPaginationParams defaultPaginationParams) {
        this.defaultPaginationParams = defaultPaginationParams;
    }

    @Override
    @Nonnull
    public GetPaginatedGroupsRequest sanitize(@Nonnull GetPaginatedGroupsRequest input)
            throws InvalidParameterException {
        GetPaginatedGroupsRequest.Builder result = GetPaginatedGroupsRequest.newBuilder();
        // validate pagination parameters
        if (input.hasPaginationParameters()) {
            PaginationParameters inputParams = input.getPaginationParameters();
            PaginationParameters.Builder resultParams = PaginationParameters.newBuilder();
            resultParams.setCursor(Integer.toString(getCursorValue(inputParams)));
            resultParams.setLimit(getPaginationLimit(inputParams));
            resultParams.setOrderBy(getOrderBy(inputParams));
            resultParams.setAscending(inputParams.getAscending());
            result.setPaginationParameters(resultParams.build());
        } else {
            result.setPaginationParameters(PaginationParameters.newBuilder()
                    .setCursor("0")
                    .setOrderBy(OrderBy.newBuilder()
                            .setGroupSearch(GroupOrderBy.GROUP_NAME)
                            .build())
                    .setLimit(defaultPaginationParams.getGroupPaginationDefaultLimit()));
        }
        // check for group filter existence
        if (input.hasGroupFilter()) {
            result.setGroupFilter(input.getGroupFilter());
        } else {
            result.setGroupFilter(GroupFilter.getDefaultInstance());
        }
        // add scopes
        result.addAllScopes(input.getScopesList());
        return result.build();
    }

    /**
     * Returns the pagination limit doing some sanity checks. If the input limit exceeds max
     * pagination limit, returns the max pagination limit. If there is no pagination limit provided,
     * it returns a default value.
     *
     * @param paginationParams the pagination parameters provided by the user.
     * @return the pagination limit for the query.
     * @throws InvalidParameterException if the limit provided is not a positive integer.
     */
    private int getPaginationLimit(@Nonnull PaginationParameters paginationParams)
            throws InvalidParameterException {
        if (!paginationParams.hasLimit()) {
            return defaultPaginationParams.getGroupPaginationDefaultLimit();
        }
        int paginationLimit = paginationParams.getLimit();
        if (paginationLimit <= 0) {
            throw new InvalidParameterException("Invalid limit value provided: '" + paginationLimit
                    + "'. Limit must be a positive integer.");
        }
        final int maxPaginationLimit = defaultPaginationParams.getGroupPaginationMaxLimit();
        if (paginationLimit > maxPaginationLimit) {
            logger.warn("Client limit " + paginationParams.getLimit() + " exceeds max limit "
                    + maxPaginationLimit + ". Page size will be reduced to "
                    + maxPaginationLimit + ".");
            paginationLimit = maxPaginationLimit;
        }
        return paginationLimit;
    }

    /**
     * Extracts the cursor from the pagination parameters, doing some sanity checking.
     *
     * @param params the pagination parameters provided by the user.
     * @return the cursor value if it's a valid one, otherwise 0.
     * @throws InvalidParameterException if the limit provided is not a positive integer.
     */
    private int getCursorValue(@Nonnull PaginationParameters params)
            throws InvalidParameterException {
        int cursorValue;
        if (params.hasCursor()) {
            try {
                cursorValue = Integer.parseInt(params.getCursor());
            } catch (NumberFormatException e) {
                throw new InvalidParameterException("Invalid cursor value provided: '"
                        + params.getCursor() + "'.");
            }
        } else {
            cursorValue = 0;
        }
        return cursorValue;
    }

    /**
     * Extracts the OrderBy value from the pagination parameters. If no group related order by is
     * provided, a default is set.
     *
     * @param params the pagination parameters provided by the user.
     * @return the OrderBy value of the request, or the default.
     */
    private OrderBy getOrderBy(@Nonnull PaginationParameters params) {
        if (!params.hasOrderBy() || !params.getOrderBy().hasGroupSearch()) {
            return OrderBy.newBuilder().setGroupSearch(GroupOrderBy.GROUP_NAME).build();
        }
        return params.getOrderBy();
    }
}
