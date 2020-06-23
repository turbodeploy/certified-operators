package com.vmturbo.search;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;

/**
 * An interface for establishing entity and group queries against search DB.
 */
public interface IApiQueryEngine {

    /**
     * Processes {@link EntityQueryApiDTO} and returns paginated results.
     *
     * @param entityQueryApiDTO the entity search query
     * @return a paginated list of records
     * @throws Exception when the query cannot be processed.
     */
    @Nonnull
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> processEntityQuery(
        @Nonnull EntityQueryApiDTO entityQueryApiDTO) throws Exception;

    /**
     * Processes {@link GroupQueryApiDTO} and returns paginated results.
     *
     * @param request the group search query
     * @return a paginated list of records
     * @throws Exception when the query cannot be processed.
     */
    SearchQueryPaginationResponse<SearchQueryRecordApiDTO> processGroupQuery(
        @Nonnull GroupQueryApiDTO request) throws Exception;

}
