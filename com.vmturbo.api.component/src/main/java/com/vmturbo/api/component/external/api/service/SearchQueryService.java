package com.vmturbo.api.component.external.api.service;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.api.serviceinterfaces.ISearchQueryService;

/**
 * This object serves the requests to the new search API.
 * <ul>
 *     <li>POST /entities/query</li>
 *     <li>POST /groups/query</li>
 *     <li>POST /search/query</li>
 * </ul>
 */
public class SearchQueryService implements ISearchQueryService {
    /**
     * Search entities.  Endpoint: POST /entities/query
     *
     * @param input {@link EntityQueryApiDTO} object with specifications for the search
     * @return list of entities returned
     */
    @Override
    @Nonnull
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> searchEntities(@Nonnull EntityQueryApiDTO input) throws Exception {
        // TODO
        throw new IllegalStateException("not implemented");
    }

    /**
     * Search groups.  Endpoint: POST /groups/query
     *
     * @param input {@link GroupQueryApiDTO} object with specifications for the search
     * @return list of groups returned
     * @throws Exception when some error happens
     */
    @Override
    @Nonnull
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> searchGroups(@Nonnull GroupQueryApiDTO input) throws Exception {
        // TODO
        throw new IllegalStateException("not implemented");
    }

}
