package com.vmturbo.search;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;

/**
 * An interface for establishing entity and group queries against search DB.
 */
public interface IApiQueryEngine {

    /**
     * Processes {@link EntityQueryApiDTO} and returns paginated results.
     *
     * @param entityQueryApiDTO
     * @return
     * @throws Exception
     */
    @Nonnull
    SearchQueryPaginationResponse processEntityQuery(EntityQueryApiDTO entityQueryApiDTO) throws Exception;

}
