package com.vmturbo.search;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;

/**
 * A factory for constructing search queries of different types (entity, group).
 */
public class QueryFactory {

    private static final Logger logger = LogManager.getLogger();

    /**
     * A context for making read-only database queries.
     */
    private final DSLContext readOnlyDSLContext;

    /**
     * Create a QueryFactor for constructing search queries.
     *
     * @param readOnlyDSLContext a context for making read-only database queries.
     */
    public QueryFactory(final DSLContext readOnlyDSLContext) {
        this.readOnlyDSLContext = Objects.requireNonNull(readOnlyDSLContext);
    }

    /**
     * Perform an entity search query, and return the results.
     *
     * @param entityQueryApiDTO the API search input
     * @return paginated search results
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> performEntityQuery(
        final EntityQueryApiDTO entityQueryApiDTO) {
        AbstractQuery query = new EntityQuery(entityQueryApiDTO, readOnlyDSLContext);
        logger.info("SearchQueryPaginationResponse processEntityQuery");
        return query.readQueryAndExecute();
    }

    /**
     * Perform a group search query, and return the results.
     *
     * @param groupQueryApiDTO the API search input
     * @return paginated search results
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> performGroupQuery(
        final GroupQueryApiDTO groupQueryApiDTO) {
        AbstractQuery query = new GroupQuery(groupQueryApiDTO, readOnlyDSLContext);
        logger.info("Processing GroupQuery");
        return query.readQueryAndExecute();
    }
}
