package com.vmturbo.search;

import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchAllQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
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
     * Default limit of results to return in search queries when not specified.
     */
    private final int apiPaginationDefaultLimit;

    /**
     * Max number of results allowed to be returned in search queries.
     */
    private final int apiPaginationMaxLimit;

    /**
     * Create a QueryFactor for constructing search queries.
     *
     * @param readOnlyDSLContext a context for making read-only database queries.
     * @param apiPaginationDefaultLimit default limit of results to return
     * @param apiPaginationMaxLimit max number of results to return
     */
    public QueryFactory(final DSLContext readOnlyDSLContext, final int apiPaginationDefaultLimit, final int apiPaginationMaxLimit) {
        this.readOnlyDSLContext = Objects.requireNonNull(readOnlyDSLContext);
        this.apiPaginationDefaultLimit = apiPaginationDefaultLimit;
        this.apiPaginationMaxLimit = apiPaginationMaxLimit;
    }

    /**
     * Perform an entity search query, and return the results.
     *
     * @param entityQueryApiDTO the API search input
     * @return paginated search results
     * @throws SearchQueryFailedException problems processing request
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> performEntityQuery(
        final EntityQueryApiDTO entityQueryApiDTO) throws SearchQueryFailedException {
        EntityQuery query = new EntityQuery(entityQueryApiDTO, readOnlyDSLContext, apiPaginationDefaultLimit, apiPaginationMaxLimit);
        logger.info("SearchQueryPaginationResponse processEntityQuery");
        return query.readQueryAndExecute();
    }

    /**
     * Perform a group search query, and return the results.
     *
     * @param groupQueryApiDTO the API search input
     * @return paginated search results
     * @throws SearchQueryFailedException problems processing request
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> performGroupQuery(
        final GroupQueryApiDTO groupQueryApiDTO) throws SearchQueryFailedException {
        GroupQuery query = new GroupQuery(groupQueryApiDTO, readOnlyDSLContext, apiPaginationDefaultLimit, apiPaginationMaxLimit);
        logger.info("Processing GroupQuery");
        return query.readQueryAndExecute();
    }

    /**
     * Perform a search all query, and return the results.
     *
     * @param searchAllQueryApiDTO the API search input
     * @return paginated search results
     * @throws SearchQueryFailedException problems processing request
     */
    public SearchQueryPaginationResponse<SearchQueryRecordApiDTO> performSearchAllQuery(
            final SearchAllQueryApiDTO searchAllQueryApiDTO) throws SearchQueryFailedException {
        SearchAllQuery query = new SearchAllQuery(searchAllQueryApiDTO, readOnlyDSLContext, apiPaginationDefaultLimit, apiPaginationMaxLimit);
        logger.info("Processing SearchAllQuery");
        return query.readQueryAndExecute();
    }

    /**
     * Perform an entity count query, and return the results.
     *
     * @param request the API count query input
     * @return the counts of entities in the system, grouped according to the request
     */
    public List<SearchCountRecordApiDTO> performEntityCount(final EntityCountRequestApiDTO request) {
        EntityCountQuery query = new EntityCountQuery(request, readOnlyDSLContext);
        logger.info("Processing entity count query");
        return query.count();
    }

    /**
     * Perform an group count query, and return the results.
     *
     * @param request the API count query input
     * @return the counts of groups in the system, grouped according to the request
     */
    public List<SearchCountRecordApiDTO> performGroupCount(final GroupCountRequestApiDTO request) {
        GroupCountQuery query = new GroupCountQuery(request, readOnlyDSLContext);
        logger.info("Processing group count query");
        return query.count();
    }
}
