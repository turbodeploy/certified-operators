package com.vmturbo.search;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.EntityCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.EntityQueryApiDTO;
import com.vmturbo.api.dto.searchquery.FieldValueTypeApiDTO;
import com.vmturbo.api.dto.searchquery.GroupCountRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupMetadataRequestApiDTO;
import com.vmturbo.api.dto.searchquery.GroupQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchAllQueryApiDTO;
import com.vmturbo.api.dto.searchquery.SearchCountRecordApiDTO;
import com.vmturbo.api.dto.searchquery.SearchQueryRecordApiDTO;
import com.vmturbo.api.pagination.searchquery.SearchQueryPaginationResponse;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Read only from search(extractor) database.
 *
 * <p>Responsible for mapping {@link EntityQueryApiDTO} request into queries, fetching data,
 *    and mapping back into response objects i.e {@link SearchQueryRecordApiDTO} for entities </p>
 **/
public class ApiQueryEngine implements IApiQueryEngine {

    /**
     * A database endpoint configuration.
     */
    private final DbEndpoint readonlyDbEndpoint;

    /**
     * A feature flag, if true then search features are enabled.
     */
    private final boolean enableSearchApi;

    /**
     * A factory for constructing search queries of different types (entity, group).
     */
    private QueryFactory queryFactory;

    /**
     * Default limit of results to return in search queries when not specified.
     */
    private final int apiPaginationDefaultLimit;

    /**
     * Max number of results allowed to be returned in search queries.
     */
    private final int apiPaginationMaxLimit;

    /**
     * Construct an ApiQueryEngine.
     *
     * @param readonlyDbEndpoint a database endpoint configuration
     * @param enableSearchApi a feature flag, if true then search features are enabled
     * @param apiPaginationDefaultLimit default limit of results to return
     * @param apiPaginationMaxLimit max number of results to return
     *
     */
    public ApiQueryEngine(@Nonnull DbEndpoint readonlyDbEndpoint, boolean enableSearchApi, final int apiPaginationDefaultLimit, final int apiPaginationMaxLimit) {
        this.readonlyDbEndpoint = Objects.requireNonNull(readonlyDbEndpoint);
        this.enableSearchApi = enableSearchApi;
        this.apiPaginationDefaultLimit = apiPaginationDefaultLimit;
        this.apiPaginationMaxLimit = apiPaginationMaxLimit;
    }

    @Override
    public SearchQueryPaginationResponse processEntityQuery(@Nonnull final EntityQueryApiDTO request)
            throws UnsupportedDialectException, SQLException, SearchQueryFailedException {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performEntityQuery(request);
    }

    @Override
    public SearchQueryPaginationResponse processGroupQuery(@Nonnull final GroupQueryApiDTO request)
            throws UnsupportedDialectException, SQLException, SearchQueryFailedException {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performGroupQuery(request);
    }

    @Override
    public SearchQueryPaginationResponse processSearchAllQuery(@Nonnull final SearchAllQueryApiDTO request)
            throws UnsupportedDialectException, SQLException, SearchQueryFailedException {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performSearchAllQuery(request);
    }

    @Override
    public List<SearchCountRecordApiDTO> countEntities(
            final EntityCountRequestApiDTO request) throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performEntityCount(request);
    }

    @Override
    public List<SearchCountRecordApiDTO> countGroups(
            final GroupCountRequestApiDTO request) throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performGroupCount(request);
    }

    /**
     * Processes a {@link EntityMetadataRequestApiDTO} and returns an (unpaginated) list of results.
     *
     * @param request describes the group by parameters of the request
     * @return a list of results
     * @throws Exception when the query cannot be processed
     */
    @Override
    public List<FieldValueTypeApiDTO> entityFields(
            @Nonnull EntityMetadataRequestApiDTO request)
            throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performEntityFieldQuery(request);
    }

    /**
     * Processes a {@link GroupMetadataRequestApiDTO} and returns an (unpaginated) list of results.
     *
     * @param request describes the group by parameters of the request
     * @return a list of results
     * @throws Exception when the query cannot be processed
     */
    @Override
    public List<FieldValueTypeApiDTO> groupFields(
            @Nonnull GroupMetadataRequestApiDTO request)
            throws Exception {
        if (!enableSearchApi) {
            throw new UnsupportedOperationException("Search API is not yet enabled!");
        }
        return getQueryFactory().performGroupFieldQuery(request);
    }

    /**
     * Initializes the API query factory from the {@link DbEndpoint}.
     *
     * @return a factory for constructing API database queries
     * @throws UnsupportedDialectException when the dialect is not supported
     * @throws SQLException when a problem has occurred connecting to the database
     */
    private QueryFactory getQueryFactory() throws UnsupportedDialectException, SQLException {
        if (this.queryFactory == null) {
            try {
                this.queryFactory = new QueryFactory(readonlyDbEndpoint.dslContext(), apiPaginationDefaultLimit, apiPaginationMaxLimit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return queryFactory;
    }
}
